import re
from tqdm import tqdm

import pandas as pd


class Reader:

    @staticmethod
    def read(path, regex_pattern, columns, nrows=None, verbose=False):
        compiler = re.compile(regex_pattern)

        log = (path).open("r")
        result = []
        ill_rows = 0
        for idx, line in tqdm(enumerate(log)):
            match = compiler.match(line)
            if not match:
                ill_rows += 1
            else:
                result.append(dict(zip(columns, [match.group(1+n) for n in range(len(columns))])))
            if nrows and (idx == nrows-1):
                break
        if verbose and (ill_rows > 0): print(f"Could not read {ill_rows} rows: Bad format")
        data = pd.DataFrame(result, columns=columns)
        if verbose: print(f'Data is of shape: {data.shape}')
        return data
