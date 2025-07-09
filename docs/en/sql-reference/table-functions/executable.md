---
slug: /en/engines/table-functions/executable
sidebar_position: 50
sidebar_label:  executable
keywords: [udf, user defined function, clickhouse, executable, table, function]
---

# executable Table Function for UDFs

The `executable` table function creates a table based on the output of a user-defined function (UDF) that you define in a script that outputs rows to **stdout**. The executable script is stored in the `users_scripts` directory and can read data from any source. Make sure your ClickHouse server has all the required packages to run the executable script. For example, if it is a Python script, ensure that the server has the necessary Python packages installed.

You can optionally include one or more input queries that stream their results to **stdin** for the script to read.

:::note
A key advantage between ordinary UDF functions and the `executable` table function and `Executable` table engine is that ordinary UDF functions cannot change the row count. For example, if the input is 100 rows, then the result must return 100 rows. When using the `executable` table function or `Executable` table engine, your script can make any data transformations you want, including complex aggregations.
:::

## Syntax

The `executable` table function requires three parameters and accepts an optional list of input queries:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

- `script_name`: the file name of the script. saved in the `user_scripts` folder (the default folder of the `user_scripts_path` setting)
- `format`: the format of the generated table
- `structure`: the table schema of the generated table
- `input_query`: an optional query (or collection or queries) whose results are passed to the script via **stdin**

:::note
If you are going to invoke the same script repeatedly with the same input queries, consider using the [`Executable` table engine](../../engines/table-engines/special/executable.md).
:::

The following Python script is named `generate_random.py` and is saved in the `user_scripts` folder. It reads in a number `i` and prints `i` random strings, with each string preceded by a number that is separated by a tab:

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Let's invoke the script and have it generate 10 random strings:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

The response looks like:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

## Settings

- `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Default value is `false`.
- `pool_size` — Size of pool. If 0 is specified as `pool_size` then there is no pool size restrictions. Default value is `16`.
- `max_command_execution_time` — Maximum executable script command execution time for processing block of data. Specified in seconds. Default value is 10.
- `command_termination_timeout` — executable script should contain main read-write loop. After table function is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown, before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is 10.
- `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000.
- `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000.

## Passing Query Results to a Script

Be sure to check out the example in the `Executable` table engine on [how to pass query results to a script](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script). Here is how you execute the same script in that example using the `executable` table function:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```
