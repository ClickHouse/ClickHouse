---
description: 'The `Executable` and `ExecutablePool` table engines allow you to define
  a table whose rows are generated from a script that you define (by writing rows
  to **stdout**).'
sidebar_label: 'Executable'
sidebar_position: 40
slug: /engines/table-engines/special/executable
title: 'Executable and ExecutablePool Table Engines'
---

# Executable and ExecutablePool Table Engines

The `Executable` and `ExecutablePool` table engines allow you to define a table whose rows are generated from a script that you define (by writing rows to **stdout**). The executable script is stored in the `users_scripts` directory and can read data from any source.

- `Executable` tables: the script is run on every query
- `ExecutablePool` tables: maintains a pool of persistent processes, and takes processes from the pool for reads

You can optionally include one or more input queries that stream their results to **stdin** for the script to read.

## Creating an Executable Table {#creating-an-executable-table}

The `Executable` table engine requires two parameters: the name of the script and the format of the incoming data. You can optionally pass in one or more input queries:

```sql
Executable(script_name, format, [input_query...])
```

Here are the relevant settings for an `Executable` table:

- `send_chunk_header`
    - Description: Send the number of rows in each chunk before sending a chunk to process. This setting can help to write your script in a more efficient way to preallocate some resources
    - Default value: false
- `command_termination_timeout`
    - Description: Command termination timeout in seconds
    - Default value: 10
- `command_read_timeout`
    - Description: Timeout for reading data from command stdout in milliseconds
    - Default value: 10000
- `command_write_timeout`
    - Description: Timeout for writing data to command stdin in milliseconds
    - Default value: 10000


Let's look at an example. The following Python script is named `my_script.py` and is saved in the `user_scripts` folder. It reads in a number `i` and prints `i` random strings, with each string preceded by a number that is separated by a tab:

```python
#!/usr/bin/python3

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

The following `my_executable_table` is built from the output of `my_script.py`, which will generate 10 random strings every time you run a `SELECT` from `my_executable_table`:

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

Creating the table returns immediately and does not invoke the script. Querying `my_executable_table` causes the script to be invoked:

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

## Passing Query Results to a Script {#passing-query-results-to-a-script}

Users of the Hacker News website leave comments. Python contains a natural language processing toolkit (`nltk`) with a `SentimentIntensityAnalyzer` for determining if comments are positive, negative, or neutral - including assigning a value between -1 (a very negative comment) and 1 (a very positive comment). Let's create an `Executable` table that computes the sentiment of Hacker News comments using `nltk`.

This example uses the `hackernews` table described [here](/engines/table-engines/mergetree-family/invertedindexes/#full-text-search-of-the-hacker-news-dataset). The `hackernews` table includes an `id` column of type `UInt64` and a `String` column named `comment`. Let's start by defining the `Executable` table:

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

Some comments about the `sentiment` table:

- The file `sentiment.py` is saved in the `user_scripts` folder (the default folder of the `user_scripts_path` setting)
- The `TabSeparated` format means our Python script needs to generate rows of raw data that contain tab-separated values
- The query selects two columns from `hackernews`. The Python script will need to parse out those column values from the incoming rows

Here is the definition of `sentiment.py`:

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

Some comments about our Python script:

- For this to work, you will need to run `nltk.downloader.download('vader_lexicon')`. This could have been placed in the script, but then it would have been downloaded every time a query was executed on the `sentiment` table - which is not efficient
- Each value of `row` is going to be a row in the result set of `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20`
- The incoming row is tab-separated, so we parse out the `id` and `comment` using the Python `split` function
- The result of `polarity_scores` is a JSON object with a handful of values. We decided to just grab the `compound` value of this JSON object
- Recall that the `sentiment` table in ClickHouse uses the `TabSeparated` format and contains two columns, so our `print` function separates those columns with a tab

Every time you write a query that selects rows from the `sentiment` table, the `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` query is executed and the result is passed to `sentiment.py`. Let's test it out:

```sql
SELECT *
FROM sentiment
```

The response looks like:

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```


## Creating an ExecutablePool Table {#creating-an-executablepool-table}

The syntax for `ExecutablePool` is similar to `Executable`, but there are a couple of relevant settings unique to an `ExecutablePool` table:

- `pool_size`
    - Description: Processes pool size. If size is 0, then there are no size restrictions
    - Default value: 16
- `max_command_execution_time`
    - Description: Max command execution time in seconds
    - Default value: 10

We can easily convert the `sentiment` table above to use `ExecutablePool` instead of `Executable`:

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
    pool_size = 4;
```

ClickHouse will maintain 4 processes on-demand when your client queries the `sentiment_pooled` table.
