Values
------

Prints every row in parentheses. Rows are separated by commas. There is no comma after the last row. The values inside the parentheses are also comma-separated. Numbers are output in decimal format without quotes. Arrays are output in square brackets. Strings, dates, and dates with times are output in quotes. Escaping rules and parsing are same as in the TabSeparated format. During formatting, extra spaces aren't inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed).

Minimum set of symbols that you must escape in Values format is single quote and backslash.

This is the format that is used in ``INSERT INTO t VALUES ...``
But you can also use it for query result.
