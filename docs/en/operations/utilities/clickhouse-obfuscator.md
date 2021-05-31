# ClickHouse obfuscator

A simple tool for table data obfuscation.

It reads an input table and produces an output table, that retains some properties of input, but contains different data.
It allows publishing almost real production data for usage in benchmarks.

It is designed to retain the following properties of data:
- cardinalities of values (number of distinct values) for every column and every tuple of columns;
- conditional cardinalities: number of distinct values of one column under the condition on the value of another column;
- probability distributions of the absolute value of integers; the sign of signed integers; exponent and sign for floats;
- probability distributions of the length of strings;
- probability of zero values of numbers; empty strings and arrays, `NULL`s;

- data compression ratio when compressed with LZ77 and entropy family of codecs;
- continuity (magnitude of difference) of time values across the table; continuity of floating-point values;
- date component of `DateTime` values;

- UTF-8 validity of string values;
- string values look natural.

Most of the properties above are viable for performance testing:

reading data, filtering, aggregatio, and sorting will work at almost the same speed
as on original data due to saved cardinalities, magnitudes, compression ratios, etc.

It works in a deterministic fashion: you define a seed value and the transformation is determined by input data and by seed.
Some transformations are one to one and could be reversed, so you need to have a large seed and keep it in secret.

It uses some cryptographic primitives to transform data but from the cryptographic point of view, it does not do it properly, that is why you should not consider the result as secure unless you have another reason. The result may retain some data you don't want to publish.


It always leaves 0, 1, -1 numbers, dates, lengths of arrays, and null flags exactly as in source data.
For example, you have a column `IsMobile` in your table with values 0 and 1. In transformed data, it will have the same value.

So, the user will be able to count the exact ratio of mobile traffic.

Let's give another example. When you have some private data in your table, like user email and you don't want to publish any single email address.
If your table is large enough and contains multiple different emails and no email has a very high frequency than all others, it will anonymize all data. But if you have a small number of different values in a column, it can reproduce some of them.
You should look at the working algorithm of this tool works, and fine-tune its command line parameters.

This tool works fine only with an average amount of data (at least 1000s of rows).
