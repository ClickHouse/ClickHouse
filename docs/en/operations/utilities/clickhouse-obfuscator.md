# ClickHouse obfuscator

Simple tool for table data obfuscation.

It reads input table and produces output table, that retain some properties of input, but contains different data.
It allows to publish almost real production data for usage in benchmarks.

It is designed to retain the following properties of data:
- cardinalities of values (number of distinct values) for every column and for every tuple of columns;
- conditional cardinalities: number of distinct values of one column under condition on value of another column;
- probability distributions of absolute value of integers; sign of signed integers; exponent and sign for floats;
- probability distributions of length of strings;
- probability of zero values of numbers; empty strings and arrays, NULLs;
- data compression ratio when compressed with LZ77 and entropy family of codecs;
- continuity (magnitude of difference) of time values across table; continuity of floating point values.
- date component of DateTime values;
- UTF-8 validity of string values;
- string values continue to look somewhat natural.

Most of the properties above are viable for performance testing:

reading data, filtering, aggregation and sorting will work at almost the same speed
as on original data due to saved cardinalities, magnitudes, compression ratios, etc.

It works in deterministic fashion: you define a seed value and transform is totally determined by input data and by seed.
Some transforms are one to one and could be reversed, so you need to have large enough seed and keep it in secret.

It use some cryptographic primitives to transform data, but from the cryptographic point of view,
It doesn't do anything properly and you should never consider the result as secure, unless you have other reasons for it.

It may retain some data you don't want to publish.

It always leave numbers 0, 1, -1 as is. Also it leaves dates, lengths of arrays and null flags exactly as in source data.
For example, you have a column IsMobile in your table with values 0 and 1. In transformed data, it will have the same value.
So, the user will be able to count exact ratio of mobile traffic.

Another example, suppose you have some private data in your table, like user email and you don't want to publish any single email address.
If your table is large enough and contain multiple different emails and there is no email that have very high frequency than all others,
It will perfectly anonymize all data. But if you have small amount of different values in a column, it can possibly reproduce some of them.
And you should take care and look at exact algorithm, how this tool works, and probably fine tune some of it command line parameters.

This tool works fine only with reasonable amount of data (at least 1000s of rows).
