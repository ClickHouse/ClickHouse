---
alias: []
description: 'Documentation for the NetCDF format'
input_format: true
keywords: ['NetCDF', 'CDF', 'climate', 'scientific data']
output_format: true
slug: /interfaces/formats/NetCDF
title: 'NetCDF'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[NetCDF](https://www.unidata.ucar.edu/software/netcdf/) is a self-describing binary format for
multidimensional arrays, used mostly for climate, weather, oceanographic and other scientific data.

ClickHouse supports the three "classic" versions of the format: CDF-1 (the original classic format),
CDF-2 (the 64-bit offset format) and CDF-5 (the 64-bit data format). The NetCDF-4 format, which is a
different format built on top of HDF5, is not supported; convert such a file first, for example with
`nccopy -k cdf5 input.nc output.nc`.

## Data model {#data-model}

A NetCDF file is a set of named multidimensional arrays, called variables, over a set of named
dimensions. Every variable becomes a column, and the rows enumerate the Cartesian product of all the
dimensions that the variables use. A variable that does not use some of these dimensions has the
same value repeated for every index along them. This is the same table that the `to_dataframe`
method of the [`xarray`](https://docs.xarray.dev/) library produces.

For example, a file with the dimensions `time`, `lat`, `lon` and the variables `time(time)`,
`lat(lat)`, `lon(lon)` and `temperature(time, lat, lon)` is read as a table with the columns `time`,
`lat`, `lon`, `temperature` and `time * lat * lon` rows, where the coordinate columns repeat.

The order of the dimensions in a row follows the order they have in the variables, so a variable
that uses all of them is read sequentially. The rows enumerate the last dimension first, which is
the order in which a NetCDF file stores its data.

The classic format has no string type. A `char` variable is read as a `String` column whose length
is the last dimension of the variable, so `char station_name(station, name_length)` is read as one
string per station. The trailing zero bytes that pad a shorter string are removed.

The last dimension of a `char` variable is taken as the length of the strings only when nothing else
in the file needs it as a dimension of the row space: it has no variable of its own, it is not the
unlimited dimension, and it is used nowhere but as the last dimension of a `char` variable. This is
the same condition that the `concat_characters` option of `xarray` documents. Otherwise, as in
`char station(station)`, the dimension stays in the row space and the variable is read as one
character per row.

Attributes of the file and of the variables are not part of the table, with the exception of
`_FillValue` and `missing_value`, which are used by
[`input_format_netcdf_fill_value_as_null`](/operations/settings/settings-formats.md/#input_format_netcdf_fill_value_as_null).
In particular, the `scale_factor` and `add_offset` attributes of the CF conventions are not applied:
a packed variable is read as the integers that are stored in the file.

## Data types matching {#data_types-matching}

| NetCDF data type (`INSERT`) | ClickHouse data type                                    | NetCDF data type (`SELECT`) |
|-----------------------------|---------------------------------------------------------|-----------------------------|
| `byte`                      | [Int8](/sql-reference/data-types/int-uint.md)           | `byte`                      |
| `short`                     | [Int16](/sql-reference/data-types/int-uint.md)          | `short`                     |
| `int`                       | [Int32](/sql-reference/data-types/int-uint.md)          | `int`                       |
| `int64`                     | [Int64](/sql-reference/data-types/int-uint.md)          | `int64`                     |
| `ubyte`                     | [UInt8](/sql-reference/data-types/int-uint.md)          | `ubyte`                     |
| `ushort`                    | [UInt16](/sql-reference/data-types/int-uint.md)         | `ushort`                    |
| `uint`                      | [UInt32](/sql-reference/data-types/int-uint.md)         | `uint`                      |
| `uint64`                    | [UInt64](/sql-reference/data-types/int-uint.md)         | `uint64`                    |
| `float`                     | [Float32](/sql-reference/data-types/float.md)           | `float`                     |
| `double`                    | [Float64](/sql-reference/data-types/float.md)           | `double`                    |
| `char`                      | [String](/sql-reference/data-types/string.md)           | `char`                      |
|                             | [FixedString](/sql-reference/data-types/fixedstring.md) | `char`                      |
|                             | [Enum8](/sql-reference/data-types/enum.md)              | `byte`                      |
|                             | [Enum16](/sql-reference/data-types/enum.md)             | `short`                     |
|                             | [Date](/sql-reference/data-types/date.md)               | `ushort`                    |
|                             | [Date32](/sql-reference/data-types/date32.md)           | `int`                       |
|                             | [DateTime](/sql-reference/data-types/datetime.md)       | `uint`                      |
|                             | [DateTime64](/sql-reference/data-types/datetime64.md)   | `int64`                     |

The types `ubyte`, `ushort`, `uint`, `int64` and `uint64` exist only in CDF-5.

A column with dates or times is written as the number that is stored in it, together with the
`units` attribute of the [CF conventions](https://cfconventions.org/) that says what that number
means, such as `days since 1970-01-01`. On reading, such a variable is a plain number again.

## Reading a file {#reading}

```sql title="Query"
SELECT * FROM file('temperature.nc') ORDER BY time, lat, lon LIMIT 3
```

```response title="Response"
┌─time─┬─lat─┬──lon─┬─temperature─┐
│    0 │ -90 │ -180 │      241.75 │
│    0 │ -90 │ -179 │      241.81 │
│    0 │ -90 │ -178 │      241.87 │
└──────┴─────┴──────┴─────────────┘
```

Only the variables that a query needs are read from the file, so selecting a few columns out of a
file with many variables does not read the rest of them.

## Writing a file {#writing}

```sql title="Query"
SELECT * FROM measurements INTO OUTFILE 'measurements.nc' FORMAT NetCDF
```

Every column becomes a one-dimensional variable over a single dimension named `row`, and a `String`
column additionally gets a dimension that holds the length of the longest string in it. A file
written by ClickHouse is therefore read back with the same structure.

A string shorter than its dimension is padded with zero bytes, as the format prescribes, so a
`String` or `FixedString` value that itself ends in a zero byte cannot be stored: it would be read
back without its trailing zero bytes by every implementation of the format. Writing such a value
throws an exception instead of corrupting it.

The names of the classic format are UTF-8 text that begins with a letter, a digit, an underscore or
a character outside of ASCII, and contains no slashes, no control characters and no trailing spaces.
A column whose name is not one of these - including a name that is not valid UTF-8, which a quoted
identifier of ClickHouse may be - cannot be written, and throws an exception.

The offsets of the data of the variables are a part of the header, and the header is at the
beginning of the file, so the whole result is kept in memory until the query finishes.

The version of the format is chosen automatically: CDF-5 when a column needs one of the types that
only CDF-5 has, or when a number that the header of a CDF-2 file writes as a 32-bit value - the
length of a dimension or the size of a variable - does not fit into it, and CDF-2 otherwise.

A [Nullable](/sql-reference/data-types/nullable.md) column is written with the `_FillValue`
attribute, which is the way the format marks missing data. The value of the attribute is the
default fill value of the type of the netCDF library, or, when the data of the column contains that
value, another value that the data does not contain, so that a value of the column is never read
back as a `NULL`.
Read the file back with
[`input_format_netcdf_fill_value_as_null`](/operations/settings/settings-formats.md/#input_format_netcdf_fill_value_as_null)
to get the `NULL`s again. A `NULL` in a `String` column is written as an empty string, because the
format has no way to mark a missing string.

## Format settings {#format-settings}

| Setting                                                                                                                        | Description                                                                                     | Default |
|--------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|---------|
| [`input_format_netcdf_fill_value_as_null`](/operations/settings/settings-formats.md/#input_format_netcdf_fill_value_as_null)         | Read the values equal to the `_FillValue` or `missing_value` attribute of a variable as `NULL`. | `false` |
| [`input_format_netcdf_add_dimension_columns`](/operations/settings/settings-formats.md/#input_format_netcdf_add_dimension_columns)   | Add a column with the index along every dimension that has no coordinate variable.               | `false` |
