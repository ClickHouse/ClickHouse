---
sidebarTitle: 'Analysis tips'
slug: /integrations/tableau/analysis-tips
keywords: ['clickhouse', 'tableau', 'online', 'mysql', 'connect', 'integrate', 'ui']
description: 'Tableau analysis tips when using ClickHouse official connector.'
title: 'Analysis tips'
doc_type: 'guide'
integration:
  - support_level: 'core'
  - category: 'data_visualization'
---

## MEDIAN() and PERCENTILE() functions {#median-and-percentile-functions}
- In Live mode the MEDIAN() and PERCENTILE() functions (since connector v0.1.3 release) use the [ClickHouse quantile()() function](/reference/functions/aggregate-functions/quantile), which significantly speeds up the calculation, but uses sampling. If you want to get accurate calculation results, then use functions `MEDIAN_EXACT()` and `PERCENTILE_EXACT()` (based on [quantileExact()()](/reference/functions/aggregate-functions/quantileExact)).
- In Extract mode you can't use MEDIAN_EXACT() and PERCENTILE_EXACT() because MEDIAN() and PERCENTILE() are always accurate (and slow).
## Additional functions for calculated fields in Live mode {#additional-functions-for-calculated-fields-in-live-mode}
ClickHouse has a huge number of functions that can be used for data analysis — much more than Tableau supports. For the convenience of users, we have added new functions that are available for use in Live mode when creating Calculated Fields. Unfortunately, it isn't possible to add descriptions to these functions in the Tableau interface, so we will add a description for them right here.
- **[`-If` Aggregation Combinator](/reference/functions/aggregate-functions/combinators#-if)** *(added in v0.2.3)* - allows to have Row-Level Filters right in the Aggregate Calculation. `SUM_IF(), AVG_IF(), COUNT_IF(), MIN_IF() & MAX_IF()` functions have been added.
- **`BAR([my_int], [min_val_int], [max_val_int], [bar_string_length_int])`** *(added in v0.2.1)* — Forget about boring bar charts! Use `BAR()` function instead (equivalent of [`bar()`](/reference/functions/regular-functions/other-functions#bar) in ClickHouse). For example, this calculated field returns nice bars as String:
    ```text
    BAR([my_int], [min_val_int], [max_val_int], [bar_string_length_int]) + "  " + FORMAT_READABLE_QUANTITY([my_int])
    ```
    ```text
    == BAR() ==
    ██████████████████▊  327.06 million
    █████  88.02 million
    ███████████████  259.37 million
    ```
- **`COUNTD_UNIQ([my_field])`** *(added in v0.2.0)* — Calculates the approximate number of different values of the argument. Equivalent of [uniq()](/reference/functions/aggregate-functions/uniq). Much faster than `COUNTD()`.
- **`DATE_BIN('day', 10, [my_datetime_or_date])`** *(added in v0.2.1)* — equivalent of [`toStartOfInterval()`](/reference/functions/regular-functions/date-time-functions#toStartOfInterval) in ClickHouse. Rounds down a Date or Date & Time to the given interval, for example:
    ```text
     == my_datetime_or_date == | == DATE_BIN('day', 10, [my_datetime_or_date]) ==
        28.07.2004 06:54:50    |              21.07.2004 00:00:00
        17.07.2004 14:01:56    |              11.07.2004 00:00:00
        14.07.2004 07:43:00    |              11.07.2004 00:00:00
    ```
- **`FORMAT_READABLE_QUANTITY([my_integer])`** *(added in v0.2.1)* — Returns a rounded number with a suffix (thousand, million, billion, etc.) as a string. It is useful for reading big numbers by human. Equivalent of [`formatReadableQuantity()`](/reference/functions/regular-functions/other-functions#formatReadableQuantity).
- **`FORMAT_READABLE_TIMEDELTA([my_integer_timedelta_sec], [optional_max_unit])`** *(added in v0.2.1)* — Accepts the time delta in seconds. Returns a time delta with (year, month, day, hour, minute, second) as a string. `optional_max_unit` is maximum unit to show. Acceptable values: `seconds`, `minutes`, `hours`, `days`, `months`, `years`. Equivalent of [`formatReadableTimeDelta()`](/reference/functions/regular-functions/other-functions#formatReadableTimeDelta).
- **`GET_SETTING([my_setting_name])`** *(added in v0.2.1)* — Returns the current value of a custom setting. Equivalent of [`getSetting()`](/reference/functions/regular-functions/other-functions#getSetting).
- **`HEX([my_string])`** *(added in v0.2.1)* — Returns a string containing the argument's hexadecimal representation. Equivalent of [`hex()`](/reference/functions/regular-functions/encoding-functions#hex).
- **`KURTOSIS([my_number])`** — Computes the sample kurtosis of a sequence. Equivalent of [`kurtSamp()`](/reference/functions/aggregate-functions/kurtSamp).
- **`KURTOSISP([my_number])`** — Computes the kurtosis of a sequence. The equivalent of [`kurtPop()`](/reference/functions/aggregate-functions/kurtPop).
- **`MEDIAN_EXACT([my_number])`** *(added in v0.1.3)* — Exactly computes the median of a numeric data sequence. Equivalent of [`quantileExact(0.5)(...)`](/reference/functions/aggregate-functions/quantileExact).
- **`MOD([my_number_1], [my_number_2])`** — Calculates the remainder after division. If arguments are floating-point numbers, they're pre-converted to integers by dropping the decimal portion. Equivalent of [`modulo()`](/reference/functions/regular-functions/arithmetic-functions#modulo).
- **`PERCENTILE_EXACT([my_number], [level_float])`** *(added in v0.1.3)* — Exactly computes the percentile of a numeric data sequence. The recommended level range is [0.01, 0.99]. Equivalent of [`quantileExact()()`](/reference/functions/aggregate-functions/quantileExact).
- **`PROPER([my_string])`** *(added in v0.2.5)* - Converts a text string so the first letter of each word is capitalized and the remaining letters are in lowercase. Spaces and non-alphanumeric characters such as punctuation also act as separators. For example:
    ```text
    PROPER("PRODUCT name") => "Product Name"
    ```
    ```text
    PROPER("darcy-mae") => "Darcy-Mae"
    ```
- **`RAND()`** *(added in v0.2.1)* — returns integer (UInt32) number, for example `3446222955`. Equivalent of [`rand()`](/reference/functions/regular-functions/random-functions#rand).
- **`RANDOM()`** *(added in v0.2.1)* — unofficial [`RANDOM()`](https://kb.tableau.com/articles/issue/random-function-produces-inconsistent-results) Tableau function, which returns float between 0 and 1.
- **`RAND_CONSTANT([optional_field])`** *(added in v0.2.1)* — Produces a constant column with a random value. Something like `{RAND()}` Fixed LOD, but faster. Equivalent of [`randConstant()`](/reference/functions/regular-functions/random-functions#randConstant).
- **`REAL([my_number])`** — Casts field to float (Float64). Details [`here`](/reference/data-types/decimal#operations-and-result-type).
- **`SHA256([my_string])`** *(added in v0.2.1)* — Calculates SHA-256 hash from a string and returns the resulting set of bytes as a string (FixedString). Convenient to use with the `HEX()` function, for example, `HEX(SHA256([my_string]))`. Equivalent of [`SHA256()`](/reference/functions/regular-functions/hash-functions#SHA256).
- **`SKEWNESS([my_number])`** — Computes the sample skewness of a sequence. Equivalent of [`skewSamp()`](/reference/functions/aggregate-functions/skewSamp).
- **`SKEWNESSP([my_number])`** — Computes the skewness of a sequence. Equivalent of [`skewPop()`](/reference/functions/aggregate-functions/skewPop).
- **`TO_TYPE_NAME([field])`** *(added in v0.2.1)* — Returns a string containing the ClickHouse type name of the passed argument. Equivalent of [`toTypeName()`](/reference/functions/regular-functions/other-functions#toTypeName).
- **`TRUNC([my_float])`** — It is the same as the `FLOOR([my_float])` function. Equivalent of [`trunc()`](/reference/functions/regular-functions/rounding-functions#trunc).
- **`UNHEX([my_string])`** *(added in v0.2.1)* — Performs the opposite operation of `HEX()`. Equivalent of [`unhex()`](/reference/functions/regular-functions/encoding-functions#unhex).
