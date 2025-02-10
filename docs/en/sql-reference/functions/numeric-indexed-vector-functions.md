---
slug: /en/sql-reference/functions/numeric-indexed-vector-functions
---

# NumericIndexedVector

NumericIndexedVector is a data structure that compresses vectors using a bitmap and a Bit-Sliced Index, and implements various pointwise operations directly on the compressed data. This significantly improves both storage and query efficiency.

A vector contains indices and their corresponding element values. The following are some characteristics and constraints of this data structure:

- The Index type can be one of UInt8, UInt16, or UInt32. **Note:** The index only supports sizes up to UInt32; UInt64 and Int64 are not supported at this time.
- The Value type can be one of Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, or Float64. **Note:** The Value type does not automatically expand. For example, if you use UInt8 as the Value type, any sum that exceeds the capacity of UInt8 will result in an overflow rather than being promoted to a higher type; similarly, operations on integers will yield integer results (e.g., division will not automatically convert to a floating-point result). Therefore, it is important to plan and design the Value type ahead of time. In real-world scenarios, floating-point types (Float32/Float64) are commonly used.
- The underlying storage uses a Bit-Sliced Index, with the bitmap storing the indexes. For more details, please refer to the paper [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).
- The Bit-Sliced Index mechanism converts Value into binary. For floating-point types, the conversion uses fixed-point representation, which may lead to precision loss. The precision can be adjusted by customizing the number of bits allocated for the fractional part; the default is 24 bits, which is sufficient for most scenarios. You can customize the number of integer bits and fractional bits when constructed NumericIndexedVector using aggregate function groupNumericIndexedVector with `-State`.
- In pointwise operations between two NumericIndexedVectors, any index that is missing in one of the vectors is treated as 0 (resulting in outcomes such as preserving the value from the vector that has the index when adding, or producing 0 when multiplying).

There are two ways to create this structure: one is to use the aggregate function groupNumericIndexedVector with `-State`, and the other is to build it from a map using `numericIndexedVectorBuild`. The `groupNumericIndexedVectorState` function allows customization of the number of integer and fractional bits through parameters, while `numericIndexedVectorBuild` does not currently support such customization.

# numericIndexedVectorBuild

Creates a NumericIndexedVector from a map. The map’s keys represent the Index and the map’s value represents the element value.

Syntax

```sql
numericIndexedVectorBuild(map)
```

Arguments

- `map` – A mapping from Index to Value.

Example

```sql
SELECT numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])) AS res, toTypeName(res);
```

Result

```text
┌─res─┬─toTypeName(res)────────────────────────────────────────────┐
│         │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)   │
└─────┴────────────────────────────────────────────────────────────┘
```

# numericIndexedVectorToMap

Converts a NumericIndexedVector to a map.

Syntax

```sql
numericIndexedVectorToMap(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
```

Result

```text
┌─res──────────────┐
│ {1:10,2:20,3:30}   │
└──────────────────┘
```

# numericIndexedVectorCardinality

Returns the cardinality (the number of unique indexes) of the NumericIndexedVector.

Syntax

```sql
numericIndexedVectorCardinality(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorCardinality(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
```

Result

```text
┌─res─┐
│  3     │
└─────┘
```

# numericIndexedVectorAllValueSum

Returns the sum of all the Value elements in the NumericIndexedVector.

Syntax

```sql
numericIndexedVectorAllValueSum(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorAllValueSum(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
```

Result

```text
┌─res─┐
│  60   │
└─────┘
```


# numericIndexedVectorGetValue

Retrieves the Value corresponding to a specified Index.

Syntax

```sql
numericIndexedVectorGetValue(numericIndexedVector, index)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `index` – The index for which the Value is to be retrieved.

Example

```sql
SELECT numericIndexedVectorGetValue(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])), 3) AS res;
```

Result

```text
┌─res─┐
│  30   │
└─────┘
```

# numericIndexedVectorPointwiseAdd

Performs pointwise addition on two NumericIndexedVectors. The result is a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseAdd(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseAdd(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], [10, 20, 30]))
    )
) AS res;
```

Result

```text
┌─res───────────────────┐
│ {1:10,2:30,3:50,4:30}  │
└───────────────────────┘
```

# numericIndexedVectorPointwiseSubtract

Performs pointwise subtraction on two NumericIndexedVectors. The result is a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseSubtract(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseSubtract(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30])))
    )
) AS res;
```

Result

```text
┌─res───────────────────┐
│ {1:10,2:10,3:10,4:-30} │
└───────────────────────┘
```

# numericIndexedVectorPointwiseMultiply

Performs pointwise multiplication on two NumericIndexedVectors. The result is a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseMultiply(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseMultiply(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30])))
    )
) AS res;
```

Result

```text
┌─res───────────┐
│ {2:200,3:600}  │
└───────────────┘
```

# numericIndexedVectorPointwiseDivide

Performs pointwise division on two NumericIndexedVectors. The result is a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseDivide(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseDivide(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [10, 20, 30])))
    )
) AS res;
```

Result

```text
┌─res─────────┐
│ {2:2,3:1.5}  │
└─────────────┘
```

# numericIndexedVectorPointwiseEqual

Performs a pointwise comparison between two NumericIndexedVectors. The result is a NumericIndexedVector containing the indices where the values are equal, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseEqual(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30])))
    )
) AS res;
```

Result

```text
┌─res───┐
│ {2:1}  │
└───────┘
```

# numericIndexedVectorPointwiseNotEqual

Performs a pointwise comparison between two NumericIndexedVectors. The result is a NumericIndexedVector containing the indices where the values are not equal, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseNotEqual(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseNotEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30])))
    )
) AS res;
```

Result

```text
┌─res───────────┐
│ {1:1,3:1,4:1}  │
└───────────────┘
```

# numericIndexedVectorPointwiseLess

Performs a pointwise comparison between two NumericIndexedVectors. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseLess(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseLess(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) AS res;
```

Result

```text
┌─res───────┐
│ {3:1,4:1}  │
└───────────┘
```

# numericIndexedVectorPointwiseLessEqual

Performs a pointwise comparison between two NumericIndexedVectors. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than or equal to the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseLessEqual(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseLessEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) AS res;
```

Result

```text
┌─res───────────┐
│ {2:1,3:1,4:1}  │
└───────────────┘
```

# numericIndexedVectorPointwiseGreater

Performs a pointwise comparison between two NumericIndexedVectors. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseGreater(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseGreater(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) AS res;
```

Result

```text
┌─res───────┐
│ {1:1,3:1}  │
└───────────┘
```

# numericIndexedVectorPointwiseGreaterEqual

Performs a pointwise comparison between two NumericIndexedVectors. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than or equal to the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseGreaterEqual(numericIndexedVector, numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseGreaterEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) AS res;
```

Result

```text
┌─res───────────┐
│ {1:1,2:1,3:1}  │
└───────────────┘
```

