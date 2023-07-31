# Distance functions

## L1Norm

Calculates the sum of absolute values of a vector.

**Syntax**

```sql
L1Norm(vector)
```

Alias: `normL1`.

**Arguments**

-   `vector` — [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   L1-norm or [taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry) distance.

Type: [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Examples**

Query:

```sql
SELECT L1Norm((1, 2));
```

Result:

```text
┌─L1Norm((1, 2))─┐
│              3 │
└────────────────┘
```

## L2Norm

Calculates the square root of the sum of the squares of the vector values.

**Syntax**

```sql
L2Norm(vector)
```

Alias: `normL2`.

**Arguments**

-   `vector` — [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   L2-norm or [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance).

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L2Norm((1, 2));
```

Result:

```text
┌───L2Norm((1, 2))─┐
│ 2.23606797749979 │
└──────────────────┘
```

## LinfNorm

Calculates the maximum of absolute values of a vector.

**Syntax**

```sql
LinfNorm(vector)
```

Alias: `normLinf`.

**Arguments**

-   `vector` — [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   Linf-norm or the maximum absolute value.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LinfNorm((1, -2));
```

Result:

```text
┌─LinfNorm((1, -2))─┐
│                 2 │
└───────────────────┘
```

## LpNorm

Calculates the root of `p`-th power of the sum of the absolute values of a vector in the power of `p`.

**Syntax**

```sql
LpNorm(vector, p)
```

Alias: `normLp`.

**Arguments**

-   `vector` — [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).
-   `p` — The power. Possible values: real number in `[1; inf)`. [UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   [Lp-norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LpNorm((1, -2), 2);
```

Result:

```text
┌─LpNorm((1, -2), 2)─┐
│   2.23606797749979 │
└────────────────────┘
```

## L1Distance

Calculates the distance between two points (the values of the vectors are the coordinates) in `L1` space (1-norm ([taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry) distance)).

**Syntax**

```sql
L1Distance(vector1, vector2)
```

Alias: `distanceL1`.

**Arguments**

-   `vector1` — First vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).
-   `vector2` — Second vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   1-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L1Distance((1, 2), (2, 3));
```

Result:

```text
┌─L1Distance((1, 2), (2, 3))─┐
│                          2 │
└────────────────────────────┘
```

## L2Distance

Calculates the distance between two points (the values of the vectors are the coordinates) in Euclidean space ([Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).

**Syntax**

```sql
L2Distance(vector1, vector2)
```

Alias: `distanceL2`.

**Arguments**

-   `vector1` — First vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).
-   `vector2` — Second vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   2-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L2Distance((1, 2), (2, 3));
```

Result:

```text
┌─L2Distance((1, 2), (2, 3))─┐
│         1.4142135623730951 │
└────────────────────────────┘
```

## LinfDistance

Calculates the distance between two points (the values of the vectors are the coordinates) in `L_{inf}` space ([maximum norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm))).

**Syntax**

```sql
LinfDistance(vector1, vector2)
```

Alias: `distanceLinf`.

**Arguments**

-   `vector1` — First vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).
-   `vector1` — Second vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   Infinity-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LinfDistance((1, 2), (2, 3));
```

Result:

```text
┌─LinfDistance((1, 2), (2, 3))─┐
│                            1 │
└──────────────────────────────┘
```

## LpDistance

Calculates the distance between two points (the values of the vectors are the coordinates) in `Lp` space ([p-norm distance](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)).

**Syntax**

```sql
LpDistance(vector1, vector2, p)
```

Alias: `distanceLp`.

**Arguments**

-   `vector1` — First vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).
-   `vector2` — Second vector. [Tuple](../../sql-reference/data-types/tuple.md) or [Array](../../sql-reference/data-types/array.md).
-   `p` — The power. Possible values: real number from `[1; inf)`. [UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   p-norm distance.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LpDistance((1, 2), (2, 3), 3);
```

Result:

```text
┌─LpDistance((1, 2), (2, 3), 3)─┐
│            1.2599210498948732 │
└───────────────────────────────┘
```


## L1Normalize

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in `L1` space ([taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry)).

**Syntax**

```sql
L1Normalize(tuple)
```

Alias: `normalizeL1`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L1Normalize((1, 2));
```

Result:

```text
┌─L1Normalize((1, 2))─────────────────────┐
│ (0.3333333333333333,0.6666666666666666) │
└─────────────────────────────────────────┘
```

## L2Normalize

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in Euclidean space (using [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).

**Syntax**

```sql
L2Normalize(tuple)
```

Alias: `normalizeL1`.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT L2Normalize((3, 4));
```

Result:

```text
┌─L2Normalize((3, 4))─┐
│ (0.6,0.8)           │
└─────────────────────┘
```

## LinfNormalize

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in `L_{inf}` space (using [maximum norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm))).

**Syntax**

```sql
LinfNormalize(tuple)
```

Alias: `normalizeLinf `.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LinfNormalize((3, 4));
```

Result:

```text
┌─LinfNormalize((3, 4))─┐
│ (0.75,1)              │
└───────────────────────┘
```

## LpNormalize

Calculates the unit vector of a given vector (the values of the tuple are the coordinates) in `Lp` space (using [p-norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)).

**Syntax**

```sql
LpNormalize(tuple, p)
```

Alias: `normalizeLp `.

**Arguments**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — The power. Possible values: any number from [1;inf). [UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   Unit vector.

Type: [Tuple](../../sql-reference/data-types/tuple.md) of [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT LpNormalize((3, 4),5);
```

Result:

```text
┌─LpNormalize((3, 4), 5)──────────────────┐
│ (0.7187302630182624,0.9583070173576831) │
└─────────────────────────────────────────┘
```

## cosineDistance

Calculates the cosine distance between two vectors (the values of the tuples are the coordinates). The less the returned value is, the more similar are the vectors.

**Syntax**

```sql
cosineDistance(tuple1, tuple2)
```

**Arguments**

-   `tuple1` — First tuple. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — Second tuple. [Tuple](../../sql-reference/data-types/tuple.md).

**Returned value**

-   Cosine of the angle between two vectors substracted from one.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT cosineDistance((1, 2), (2, 3));
```

Result:

```text
┌─cosineDistance((1, 2), (2, 3))─┐
│           0.007722123286332261 │
└────────────────────────────────┘
```
