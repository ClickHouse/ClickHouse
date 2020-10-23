# Bit functions

Bit functions work for any pair of types from UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, or Float64.

The result type is an integer with bits equal to the maximum bits of its arguments. If at least one of the arguments is signed, the result is a signed number. If an argument is a floating-point number, it is cast to Int64.

## bitAnd(a, b)

## bitOr(a, b)

## bitXor(a, b)

## bitNot(a)

## bitShiftLeft(a, b)

## bitShiftRight(a, b)

## bitRotateLeft(a, b)

## bitRotateRight(a, b)

## bitTest {#bittest}

Takes any integer and converts it into [binary form](https://en.wikipedia.org/wiki/Binary_number), returns the value of a bit at specified position. The countdown starts from 0 from the right to the left.

**Syntax** 

```sql
SELECT bitTest(number, index)
```

**Parameters**

- `number` – integer number.
- `index` – position of bit. 

**Returned values**

Returns a value of bit at specified position.

Type: `UInt8`.

**Example**

For example, the number 43 in base-2 (binary) numeral system is 101011.

Query:

```sql
SELECT bitTest(43, 1)
```

Result:

```text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

Another example:

Query:

```sql
SELECT bitTest(43, 2)
```

Result:

```text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

Returns result of [logical conjuction](https://en.wikipedia.org/wiki/Logical_conjunction) (AND operator) of all bits at given positions. The countdown starts from 0 from the right to the left.

The conjuction for bitwise operations:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**Syntax** 

```sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**Parameters** 

- `number` – integer number.
- `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`) is true if and only if all of its positions are true (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**Returned values**

Returns result of logical conjuction.

Type: `UInt8`.

**Example**

For example, the number 43 in base-2 (binary) numeral system is 101011.

Query:

```sql
SELECT bitTestAll(43, 0, 1, 3, 5)
```

Result:

```text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

Another example:

Query:

```sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2)
```

Result:

```text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

Returns result of [logical disjunction](https://en.wikipedia.org/wiki/Logical_disjunction) (OR operator) of all bits at given positions. The countdown starts from 0 from the right to the left.

The disjunction for bitwise operations:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**Syntax** 

```sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**Parameters** 

- `number` – integer number.
- `index1`, `index2`, `index3`, `index4` – positions of bit.

**Returned values**

Returns result of logical disjuction.

Type: `UInt8`.

**Example**

For example, the number 43 in base-2 (binary) numeral system is 101011.

Query:

```sql
SELECT bitTestAny(43, 0, 2)
```

Result:

```text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

Another example:

Query:

```sql
SELECT bitTestAny(43, 4, 2)
```

Result:

```text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/query_language/functions/bit_functions/) <!--hide-->
