# Operators

All operators are transformed to the corresponding functions at the query parsing stage, in accordance with their precedence and associativity.
Groups of operators are listed in order of priority (the higher it is in the list, the earlier the operator is connected to its arguments).

## Access Operators

`a[N]`  Access to an element of an array; ` arrayElement(a, N) function`.

`a.N` – Access to a tuble element; `tupleElement(a, N)` function.

## Numeric Negation Operator

`-a`  – The `negate (a)` function.

## Multiplication and Division Operators

`a * b`  – The `multiply (a, b) function.`

`a / b`  – The ` divide(a, b) function.`

`a % b` – The `modulo(a, b) function.`

## Addition and Subtraction Operators

`a + b` – The `plus(a, b) function.`

`a - b`  – The `minus(a, b) function.`

## Comparison Operators

`a = b` – The `equals(a, b) function.`

`a == b` – The ` equals(a, b) function.`

`a != b` – The `notEquals(a, b) function.`

`a <> b` – The `notEquals(a, b) function.`

`a <= b` – The `lessOrEquals(a, b) function.`

`a >= b` – The `greaterOrEquals(a, b) function.`

`a < b` – The `less(a, b) function.`

`a > b` – The `greater(a, b) function.`

`a LIKE s` – The `like(a, b) function.`

`a NOT LIKE s` – The `notLike(a, b) function.`

`a BETWEEN b AND c` – The same as `a >= b AND a <= c.`

## Operators for Working With Data Sets

*See the section "IN operators".*

`a IN ...` – The `in(a, b) function`

`a NOT IN ...` – The `notIn(a, b) function.`

`a GLOBAL IN ...` – The `globalIn(a, b) function.`

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b) function.`

## Logical Negation Operator

`NOT a` The `not(a) function.`

## Logical AND Operator

`a AND b` – The`and(a, b) function.`

## Logical OR Operator

`a OR b` – The `or(a, b) function.`

## Conditional Operator

`a ? b : c` – The `if(a, b, c) function.`

Note:

The conditional operator calculates the values of b and c, then checks whether condition a is met, and then returns the corresponding value. If "b" or "c" is an arrayJoin() function, each row will be replicated regardless of the "a" condition.

## Conditional Expression

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    ELSE c
END
```

If "x" is specified, then transform(x, \[a, ...\], \[b, ...\], c). Otherwise – multiIf(a, b, ..., c).

## Concatenation Operator

`s1 || s2` – The `concat(s1, s2) function.`

## Lambda Creation Operator

`x -> expr` – The `lambda(x, expr) function.`

The following operators do not have a priority, since they are brackets:

## Array Creation Operator

`[x1, ...]` – The `array(x1, ...) function.`

## Tuple Creation Operator

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## Associativity

All binary operators have left associativity. For example, `1 + 2 + 3` is transformed to `plus(plus(1, 2), 3)`.
Sometimes this doesn't work the way you expect. For example, ` SELECT 4 > 2 > 3`  will result in 0.

For efficiency, the `and` and `or` functions accept any number of arguments. The corresponding chains of `AND` and `OR` operators are transformed to a single call of these functions.

