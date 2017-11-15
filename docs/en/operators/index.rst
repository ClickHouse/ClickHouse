Operators
=========

All operators are transformed to the corresponding functions at the query parsing stage, in accordance with their precedence and associativity.

Access operators
----------------

``a[N]`` - Access to an array element, arrayElement(a, N) function.

``a.N`` - Access to a tuple element, tupleElement(a, N) function.

Numeric negation operator
-------------------------

``-a`` - negate(a) function

Multiplication and division operators
-------------------------------------

``a * b`` - multiply(a, b) function

``a / b`` - divide(a, b) function

``a % b`` - modulo(a, b) function

Addition and subtraction operators
----------------------------------

``a + b`` - plus(a, b) function

``a - b`` - minus(a, b) function

Comparison operators
--------------------

``a = b`` - equals(a, b) function

``a == b`` - equals(a, b) function

``a != b`` - notEquals(a, b) function

``a <> b`` - notEquals(a, b) function

``a <= b`` - lessOrEquals(a, b) function

``a >= b`` - greaterOrEquals(a, b) function

``a < b`` - less(a, b) function

``a > b`` - greater(a, b) function

``a LIKE s`` - like(a, b) function

``a NOT LIKE s`` - notLike(a, b) function

``a BETWEEN b AND c`` - equivalent to a >= b AND a <= c


Operators for working with data sets
------------------------------------

*See the section "IN operators".*

``a IN ...`` - in(a, b) function

``a NOT IN ...`` - notIn(a, b) function

``a GLOBAL IN ...`` - globalIn(a, b) function

``a GLOBAL NOT IN ...`` - globalNotIn(a, b) function



Logical negation operator
-------------------------

``NOT a`` - ``not(a)`` function


Logical "AND" operator
----------------------

``a AND b`` - function ``and(a, b)``


Logical "OR" operator
---------------------

``a OR b`` - function ``or(a, b)``

Conditional operator
--------------------

``a ? b : c`` - function ``if(a, b, c)``

Conditional expression
----------------------

.. code-block:: sql

  CASE [x]
      WHEN a THEN b
      [WHEN ... THEN ...]
      ELSE c
  END

If x is given - transform(x, [a, ...], [b, ...], c). Otherwise, multiIf(a, b, ..., c).

String concatenation operator
-----------------------------

``s1 || s2`` - concat(s1, s2) function

Lambda creation operator
------------------------

``x -> expr`` - lambda(x, expr) function

The following operators do not have a priority, since they are brackets:

Array creation operator
-----------------------

``[x1, ...]`` - array(x1, ...) function

Tuple creation operator
-----------------------
``(x1, x2, ...)`` - tuple(x2, x2, ...) function


Associativity
-------------

All binary operators have left associativity. For example, ``'1 + 2 + 3'`` is transformed to ``'plus(plus(1, 2), 3)'``.
Sometimes this doesn't work the way you expect. For example, ``'SELECT 4 > 3 > 2'`` results in ``0``.

For efficiency, the 'and' and 'or' functions accept any number of arguments. The corresponding chains of AND and OR operators are transformed to a single call of these functions.
