<a name="data_type-nullable"></a>

# Nullable(TypeName)

Позволяет хранить в таблице [NULL](../query_language/syntax.md#null-literal) вместо значения типа `TypeName`.

В качестве `TypeName` нельзя использовать составные типы данных [Array](array.md#data_type-array) и [Tuple](typle.md#data_type-tuple). Составные типы данных могут содержать значения типа `Nullable`, например `Array(Nullable(Int8))`.

Поле типа `Nullable` нельзя включать в индексы.

## Пример использования

```
:) CREATE TABLE t_null(x Int8, y Nullable(Int8)) engine TinyLog

CREATE TABLE t_null
(
    x Int8,
    y Nullable(Int8)
)
ENGINE = TinyLog

Ok.

0 rows in set. Elapsed: 0.012 sec.

:) INSERT INTO t_null VALUES (1, NULL)

INSERT INTO t_null VALUES

Ok.

1 rows in set. Elapsed: 0.007 sec.

:) SELECT x+y from t_null

SELECT x + y
FROM t_null

┌─plus(x, y)─┐
│         \N │
└────────────┘

1 rows in set. Elapsed: 0.009 sec.

```
