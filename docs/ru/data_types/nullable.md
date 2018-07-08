<a name="data_type-nullable"></a>

# Nullable(TypeName)

Позволяет хранить в таблице [NULL](../query_language/syntax.md#null-literal) вместо значения типа `TypeName`. Например, `Nullable(Int8)` позволяет хранить `NULL` вместо значений `Int8`.

В качестве `TypeName` нельзя использовать составные типы данных [Array](array.md#data_type-array) и [Tuple](tuple.md#data_type-tuple). Составные типы данных могут содержать значения типа `Nullable`, например `Array(Nullable(Int8))`.

Поле типа `Nullable` нельзя включать в индексы.

`NULL` — значение по умолчанию для типа `Nullable`, если в конфигурации сервера ClickHouse не указано иное.

##Особенности хранения

Для хранения значения типа `Nullable` ClickHouse использует:

- Отдельный файл с масками `NULL` (далее маска).
- Непосредственно файл со значениями.

Маска определяет, что лежит в ячейке данных: `NULL` или значение.

В случае, когда маска указывает, что в ячейке хранится `NULL`, в файле значений хранится значение по умолчанию для типа данных. Т.е. если, например, поле имеет тип `Nullable(Int8)`, то ячейка будет хранить значение по умолчанию для `Int8`. Эта особенность увеличивает размер хранилища. Также для некоторых операций возможно снижение производительности. Будьте внимательны при проектировании системы хранения.

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
