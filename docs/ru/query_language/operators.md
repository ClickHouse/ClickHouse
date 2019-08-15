# Операторы

Все операторы преобразуются в соответствующие функции на этапе парсинга запроса, с учётом их приоритетов и ассоциативности.
Далее будут перечислены группы операторов в порядке их приоритета (чем выше, тем раньше оператор связывается со своими аргументами).

## Операторы доступа

`a[N]` - доступ к элементу массива, функция `arrayElement(a, N)`.

`a.N` - доступ к элементу кортежа, функция `tupleElement(a, N)`.

## Оператор числового отрицания

`-a` - функция `negate(a)`.

## Операторы умножения и деления

`a * b` - функция `multiply(a, b)`

`a / b` - функция `divide(a, b)`

`a % b` - функция `modulo(a, b)`

## Операторы сложения и вычитания

`a + b` - функция `plus(a, b)`

`a - b` - функция `minus(a, b)`

## Операторы сравнения

`a = b` - функция `equals(a, b)`

`a == b` - функция `equals(a, b)`

`a != b` - функция `notEquals(a, b)`

`a <> b` - функция `notEquals(a, b)`

`a <= b` - функция `lessOrEquals(a, b)`

`a >= b` - функция `greaterOrEquals(a, b)`

`a < b` - функция `less(a, b)`

`a > b` - функция `greater(a, b)`

`a LIKE s` - функция `like(a, b)`

`a NOT LIKE s` - функция `notLike(a, b)`

`a BETWEEN b AND c` - равнозначно `a >= b AND a <= c`

`a NOT BETWEEN b AND c` - равнозначно `a < b OR a > c`

## Операторы для работы с множествами

*Смотрите раздел [Операторы IN](select.md#select-in-operators).*

`a IN ...` - функция `in(a, b)`

`a NOT IN ...` - функция `notIn(a, b)`

`a GLOBAL IN ...` - функция `globalIn(a, b)`

`a GLOBAL NOT IN ...` - функция `globalNotIn(a, b)`

## Оператор для работы с датами и временем

``` sql
EXTRACT(part FROM date);
```

Позволяет извлечь отдельные части из переданной даты. Например, можно получить месяц из даты, или минуты из времени. 

В параметре `part` указывается, какой фрагмент даты нужно получить. Доступные значения:

- `DAY` — День. Возможные значения: 1–31.
- `MONTH` — Номер месяца. Возможные значения: 1–12.
- `YEAR` — Год.
- `SECOND` — Секунда. Возможные значения: 0–59.
- `MINUTE` — Минута. Возможные значения: 0–59.
- `HOUR` — Час. Возможные значения: 0–23.

Эти значения могут быть указаны также в нижнем регистре (`day`, `month`).

В параметре `date` указывается исходная дата. Поддерживаются типы [Date](../data_types/date.md) и [DateTime](../data_types/datetime.md).

Примеры:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

В следующем примере создадим таблицу и добавим в неё значение с типом `DateTime`.

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64, 
    OrderName String, 
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```
``` sql
SELECT 
    toYear(OrderDate) AS OrderYear, 
    toMonth(OrderDate) AS OrderMonth, 
    toDayOfMonth(OrderDate) AS OrderDay, 
    toHour(OrderDate) AS OrderHour, 
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;

┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Больше примеров приведено в [тестах](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00619_extract.sql).

## Оператор логического отрицания

`NOT a` - функция `not(a)`

## Оператор логического 'И'

`a AND b` - функция `and(a, b)`

## Оператор логического 'ИЛИ'

`a OR b` - функция `or(a, b)`

## Условный оператор

`a ? b : c` - функция `if(a, b, c)`

Примечание:

Условный оператор сначала вычисляет значения b и c, затем проверяет выполнение условия a, и только после этого возвращает соответствующее значение. Если в качестве b или с выступает функция [arrayJoin()](functions/array_join.md#functions_arrayjoin), то размножение каждой строки произойдет вне зависимости от условия а.

## Условное выражение {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

В случае указания `x` - функция `transform(x, [a, ...], [b, ...], c)`. Иначе — `multiIf(a, b, ..., c)`.
При отсутствии секции `ELSE c`, значением по умолчанию будет `NULL`.

!!! note "Примечание"
    Функция `transform` не умеет работать с `NULL`.

## Оператор склеивания строк

`s1 || s2` - функция `concat(s1, s2)`

## Оператор создания лямбда-выражения

`x -> expr` - функция `lambda(x, expr)`

Следующие операторы не имеют приоритета, так как представляют собой скобки:

## Оператор создания массива

`[x1, ...]` - функция `array(x1, ...)`

## Оператор создания кортежа

`(x1, x2, ...)` - функция `tuple(x2, x2, ...)`

## Ассоциативность

Все бинарные операторы имеют левую ассоциативность. Например, `1 + 2 + 3` преобразуется в `plus(plus(1, 2), 3)`.
Иногда это работает не так, как ожидается. Например, `SELECT 4 > 3 > 2` выдаст 0.

Для эффективности, реализованы функции `and` и `or`, принимающие произвольное количество аргументов. Соответствующие цепочки операторов `AND` и `OR`, преобразуются в один вызов этих функций.

## Проверка на `NULL`

ClickHouse поддерживает операторы `IS NULL` и `IS NOT NULL`.

### IS NULL {#operator-is-null}

- Для значений типа [Nullable](../data_types/nullable.md) оператор `IS NULL` возвращает:
    - `1`, если значение — `NULL`.
    - `0` в обратном случае.
- Для прочих значений оператор `IS NULL` всегда возвращает `0`.

```bash
:) SELECT x+100 FROM t_null WHERE y IS NULL

SELECT x + 100
FROM t_null
WHERE isNull(y)

┌─plus(x, 100)─┐
│          101 │
└──────────────┘

1 rows in set. Elapsed: 0.002 sec.
```


### IS NOT NULL

- Для значений типа [Nullable](../data_types/nullable.md) оператор `IS NOT NULL` возвращает:
    - `0`, если значение — `NULL`.
    - `1`, в обратном случае.
- Для прочих значений оператор `IS NOT NULL` всегда возвращает `1`.

```bash
:) SELECT * FROM t_null WHERE y IS NOT NULL

SELECT *
FROM t_null
WHERE isNotNull(y)

┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘

1 rows in set. Elapsed: 0.002 sec.
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/operators/) <!--hide-->
