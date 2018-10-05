# AggregatingMergeTree

Отличается от `MergeTree` тем, что при слиянии, выполняет объединение состояний агрегатных функций, хранимых в таблице, для строчек с одинаковым значением первичного ключа.

Чтобы это работало, используются: тип данных `AggregateFunction`, а также модификаторы `-State` и `-Merge` для агрегатных функций. Рассмотрим подробнее.

Существует тип данных `AggregateFunction`. Это параметрический тип данных. В качестве параметров передаются: имя агрегатной функции, затем типы её аргументов.

Примеры:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

Столбец такого типа хранит состояние агрегатной функции.

Чтобы получить значение такого типа, следует использовать агрегатные функции с суффиксом `State`.

Пример:
`uniqState(UserID), quantilesState(0.5, 0.9)(SendTiming)`

В отличие от соответствующих функций `uniq`, `quantiles`, такие функции возвращают не готовое значение, а состояние. То есть, значение типа `AggregateFunction`.

Значение типа `AggregateFunction` нельзя вывести в Pretty-форматах. В других форматах, значения такого типа выводятся в виде implementation-specific бинарных данных. То есть, значения типа `AggregateFunction` не предназначены для вывода, сохранения в дамп.

Единственную полезную вещь, которую можно сделать со значениями типа `AggregateFunction` — это объединить состояния и получить результат, по сути — доагрегировать до конца. Для этого используются агрегатные функции с суффиксом Merge.
Пример: `uniqMerge(UserIDState)`, где `UserIDState` имеет тип `AggregateFunction`.

То есть, агрегатная функция с суффиксом Merge берёт множество состояний, объединяет их, и возвращает готовый результат.
Для примера, эти два запроса возвращают один и тот же результат:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

Существует движок `AggregatingMergeTree`. Он занимается тем, что при слияниях, выполняет объединение состояний агрегатных функций из разных строчек таблицы с одним значением первичного ключа.

В таблицу, содержащую столбцы типа `AggregateFunction` невозможно вставить строчку обычным запросом INSERT, так как невозможно явно указать значение типа `AggregateFunction`. Вместо этого, для вставки данных, следует использовать `INSERT SELECT` с агрегатными функциями `-State`.

При SELECT-е из таблицы `AggregatingMergeTree`, используйте GROUP BY и агрегатные функции с модификатором -Merge, чтобы доагрегировать данные.

Таблицы типа `AggregatingMergeTree` могут использоваться для инкрементальной агрегации данных, в том числе, для агрегирующих материализованных представлений.

Пример:

Создаём материализованное представление типа `AggregatingMergeTree`, следящее за таблицей `test.visits`:

```sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Вставляем данные в таблицу `test.visits`. Данные будут также вставлены в представление, где они будут агрегированы:

```sql
INSERT INTO test.visits ...
```

Делаем `SELECT` из представления, используя `GROUP BY`, чтобы доагрегировать данные:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

Вы можете создать такое материализованное представление и навесить на него обычное представление, выполняющее доагрегацию данных.

Заметим, что в большинстве случаев, использование `AggregatingMergeTree` является неоправданным, так как можно достаточно эффективно выполнять запросы по неагрегированным данным.
