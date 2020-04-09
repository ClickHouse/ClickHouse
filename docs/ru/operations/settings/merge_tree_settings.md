# Настройки MergeTree таблиц {#merge-tree-settings}

Значения по умолчанию (для всех таблиц) задаются в config.xml в секции merge_tree.

Пример:
```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

Эти значения можно задать (перекрыть) у таблиц в секции `SETTINGS` у команды `CREATE TABLE`.

Пример:
```sql
CREATE TABLE foo
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

Или изменить с помощью команды `ALTER TABLE ... MODIFY SETTING`.

Пример:
```sql
ALTER TABLE foo
    MODIFY SETTING max_suspicious_broken_parts = 100;
```


## parts_to_throw_insert {#parts-to-throw-insert}

Eсли число кусков в партиции превышает значение `parts_to_throw_insert` INSERT прерывается с исключением 'Too many parts (N). Merges are processing significantly slower than inserts'.

Возможные значения:

- Положительное целое число.

Значение по умолчанию: 300.

Для достижения максимальной производительности запросов `SELECT` необходимо минимизировать количество обрабатываемых кусков, см. [Дизайн MergeTree](../../development/architecture.md#merge-tree).
Можно установить большее значение 600 (1200), это уменьшит вероятность возникновения ошибки 'Too many parts', но в тоже время вы позже заметите возможную проблему со слияниями.


## parts_to_delay_insert {#parts-to-delay-insert}

Eсли число кусков в партиции превышает значение `parts_to_delay_insert` `INSERT` искусственно замедляется.

Возможные значения:

- Положительное целое число.

Значение по умолчанию: 150.

ClickHouse искусственно выполняет `INSERT` дольше (добавляет 'sleep'), чтобы фоновый механизм слияния успевал слиять куски быстрее чем они добавляются.


## max_delay_to_insert {#max-delay-to-insert}

Время в секундах на которое будет замедлен `INSERT`, если число кусков в партиции превышает значение [parts_to_delay_insert](#parts-to-delay-insert)

Возможные значения:

- Положительное целое число.

Значение по умолчанию: 1.

Величина задержи (в миллисекундах) для `INSERT` вычисляется по формуле

```code
max_k = parts_to_throw_insert - parts_to_delay_insert
k = 1 + parts_count_in_partition - parts_to_delay_insert
delay_milliseconds = pow(max_delay_to_insert * 1000, k / max_k)
```

Т.е. если в партиции уже 299 кусков и parts_to_throw_insert = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1, `INSERT` замедлится на `pow( 1 * 1000, (1 + 299 - 150) / (300 - 150) ) = 1000` миллисекунд.



[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/settings/merge_tree_settings/) <!--hide-->
