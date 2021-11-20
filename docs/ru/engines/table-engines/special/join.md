---
toc_priority: 40
toc_title: Join
---

# Join {#join}

Подготовленная структура данных для использования в операциях [JOIN](../../../engines/table-engines/special/join.md#select-join).

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

Смотрите подробное описание запроса [CREATE TABLE](../../../engines/table-engines/special/join.md#create-table-query).

**Параметры движка**

-   `join_strictness` – [строгость JOIN](../../../engines/table-engines/special/join.md#select-join-types).
-   `join_type` – [тип JOIN](../../../engines/table-engines/special/join.md#select-join-types).
-   `k1[, k2, ...]` – ключевые столбцы секции `USING` с которыми выполняется операция `JOIN`.

Вводите параметры `join_strictness` и `join_type` без кавычек, например, `Join(ANY, LEFT, col1)`. Они должны быть такими же как и в той операции `JOIN`, в которой таблица будет использоваться. Если параметры не совпадают, ClickHouse не генерирует исключение и может возвращать неверные данные.

## Использование таблицы {#ispolzovanie-tablitsy}

### Пример {#primer}

Создание левой таблицы:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

Создание правой таблицы с движком `Join`:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

Объединение таблиц:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

В качестве альтернативы, можно извлечь данные из таблицы `Join`, указав значение ключа объединения:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### Выборка и вставка данных {#vyborka-i-vstavka-dannykh}

Для добавления данных в таблицы с движком `Join` используйте запрос `INSERT`. Если таблица создавалась со строгостью `ANY`, то данные с повторяющимися ключами игнорируются. Если задавалась строгость `ALL`, то добавляются все строки.

Из таблиц нельзя выбрать данные с помощью запроса `SELECT`. Вместо этого, используйте один из следующих методов:

-   Используйте таблицу как правую в секции `JOIN`.
-   Используйте функцию [joinGet](../../../engines/table-engines/special/join.md#joinget), которая позволяет извлекать данные из таблицы таким же образом как из словаря.

### Ограничения и настройки {#join-limitations-and-settings}

При создании таблицы, применяются следующие параметры :

-   [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)
-   [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [join_any_take_last_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)
-   [persistent](../../../operations/settings/settings.md#persistent)

Таблицы с движком `Join` нельзя использовать в операциях `GLOBAL JOIN`.

Движок `Join` позволяет использовать параметр [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls) в запросе `CREATE TABLE`, который также можно использовать в запросе [SELECT](../../../engines/table-engines/special/join.md). Если у вас разные настройки `join_use_nulls`, вы можете получить сообщение об ошибке при объединении таблиц. Это зависит от типа соединения. Когда вы используете функцию [joinGet](../../../engines/table-engines/special/join.md#joinget), вам необходимо использовать один и тот же параметр `join_use_nulls` в запросах `CRATE TABLE` и `SELECT`.

## Хранение данных {#khranenie-dannykh}

Данные таблиц `Join` всегда находятся в RAM. При вставке строк в таблицу ClickHouse записывает блоки данных в каталог на диске, чтобы их можно было восстановить при перезапуске сервера.

При аварийном перезапуске сервера блок данных на диске может быть потерян или повреждён. В последнем случае, может потребоваться вручную удалить файл с повреждёнными данными.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/join/) <!--hide-->
