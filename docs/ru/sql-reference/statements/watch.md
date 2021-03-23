---
toc_priority: 53
toc_title: WATCH
---

# Запрос WATCH (экспериментальный) {#watch}

!!! important "Важно"
    Это экспериментальная функция. Она может повлечь потерю совместимости в будущих версиях.
    Чтобы использовать `LIVE VIEW` и запросы `WATCH`, включите настройку `set allow_experimental_live_view = 1`. 


``` sql
WATCH [db.]live_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

Запрос `WATCH` постоянно возвращает содержимое [живого представления](./create/view.md#live-view). Если параметр `LIMIT` не был задан, запрос `WATCH` не перестанет непрерывно возвращать содержимое [живого представления](./create/view.md#live-view).

```sql
WATCH [db.]live_view;
```

Виртуальный столбец `_version` в результате запроса обозначает версию данного результата.

**Пример:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv;
```

```bash
┌───────────────now()─┬─_version─┐
│ 2021-02-21 09:17:21 │        1 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 09:17:26 │        2 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 09:17:31 │        3 │
└─────────────────────┴──────────┘
...
```

По умолчанию запрашиваемые данные возвращаются кленту в качестве результата, однако в сочетании с запросом [INSERT INTO](../../sql-reference/statements/insert-into.md) они могут быть перенаправлены для вставки в другую таблицу.

**Пример:**

```sql
INSERT INTO [db.]table WATCH [db.]live_view ...
```

## Параметр EVENTS {#events-clause}

С помощью параметра `EVENTS` можно получить компактную форму результата запроса `WATCH`. Вместо полного результата вы получаете номер последней версии результата.

```sql
WATCH [db.]live_view EVENTS;
```

**Пример:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv EVENTS;
```

```bash
┌─version─┐
│       1 │
└─────────┘
┌─version─┐
│       2 │
└─────────┘
...
```

## Параметр LIMIT {#limit-clause}

Параметр `LIMIT n` задает количество обновлений запроса `WATCH`, после которого он оканчивает работу. По умолчанию это число не задано, поэтому запрос не прекратит выполняться. Значение `0` этого параметра будет означать, что запрос `WATCH` не будет отслеживать новые результаты запроса и закончит выполнение сразу после того, как вернет единственный актуальный результат запроса.

```sql
WATCH [db.]live_view LIMIT 1;
```

**Пример:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv EVENTS LIMIT 1;
```

```bash
┌─version─┐
│       1 │
└─────────┘
```

## Параметр FORMAT {#format-clause}

Параметр `FORMAT` работает аналогично одноименному параметру запроса [SELECT](../../sql-reference/statements/select/format.md#format-clause).

!!! info "Примечание"
    При отслеживании [живого представления](./create/view.md#live-view) через интерфейс HTTP следует использовать формат [JSONEachRowWithProgress](../../../interfaces/formats/#jsoneachrowwithprogress). Постоянные сообщения об изменениях будут добавлены в поток вывода для поддержания активности долговременного HTTP-соединения до тех пор, пока результат запроса изменяется. Проомежуток времени между сообщениями об изменениях управляется настройкой[live_view_heartbeat_interval](./create/view.md#live-view-settings).

