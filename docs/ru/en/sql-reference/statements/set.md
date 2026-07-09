---
description: 'Документация по оператору SET'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'Оператор SET'
doc_type: 'reference'
---

```sql
SET param = value
```

Устанавливает значение `value` для [настройки](/ru/operations/settings/overview) `param` в текущем сеансе. [Настройки сервера](../../operations/server-configuration-parameters/settings.md) таким способом изменить нельзя.

Вы также можете задать все значения из указанного профиля настроек одним запросом.

```sql
SET profile = 'profile-name-from-the-settings-file'
```

Для булевых настроек со значением `true` можно использовать сокращённый синтаксис, опуская присваивание значения. Если указано только имя настройки, ей автоматически присваивается `1` (`true`).

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

<div id="set-time-zone">
  ## SET TIME ZONE
</div>

```sql
SET TIME ZONE [=] 'timezone'
```

Устанавливает часовой пояс сеанса. Это алиас для `SET session_timezone = 'timezone'`, предусмотренный для совместимости с PostgreSQL и другими SQL-базами данных.

Многие SQL-клиенты, ORM и драйверы JDBC автоматически выполняют `SET TIME ZONE` при подключении. Этот синтаксис позволяет таким инструментам работать с ClickHouse без дополнительных обходных решений.

```sql
SET TIME ZONE 'UTC';
SET TIME ZONE 'Europe/Amsterdam';
SET TIME ZONE 'America/New_York';

-- Verify the current session time zone
SELECT getSetting('session_timezone');
```

Значение часового пояса должно быть корректным именем из [базы данных часовых поясов IANA](https://www.iana.org/time-zones). Некорректное имя часового пояса приведёт к ошибке.

Дополнительные сведения о настройке `session_timezone` см. в разделе [session&#95;timezone](/ru/operations/settings/settings#session_timezone).

<div id="setting-query-parameters">
  ## Настройка параметров запроса
</div>

Оператор `SET` также можно использовать для определения параметров запроса, добавив префикс `param_` к имени параметра.
Параметры запроса позволяют писать универсальные запросы с плейсхолдерами, которые подставляются фактическими значениями во время выполнения.

```sql
SET param_name = value
```

Чтобы использовать параметр запроса в запросе, укажите его в формате `{name: datatype}`:

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

Параметры запроса особенно полезны, когда один и тот же запрос нужно выполнить несколько раз с разными значениями.

Более подробную информацию о параметрах запроса, включая их использование с типом `Identifier`, см. в разделе [Определение и использование параметров запроса](../../sql-reference/syntax.md#defining-and-using-query-parameters).

Дополнительные сведения см. в разделе [Settings](../../operations/settings/settings.md).