---
description: 'Настройки на уровне запроса'
sidebar_label: 'Настройки сеанса на уровне запроса'
slug: /operations/settings/query-level
title: 'Настройки сеанса на уровне запроса'
doc_type: 'reference'
---

<div id="overview">
  ## Обзор
</div>

Существует несколько способов выполнять команды с определёнными настройками.
Настройки задаются по слоям, и каждый последующий слой переопределяет предыдущие значения настройки.

<div id="order-of-priority">
  ## Порядок приоритета
</div>

Порядок определения настройки по приоритету:

1. Применение настройки непосредственно к пользователю или в профиле настроек

   * SQL (рекомендуется)
   * добавление одного или нескольких XML- или YAML-файлов в `/etc/clickhouse-server/users.d`

2. Настройки сеанса

   * Отправьте `SET setting=value` из SQL-консоли ClickHouse Cloud или через
     `clickhouse client` в интерактивном режиме. Аналогично можно использовать
     сеансы ClickHouse по HTTP-протоколу. Для этого необходимо указать
     HTTP-параметр `session_id`.

3. Настройки запроса

   * При запуске `clickhouse client` в неинтерактивном режиме задайте
     параметр запуска `--setting=value`.
   * При использовании HTTP API передавайте CGI-параметры (`URL?setting_1=value&setting_2=value...`).
   * Задайте настройки в секции
     [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     запроса SELECT. Значение настройки применяется только к этому запросу
     и после его выполнения сбрасывается до значения по умолчанию или предыдущего значения.

<div id="converting-a-setting-to-its-default-value">
  ## Возврат настройки к значению по умолчанию
</div>

Если вы изменили настройку и хотите вернуть ей значение по умолчанию, задайте для неё значение `DEFAULT`. Синтаксис:

```sql
SET setting_name = DEFAULT
```

Например, значение `async_insert` по умолчанию — `0`. Допустим, вы изменили его на `1`:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

Ответ:

```response
┌─value──┐
│ 1      │
└────────┘
```

Следующая команда возвращает его значение к 0:

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

Теперь для параметра снова установлено значение по умолчанию:

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## Пользовательские настройки
</div>

Помимо общих [настроек](/ru/operations/settings/settings.md), пользователи могут определять пользовательские настройки.
Пользовательские настройки позволяют передавать **параметры, специфичные для сеанса**, на которые можно ссылаться в запросах, политиках или функциях. Это полезно, когда требуется:

* Фильтровать данные на основе identity пользователя или organization
* Применять различную business logic в зависимости от контекста
* Сохранять информацию о состоянии между запросами в рамках сеанса

Имя пользовательской настройки должно начинаться с одного из заранее определённых префиксов из заданного вами списка.
Список префиксов можно указать с помощью настройки сервера [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes), заданной в вашем файле конфигурации сервера.

В примере ниже в качестве пользовательского префикса выбран `SQL_`:

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
В ClickHouse Cloud нельзя указать собственный префикс.
Все пользовательские настройки начинаются с префикса `SQL_`.
:::

Чтобы задать пользовательскую настройку, используйте команду `SET`:

```sql
SET SQL_a = 123;
```

Чтобы получить текущее значение пользовательской настройки, используйте функцию `getSetting()`:

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## Примеры
</div>

Во всех этих примерах значение настройки `async_insert` устанавливается в `1`, а
также показывается, как просматривать настройки в работающей системе.

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### Использование SQL для прямого назначения настройки пользователю
</div>

Это создаёт пользователя `ingester` с параметром `async_inset = 1`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### Изучите профиль настроек и его привязку
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### Использование SQL для создания профиля настроек и назначения его пользователю
</div>

Эта команда создаёт профиль `log_ingest` с настройкой `async_inset = 1`:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

Это создаёт пользователя `ingester` и назначает ему профиль настроек `log_ingest`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### Создание профиля настроек и пользователя с помощью XML
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment">
  #### Изучите профиль настроек и его привязку
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### Назначить настройку для сеанса
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### Задать настройку при выполнении запроса
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## См. также
</div>

* Описание настроек ClickHouse см. на странице [Настройки](/ru/operations/settings/settings.md).
* [Глобальные настройки сервера](/ru/operations/server-configuration-parameters/settings.md)