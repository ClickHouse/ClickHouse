---
description: 'Существующие и правильно настроенные пользователи ClickHouse могут проходить
  проверку подлинности по протоколу аутентификации Kerberos.'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

Существующие и правильно настроенные пользователи ClickHouse могут проходить аутентификацию через протокол аутентификации Kerberos.

В настоящее время Kerberos можно использовать только как внешний аутентификатор для существующих пользователей, определённых в `users.xml` или в локальных путях управления доступом. Эти пользователи могут использовать только HTTP-запросы и должны поддерживать аутентификацию с помощью механизма GSS-SPNEGO.

При таком подходе Kerberos должен быть настроен в системе и включен в конфигурации ClickHouse.

<div id="enabling-kerberos-in-clickhouse">
  ## Включение Kerberos в ClickHouse
</div>

Чтобы включить Kerberos, необходимо добавить раздел `kerberos` в файл `config.xml`. Этот раздел может содержать дополнительные параметры.

<div id="parameters">
  #### Параметры
</div>

* `principal` — каноническое имя сервисного субъекта, которое будет получено и использовано при приеме контекстов безопасности.
  * Этот параметр необязателен; если он не указан, будет использоваться субъект `default`.

* `realm` — realm, который будет использоваться для ограничения аутентификации только теми запросами, у которых realm инициатора совпадает с ним.
  * Этот параметр необязателен; если он не указан, дополнительная фильтрация по realm применяться не будет.

* `keytab` — путь к файлу keytab сервиса.
  * Этот параметр необязателен; если он не указан, путь к файлу keytab сервиса должен быть задан в переменной окружения `KRB5_KTNAME`.

Пример (добавляется в `config.xml`):

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

С указанием principal:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

С фильтрацией по realm:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
Можно определить только один раздел `kerberos`. Если указано несколько разделов `kerberos`, ClickHouse отключит аутентификацию Kerberos.
:::

:::note
Разделы `principal` и `realm` нельзя указывать одновременно. Если указаны оба раздела — `principal` и `realm`, ClickHouse отключит аутентификацию Kerberos.
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## Kerberos как внешний аутентификатор для существующих пользователей
</div>

Kerberos можно использовать как способ проверки подлинности локально определённых пользователей (пользователей, определённых в `users.xml` или в локальных путях управления доступом). В настоящее время **только** запросы через HTTP-интерфейс могут быть *kerberized* (через механизм GSS-SPNEGO).

Формат имени субъекта Kerberos обычно соответствует следующему шаблону:

* *primary/instance@REALM*

Часть */instance* может встречаться ноль или более раз. **Для успешной аутентификации ожидается, что часть *primary* канонического имени субъекта инициатора будет совпадать с именем пользователя, аутентифицируемого через Kerberos**.

<div id="enabling-kerberos-in-users-xml">
  ### Включение Kerberos в `users.xml`
</div>

Чтобы включить для пользователя аутентификацию Kerberos, укажите в определении пользователя секцию `kerberos` вместо `password` или аналогичных секций.

Параметры:

* `realm` — realm, который используется, чтобы ограничить аутентификацию только теми запросами, у инициатора которых совпадает realm.
  * Этот параметр необязателен; если его не указать, дополнительная фильтрация по realm применяться не будет.

Пример (добавляется в `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
Обратите внимание, что аутентификация Kerberos не может использоваться одновременно с каким-либо другим механизмом аутентификации. Наличие любых других секций, например `password`, наряду с `kerberos` приведет к остановке ClickHouse.
:::

:::info Reminder
Обратите внимание: теперь, когда пользователь `my_user` использует `kerberos`, Kerberos должен быть включен в основном файле `config.xml`, как описано выше.
:::

<div id="enabling-kerberos-using-sql">
  ### Включение Kerberos с помощью SQL
</div>

Когда в ClickHouse включена [система управления доступом и учётными записями на основе SQL](/ru/operations/access-rights#access-control-usage), пользователей с аутентификацией Kerberos также можно создавать с помощью SQL-команд.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...или без фильтрации по realm:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```