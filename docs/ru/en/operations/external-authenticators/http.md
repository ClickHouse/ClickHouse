---
description: 'Документация по HTTP'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

HTTP-сервер можно использовать для аутентификации пользователей ClickHouse. HTTP-аутентификация может использоваться только как внешний аутентификатор для существующих пользователей, заданных в `users.xml` или в локальных путях управления доступом. В настоящее время поддерживается схема HTTP-аутентификации [Basic](https://datatracker.ietf.org/doc/html/rfc7617) с использованием метода GET.

<div id="http-auth-server-definition">
  ## Определение сервера HTTP-аутентификации
</div>

Чтобы задать сервер HTTP-аутентификации, необходимо добавить раздел `http_authentication_servers` в файл `config.xml`.

**Пример**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

Обратите внимание, что в разделе `http_authentication_servers` можно определить несколько HTTP-серверов, указав для них разные имена.

**Параметры**

* `uri` - URI для отправки запроса аутентификации

Тайм-ауты в миллисекундах для сокета, используемого для взаимодействия с сервером:

* `connection_timeout_ms` - По умолчанию: 1000 мс.
* `receive_timeout_ms` - По умолчанию: 1000 мс.
* `send_timeout_ms` - По умолчанию: 1000 мс.

Параметры повторных попыток:

* `max_tries` - Максимальное количество попыток отправить запрос аутентификации. По умолчанию: 3
* `retry_initial_backoff_ms` - Начальный интервал задержки при повторной попытке. По умолчанию: 50 мс
* `retry_max_backoff_ms` - Максимальный интервал задержки. По умолчанию: 1000 мс

Пересылаемые заголовки:

В этом разделе определяется, какие заголовки будут пересылаться из заголовков клиентского запроса во внешний HTTP-сервер аутентификации. Обратите внимание, что заголовки сопоставляются с заголовками, указанными в config, регистронезависимым образом, но пересылаются как есть, то есть без изменений.

<div id="enabling-http-auth-in-users-xml">
  ### Включение HTTP-аутентификации в `users.xml`
</div>

Чтобы включить HTTP-аутентификацию для пользователя, укажите в определении пользователя раздел `http_authentication` вместо `password` или аналогичных разделов.

Параметры:

* `server` — имя сервера HTTP-аутентификации, настроенного в основном файле `config.xml`, как описано выше.
* `scheme` — схема HTTP-аутентификации. В настоящее время поддерживается только `Basic`. По умолчанию: Basic

Пример (добавляется в `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
Обратите внимание: HTTP-аутентификацию нельзя использовать вместе с каким-либо другим механизмом аутентификации. Наличие любых других секций, например `password`, наряду с `http_authentication` приведет к остановке ClickHouse.
:::

<div id="enabling-http-auth-using-sql">
  ### Включение HTTP-аутентификации с помощью SQL
</div>

Если в ClickHouse включена [система управления доступом и учётными записями на основе SQL](/ru/operations/access-rights#access-control-usage), пользователей, проходящих HTTP-аутентификацию, также можно создавать с помощью SQL-команд.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...или по умолчанию используется `Basic`, если схема явно не указана

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### Передача настроек сеанса
</div>

Если тело ответа сервера HTTP-аутентификации имеет формат JSON и содержит подобъект `settings`, ClickHouse попытается разобрать его пары `key: value` как строковые значения и применить их как настройки сеанса для текущего сеанса аутентифицированного пользователя. Если разбор не удастся, тело ответа сервера будет проигнорировано.