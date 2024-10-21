---
slug: /ru/operations/external-authenticators/jwt
---
# JWT
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Существующие и корректно настроенные пользователи ClickHouse могут быть аутентифицированы с помощью JWT.

Сейчас JWT работает только как внешний аутентификатор для уже существующих пользователей.
Имя пользователя будет извлечено из JWT после проверки срока действия токена и подписи. 
Подпись может быть проверена с помощью:
- статического (указанного в конфигурации) открытого ключа,
- статического (указанного в конфигурации) JWKS или файла, содержащего JWKS,
- полученного от JWKS-сервера.

Имя пользователя ClickHouse должно быть обязательно указано в поле (claim) `"sub"`, в противном случае токен не будет принят.

Можно также дополнительно проверять JWT на наличие определённого содержимого (payload).
В этом случае проверяется наличие указанных полей (claims) из настроек пользователя в содержимом JWT.
Смотри [Настройка JWT аутентификации пользователя через `users.xml`](#enabling-jwt-auth-in-users-xml) и [Настройка JWT аутентификации пользователя через SQL](#enabling-jwt-auth-using-sql)


## Настройка JWT валидаторов {#enabling-jwt-validators}

Для аутентификации с помощью JWT сконфигурировать как минимум один валидатор.
Это делается в секции `jwt_validators` в `config.xml`. Эта секция может содержать несколько JWT-верификаторов.

### Проверка JWT с помощью статического ключа {$verifying-jwt-signature-using-static-key}

**Пример**
```xml
<clickhouse>
    <!- ... -->
    <jwt_validators>
        <my_static_key_validator>
          <algo>HS256</algo>
          <static_key>my_static_secret</static_key>
        </my_static_key_validator>
    </jwt_validators>
</clickhouse>
```

#### Параметры:

- `algo` - Алгоритм для проверки подписи. Поддерживаемые алгоритмы:

  | HMSC  | RSA   | ECDSA  | PSS   | EdDSA   |
  | ----- | ----- | ------ | ----- | ------- |
  | HS256 | RS256 | ES256  | PS256 | Ed25519 |
  | HS384 | RS384 | ES384  | PS384 | Ed448   |
  | HS512 | RS512 | ES512  | PS512 |         |
  |       |       | ES256K |       |         |
  Можно не проверять подпись, указав `None` для этого параметра.
- `static_key` - ключ симметричного алгоритма. Обязателен для алгоритмов семейства `HS*`.
- `static_key_in_base64` - указывает, закодирован ли `static_key` в формате base64. Необязательный параметр, по умолчанию: `False`.
- `public_key` - открытый ключ для асимметричных алгоритмов. Обязателен для всех алгоритмов, кроме семейства `HS*` и `None`.
- `private_key` - закрытый ключ для асимметричных алгоритмов. Необязательный параметр.
- `public_key_password` - пароль открытого ключа, необязательный параметр.
- `private_key_password` - пароль закрытого ключа, необязательный параметр.

### Проверка JWT с помощью статического JWKS {$verifying-jwt-signature-using-static-jwks}

:::note
Проверка с помощью JWKS невозможна для алгоритмов семейства `HS*`.
:::

**Пример**
```xml
<clickhouse>
    <!- ... -->
    <jwt_validators>
        <my_static_jwks_validator>
          <static_jwks>{"keys": [{"kty": "RSA", "alg": "RS256", "kid": "mykid", "n": "_public_key_mod_", "e": "AQAB"}]}</static_jwks>
        </my_static_jwks_validator>
    </jwt_validators>
</clickhouse>
```

#### Параметры:
- `static_jwks` - содержимое JWKS в виде JSON.
- `static_jwks_file` - путь к файлу, содержащему JWKS.

:::note
Должен быть указан один и только один из этих двух параметров.
:::

### Проверка JWT с помощью JWKS сервера {$verifying-jwt-signature-using-static-jwks}

:::note
Проверка с помощью JWKS невозможна для алгоритмов семейства `HS*`.
:::

**Пример**
```xml
<clickhouse>
    <!- ... -->
    <jwt_validators>
        <basic_auth_server>
          <uri>http://localhost:8000/.well-known/jwks.json</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <refresh_ms>300000</refresh_ms>
        </basic_auth_server>
    </jwt_validators>
</clickhouse>
```

#### Параметры:

- `uri` - адрес, по которому доступен JWKS. Обязательный параметр.
- `refresh_ms` - Период обновления JWKS. Необязательный параметр, по умолчанию: 300000.

Таймауты в миллисекундах для сокета, используемого для связи с сервером (необязательные параметры):
- `connection_timeout_ms` - По умолчанию: 1000.
- `receive_timeout_ms` - По умолчанию: 1000.
- `send_timeout_ms` - По умолчанию: 1000.

Настройка повторных попыток (необязательные параметры):
- `max_tries` - Максимальное количество попыток аутентификации. По умолчанию: 3.
- `retry_initial_backoff_ms` - Стартовый интервал между повторными попытками (backoff). По умолчанию: 50.
- `retry_max_backoff_ms` - Максимальный интервал между повторными попытками (backoff). По умолчанию: 1000.

### Настройка JWT аутентификации пользователя в `users.xml` {#enabling-jwt-auth-in-users-xml}

Чтобы включить аутентификацию с помощью JWT для пользователя, укажите секцию `jwt` вместо секции `password` и аналогичных секций.

**Пример (`users.xml`)**
```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <jwt>
            <claims>{"resource_access":{"account": {"roles": ["view-profile"]}}}</claims>
        </jwt>
    </my_user>
</clickhouse>
```

#### Параметры
- `claims` - строка, содержащая JSON, который должен присутствовать в содержимом токена.

В данном случае содержимое JWT должно содержать значение ["view-profile"] по пути `resource_access.account.roles`, 
в противном случае аутентификация не будет успешной, даже если в остальном JWT верный.

**Пример payload**
```json
{
  "sub": "my_user",
  "resource_access": {
    "account": {
      "roles": ["view-profile"]
    }
  },
}
```

:::note
Аутентификация JWT не может использоваться вместе с другими методами аутентификации. Наличие любых других секций, таких как `password`, наряду с секцией `jwt` приведет к аварийному завершению работы.
:::

### Настройка JWT аутентификации пользователя через SQL {#enabling-jwt-auth-using-sql}

When [SQL-driven Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled in ClickHouse, users identified by JWT authentication can also be created using SQL statements.

В случае если в ClickHouse включено управление доступом через SQL ([SQL-driven Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control)), 
можно создать пользователя с аутентификацией через JWT с помощью SQL-запросов.

**Без проверки содержимого JWT**
```sql
CREATE USER my_user IDENTIFIED WITH jwt
```

**С проверкой содержимого JWT**
```sql
CREATE USER my_user IDENTIFIED WITH jwt CLAIMS '{"resource_access":{"account": {"roles": ["view-profile"]}}}'
```

## Примеры аутентификации {#jwt-authentication-examples}

#### `clickhouse-client`

```
clickhouse-client -jwt <token>
```

#### HTTP

```
curl 'http://localhost:8080/?' \
 -H 'Authorization: Bearer <TOKEN>' \
 -H 'Content type: text/plain;charset=UTF-8' \
 --data-raw 'SELECT current_user()'
```
:::note
ClickHouse ищет токен в следующих местах (по порядку):
1. Заголовок `X-ClickHouse-JWT-Token`.
2. Стандартный заголовок `Authorization`.
3. Параметр `token`. В этом случае параметр не должен содержать префикс `Bearer`.
:::

### Передача параметров сессии {#passing-session-settings}
