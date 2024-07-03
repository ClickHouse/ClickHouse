---
slug: /en/operations/external-authenticators/jwt
---
# JWT
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Existing and properly configured ClickHouse users can be authenticated via JWT.

Currently, JWT can only be used as an external authenticator for existing users, which are defined in `users.xml` or in local access control paths. 

The username will be extracted from the JWT after validating the token against the signature received from the JWKS servers and the expiration date. Additionally, each user can be verified using a JWT payload. In this case, after running general checks, the occurrence of CLAIMS from the user settings in the JWT payload is checked.

For this approach, JWKS must be configured in the system and must be enabled in ClickHouse config.


## Enabling JWKS in ClickHouse {#enabling-jwks-in-clickhouse}

To enable JWKS, one should include `jwks` section in `config.xml`. This section may contain several JWKS servers, minimum is 1.

## JWKS server definition {#jwks-server-definition}


**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwks_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/jwks.json</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <refresh_ms>300000</refresh_ms>
        </basic_auth_server>
    </jwks_authentication_servers>
</clickhouse>
```

Note, that you can define multiple HTTP servers inside the `jwks_authentication_servers` section using distinct names.

#### Parameters:

- `uri` - URI for making authentication request
- `refresh_ms` - Period for resend request for refreshing JWKS. Default: 300000 ms.

Timeouts in milliseconds on the socket used for communicating with the server:
- `connection_timeout_ms` - Default: 1000 ms.
- `receive_timeout_ms` - Default: 1000 ms.
- `send_timeout_ms` - Default: 1000 ms.

Retry parameters:
- `max_tries` - The maximum number of attempts to make an authentication request. Default: 3
- `retry_initial_backoff_ms` - The backoff initial interval on retry. Default: 50 ms
- `retry_max_backoff_ms` - The maximum backoff interval. Default: 1000 ms

### Enabling JWT authentication in `users.xml` {#enabling-jwt-auth-in-users-xml}

In order to enable HTTP authentication for the user, specify `jwt` section instead of `password` or similar sections in the user definition.

Parameters:
- `claims` - An optional string containing a json object that should be contained in the token payload.

Example (goes into `users.xml`):
```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <jwks>
            <claims>{"resource_access":{"account": {"roles": ["view-profile"]}}}</claims>
        </jwks>
    </my_user>
</clickhouse>
```

In this sample payload must contains "view-profile" in array on path resource_access.account.roles like

```
{
...
  "realm_access": {
    "roles": [
      "default-roles-master",
      "offline_access",
      "uma_authorization"
    ]
  },
...  
}
```

:::note
Note that HTTP authentication cannot be used alongside with any other authentication mechanism. The presence of any other sections like `password` alongside `http_authentication` will force ClickHouse to shutdown.
:::

### Enabling JWT authentication using SQL {#enabling-jwt-auth-using-sql}

When [SQL-driven Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled in ClickHouse, users identified by JWT authentication can also be created using SQL statements.

```sql
CREATE USER my_user IDENTIFIED WITH jwt CLAIMS '{"resource_access":{"account": {"roles": ["view-profile"]}}}'
```

...or, without additional JWT payload checks

```sql
CREATE USER my_user IDENTIFIED WITH jwt
```

## JWT authorization examples {#jwt-authorization-examples}

#### Console client

```
clickhouse-client -jwt <token>
```

#### HTTP requests

```
curl 'http://localhost:8080/?add_http_cors_header=1&default_format=JSONCompact&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break' \
 -H 'Authorization: Bearer <TOKEN>' \
 -H 'Content type: text/plain;charset=UTF-8' \
 --data-raw 'SELECT current_user()'
```
:::note
The token can be obtained (by priority):
- header X-ClickHouse-JWT-Token
- header Authorization
- request parameter "token". In this case, the "Bearer" prefix should not exist.
:::