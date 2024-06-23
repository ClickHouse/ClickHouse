---
slug: /en/operations/external-authenticators/http
title: "HTTP"
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

HTTP server can be used to authenticate ClickHouse users. HTTP authentication can only be used as an external authenticator for existing users, which are defined in `users.xml` or in local access control paths. Currently, [Basic](https://datatracker.ietf.org/doc/html/rfc7617) authentication scheme using GET method is supported.

## HTTP authentication server definition {#http-auth-server-definition}

To define HTTP authentication server you must add `http_authentication_servers` section to the `config.xml`.

**Example**
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
        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

Note, that you can define multiple HTTP servers inside the `http_authentication_servers` section using distinct names.

**Parameters**
- `uri` - URI for making authentication request

Timeouts in milliseconds on the socket used for communicating with the server:
- `connection_timeout_ms` - Default: 1000 ms.
- `receive_timeout_ms` - Default: 1000 ms.
- `send_timeout_ms` - Default: 1000 ms.

Retry parameters:
- `max_tries` - The maximum number of attempts to make an authentication request. Default: 3
- `retry_initial_backoff_ms` - The backoff initial interval on retry. Default: 50 ms
- `retry_max_backoff_ms` - The maximum backoff interval. Default: 1000 ms

### Enabling HTTP authentication in `users.xml` {#enabling-http-auth-in-users-xml}

In order to enable HTTP authentication for the user, specify `http_authentication` section instead of `password` or similar sections in the user definition.

Parameters:
- `server` - Name of the HTTP authentication server configured in the main `config.xml` file as described previously.
- `scheme` - HTTP authentication scheme. `Basic` is only supported now. Default: Basic

Example (goes into `users.xml`):
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
Note that HTTP authentication cannot be used alongside with any other authentication mechanism. The presence of any other sections like `password` alongside `http_authentication` will force ClickHouse to shutdown.
:::

### Enabling HTTP authentication using SQL {#enabling-http-auth-using-sql}

When [SQL-driven Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled in ClickHouse, users identified by HTTP authentication can also be created using SQL statements.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...or, `Basic` is default without explicit scheme definition

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

### Passing session settings {#passing-session-settings}

If a response body from HTTP authentication server has JSON format and contains `settings` sub-object, ClickHouse will try parse its key: value pairs as string values and set them as session settings for authenticated user's current session. If parsing is failed, a response body from server will be ignored.
