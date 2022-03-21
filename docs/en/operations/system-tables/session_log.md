# system.session_log {#system_tables-session_log}

Contains information about all successful and failed login and logout events.

Columns:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — Login/logout result. Possible values:
    -   `LoginFailure` — Login error.
    -   `LoginSuccess` — Successful login.
    -   `Logout` — Logout from the system.
-   `auth_id` ([UUID](../../sql-reference/data-types/uuid.md)) — Authentication ID, which is a UUID that is automatically generated each time user logins.
-   `session_id` ([String](../../sql-reference/data-types/string.md)) — Session ID that is passed by client via [HTTP](../../interfaces/http.md) interface.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Login/logout date.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Login/logout time.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Login/logout starting time with microseconds precision.
-   `user` ([String](../../sql-reference/data-types/string.md)) — User name.
-   `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)) — The authentication type. Possible values:
    -   `NO_PASSWORD`
    -   `PLAINTEXT_PASSWORD`
    -   `SHA256_PASSWORD`
    -   `DOUBLE_SHA1_PASSWORD`
    -   `LDAP`
    -   `KERBEROS`
-   `profiles` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — The list of profiles set for all roles and/or users.
-   `roles` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — The list of roles to which the profile is applied.
-   `settings` ([Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md), [String](../../sql-reference/data-types/string.md)))) — Settings that were changed when the client logged in/out.
-   `client_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — The IP address that was used to log in/out.
-   `client_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The client port that was used to log in/out.
-   `interface` ([Enum8](../../sql-reference/data-types/enum.md)) — The interface from which the login was initiated. Possible values:
    -   `TCP`
    -   `HTTP`
    -   `gRPC`
    -   `MySQL`
    -   `PostgreSQL`
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — The hostname of the client machine where the [clickhouse-client](../../interfaces/cli.md) or another TCP client is run.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — The `clickhouse-client` or another TCP client name.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Revision of the `clickhouse-client` or another TCP client.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The major version of the `clickhouse-client` or another TCP client.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The minor version of the `clickhouse-client` or another TCP client.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Patch component of the `clickhouse-client` or another TCP client version.
-   `failure_reason` ([String](../../sql-reference/data-types/string.md)) — The exception message containing the reason for the login/logout failure.

**Example**

Query:

``` sql
SELECT * FROM system.session_log LIMIT 1 FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
type:                    LoginSuccess
auth_id:                 45e6bd83-b4aa-4a23-85e6-bd83b4aa1a23
session_id:
event_date:              2021-10-14
event_time:              2021-10-14 20:33:52
event_time_microseconds: 2021-10-14 20:33:52.104247
user:                    default
auth_type:               PLAINTEXT_PASSWORD
profiles:                ['default']
roles:                   []
settings:                [('load_balancing','random'),('max_memory_usage','10000000000')]
client_address:          ::ffff:127.0.0.1
client_port:             38490
interface:               TCP
client_hostname:
client_name:             ClickHouse client
client_revision:         54449
client_version_major:    21
client_version_minor:    10
client_version_patch:    0
failure_reason:
```
