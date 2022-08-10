# Kerberos {#external-authenticators-kerberos}

Existing and properly configured ClickHouse users can be authenticated via Kerberos authentication protocol.

Currently, Kerberos can only be used as an external authenticator for existing users, which are defined in `users.xml` or in local access control paths. Those users may only use HTTP requests and must be able to authenticate using GSS-SPNEGO mechanism.

For this approach, Kerberos must be configured in the system and must be enabled in ClickHouse config.


## Enabling Kerberos in ClickHouse {#enabling-kerberos-in-clickhouse}

To enable Kerberos, one should include `kerberos` section in `config.xml`. This section may contain additional parameters.

#### Parameters:

- `principal` - canonical service principal name that will be acquired and used when accepting security contexts.
    - This parameter is optional, if omitted, the default principal will be used.


- `realm` - a realm, that will be used to restrict authentication to only those requests whose initiator's realm matches it.
    - This parameter is optional, if omitted, no additional filtering by realm will be applied.

Example (goes into `config.xml`):

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

With principal specification:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

With filtering by realm:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::warning
You can define only one `kerberos` section. The presence of multiple `kerberos` sections will force ClickHouse to disable Kerberos authentication.
:::

:::warning
`principal` and `realm` sections cannot be specified at the same time. The presence of both `principal` and `realm` sections will force ClickHouse to disable Kerberos authentication.
:::

## Kerberos as an external authenticator for existing users {#kerberos-as-an-external-authenticator-for-existing-users}

Kerberos can be used as a method for verifying the identity of locally defined users (users defined in `users.xml` or in local access control paths). Currently, **only** requests over the HTTP interface can be *kerberized* (via GSS-SPNEGO mechanism).

Kerberos principal name format usually follows this pattern:

- *primary/instance@REALM*

The */instance* part may occur zero or more times. **The *primary* part of the canonical principal name of the initiator is expected to match the kerberized user name for authentication to succeed**.

### Enabling Kerberos in `users.xml` {#enabling-kerberos-in-users-xml}

In order to enable Kerberos authentication for the user, specify `kerberos` section instead of `password` or similar sections in the user definition.

Parameters:

- `realm` - a realm that will be used to restrict authentication to only those requests whose initiator's realm matches it.
    - This parameter is optional, if omitted, no additional filtering by realm will be applied.

Example (goes into `users.xml`):

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

:::warning
Note that Kerberos authentication cannot be used alongside with any other authentication mechanism. The presence of any other sections like `password` alongside `kerberos` will force ClickHouse to shutdown.
:::

:::info Reminder    
Note, that now, once user `my_user` uses `kerberos`, Kerberos must be enabled in the main `config.xml` file as described previously.
:::

### Enabling Kerberos using SQL {#enabling-kerberos-using-sql}

When [SQL-driven Access Control and Account Management](../access-rights.md#access-control) is enabled in ClickHouse, users identified by Kerberos can also be created using SQL statements.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...or, without filtering by realm:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```
