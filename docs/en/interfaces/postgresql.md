---
sidebar_position: 20
sidebar_label: PostgreSQL Interface
---

# PostgreSQL Interface

ClickHouse supports the PostgreSQL wire protocol, which allows you to use Postgres clients to connect to ClickHouse. In a sense, ClickHouse can pretend to be a PostgreSQL instance - allowing you to connect a PostgreSQL client application to ClickHouse that is not already directly supported by ClickHouse (for example, Amazon Redshift).

To enable the PostgreSQL wire protocol, add the [postgresql_port](../operations/server-configuration-parameters/settings#server_configuration_parameters-postgresql_port) setting to your server's configuration file. For example, you could define the port in a new XML file in your `config.d` folder:

```xml
<clickhouse>
	<postgresql_port>9005</postgresql_port>
</clickhouse>
```

Startup your ClickHouse server and look for a log message similar to the following that mentions **Listening for PostgreSQL compatibility protocol**:

```response
{} <Information> Application: Listening for PostgreSQL compatibility protocol: 127.0.0.1:9005
```

## Connect psql to ClickHouse

The following command demonstrates how to connect the PostgreSQL client `psql` to ClickHouse:

```bash
psql -p [port] -h [hostname] -U [username] [database_name]
```

For example:

```bash
psql -p 9005 -h 127.0.0.1 -U alice default
```

:::note
The `psql` client requires a login with a password, so you will not be able connect using the `default` user with no password. Either assign a password to the `default` user, or login as a different user.
:::

The `psql` client prompts for the password:

```response
Password for user alice:
psql (14.2, server 22.3.1.1)
WARNING: psql major version 14, server major version 22.
         Some psql features might not work.
Type "help" for help.

default=>
```

And that's it! You now have a PostgreSQL client connected to ClickHouse, and all commands and queries are executed on ClickHouse.

:::caution
The PostgreSQL protocol currently only supports plain-text passwords.
:::

## Using SSL

If you have SSL/TLS configured on your ClickHouse instance, then `postgresql_port` will use the same settings (the port is shared for both secure and insecure clients).

Each client has their own method of how to connect using SSL. The following command demonstrates how to pass in the certificates and key to securely connect `psql` to ClickHouse:

```bash
psql "port=9005 host=127.0.0.1 user=alice dbname=default sslcert=/path/to/certificate.pem sslkey=/path/to/key.pem sslrootcert=/path/to/rootcert.pem sslmode=verify-ca"
```

View the [PostgreSQL docs](https://jdbc.postgresql.org/documentation/head/ssl-client.html) for more details on their SSL settings.

[Original article](https://clickhouse.com/docs/en/interfaces/postgresql)
