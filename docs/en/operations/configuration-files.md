---
toc_priority: 50
toc_title: Configuration Files
---

# Configuration Files {#configuration_files}

ClickHouse supports multi-file configuration management. The main server configuration file is `/etc/clickhouse-server/config.xml`. Other files must be in the `/etc/clickhouse-server/config.d` directory.

All the configuration files should be in XML format. Also, they should have the same root element, usually `<yandex>`.

## Override {#override}

Some settings specified in the main configuration file can be overridden in other configuration files:

-   The `replace` or `remove` attributes can be specified for the elements of these configuration files.
-   If neither is specified, it combines the contents of elements recursively, replacing values of duplicate children.
-   If `replace` is specified, it replaces the entire element with the specified one.
-   If `remove` is specified, it deletes the element.

## Substitution {#substitution}

The config can also define “substitutions”. If an element has the `incl` attribute, the corresponding substitution from the file will be used as the value. By default, the path to the file with substitutions is `/etc/metrika.xml`. This can be changed in the [include\_from](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-include_from) element in the server config. The substitution values are specified in `/yandex/substitution_name` elements in this file. If a substitution specified in `incl` does not exist, it is recorded in the log. To prevent ClickHouse from logging missing substitutions, specify the `optional="true"` attribute (for example, settings for [macros](../operations/server-configuration-parameters/settings.md)).

Substitutions can also be performed from ZooKeeper. To do this, specify the attribute `from_zk = "/path/to/node"`. The element value is replaced with the contents of the node at `/path/to/node` in ZooKeeper. You can also put an entire XML subtree on the ZooKeeper node and it will be fully inserted into the source element.

## User Settings {#user-settings}

The `config.xml` file can specify a separate config with user settings, profiles, and quotas. The relative path to this config is set in the `users_config` element. By default, it is `users.xml`. If `users_config` is omitted, the user settings, profiles, and quotas are specified directly in `config.xml`.

Users configuration can be splitted into separate files similar to `config.xml` and `config.d/`.
Directory name is defined as `users_config` setting without `.xml` postfix concatenated with `.d`.
Directory `users.d` is used by default, as `users_config` defaults to `users.xml`.

## Example {#example}

For example, you can have separate config file for each user like this:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<yandex>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</yandex>
```

## Implementation Details {#implementation-details}

For each config file, the server also generates `file-preprocessed.xml` files when starting. These files contain all the completed substitutions and overrides, and they are intended for informational use. If ZooKeeper substitutions were used in the config files but ZooKeeper is not available on the server start, the server loads the configuration from the preprocessed file.

The server tracks changes in config files, as well as files and ZooKeeper nodes that were used when performing substitutions and overrides, and reloads the settings for users and clusters on the fly. This means that you can modify the cluster, users, and their settings without restarting the server.

[Original article](https://clickhouse.tech/docs/en/operations/configuration-files/) <!--hide-->
