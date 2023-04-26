---
sidebar_position: 50
sidebar_label: Configuration Files
---

# Configuration Files

ClickHouse supports multi-file configuration management. The main server configuration file is `/etc/clickhouse-server/config.xml` or `/etc/clickhouse-server/config.yaml`. Other files must be in the `/etc/clickhouse-server/config.d` directory. Note, that any configuration file can be written either in XML or YAML, but mixing formats in one file is not supported. For example, you can have main configs as `config.xml` and `users.xml` and write additional files in `config.d` and `users.d` directories in `.yaml`.

All XML files should have the same root element, usually `<clickhouse>`. As for YAML, `clickhouse:` should not be present, the parser will insert it automatically.

## Override {#override}

Some settings specified in the main configuration file can be overridden in other configuration files:

-   The `replace` or `remove` attributes can be specified for the elements of these configuration files.
-   If neither is specified, it combines the contents of elements recursively, replacing values of duplicate children.
-   If `replace` is specified, it replaces the entire element with the specified one.
-   If `remove` is specified, it deletes the element.

You can also declare attributes as coming from environment variables by using `from_env="VARIABLE_NAME"`:

```xml
<clickhouse>
    <macros>
        <replica from_env="REPLICA" />
        <layer from_env="LAYER" />
        <shard from_env="SHARD" />
    </macros>
</clickhouse>
```

## Substitution {#substitution}

The config can also define “substitutions”. If an element has the `incl` attribute, the corresponding substitution from the file will be used as the value. By default, the path to the file with substitutions is `/etc/metrika.xml`. This can be changed in the [include_from](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-include_from) element in the server config. The substitution values are specified in `/clickhouse/substitution_name` elements in this file. If a substitution specified in `incl` does not exist, it is recorded in the log. To prevent ClickHouse from logging missing substitutions, specify the `optional="true"` attribute (for example, settings for [macros](../operations/server-configuration-parameters/settings.md#macros)).

If you want to replace an entire element with a substitution use `include` as the element name.

XML substitution example:

```xml
<clickhouse>
    <!-- Appends XML subtree found at `/profiles-in-zookeeper` ZK path to `<profiles>` element. -->
    <profiles from_zk="/profiles-in-zookeeper" />

    <users>
        <!-- Replaces `include` element with the subtree found at `/users-in-zookeeper` ZK path. -->
        <include from_zk="/users-in-zookeeper" />
        <include from_zk="/other-users-in-zookeeper" />
    </users>
</clickhouse>
```

Substitutions can also be performed from ZooKeeper. To do this, specify the attribute `from_zk = "/path/to/node"`. The element value is replaced with the contents of the node at `/path/to/node` in ZooKeeper. You can also put an entire XML subtree on the ZooKeeper node and it will be fully inserted into the source element.

## User Settings {#user-settings}

The `config.xml` file can specify a separate config with user settings, profiles, and quotas. The relative path to this config is set in the `users_config` element. By default, it is `users.xml`. If `users_config` is omitted, the user settings, profiles, and quotas are specified directly in `config.xml`.

Users configuration can be split into separate files similar to `config.xml` and `config.d/`.
Directory name is defined as `users_config` setting without `.xml` postfix concatenated with `.d`.
Directory `users.d` is used by default, as `users_config` defaults to `users.xml`.

Note that configuration files are first merged taking into account [Override](#override) settings and includes are processed after that.

## XML example {#example}

For example, you can have separate config file for each user like this:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<clickhouse>
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
</clickhouse>
```

## YAML examples {#example}

Here you can see default config written in YAML: [config.yaml.example](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

There are some differences between YAML and XML formats in terms of ClickHouse configurations. Here are some tips for writing a configuration in YAML format.

You should use a Scalar node to write a key-value pair:
``` yaml
key: value
```

To create a node, containing other nodes you should use a Map:
``` yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

To create a list of values or nodes assigned to one tag you should use a Sequence:
``` yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

If you want to write an attribute for a Sequence or Map node, you should use a @ prefix before the attribute key. Note, that @ is reserved by YAML standard, so you should also to wrap it into double quotes:

``` yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

From that Map we will get these XML nodes:

``` xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

You can also set attributes for Sequence:

``` yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

So, we can get YAML config equal to this XML one:

``` xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

## Implementation Details {#implementation-details}

For each config file, the server also generates `file-preprocessed.xml` files when starting. These files contain all the completed substitutions and overrides, and they are intended for informational use. If ZooKeeper substitutions were used in the config files but ZooKeeper is not available on the server start, the server loads the configuration from the preprocessed file.

The server tracks changes in config files, as well as files and ZooKeeper nodes that were used when performing substitutions and overrides, and reloads the settings for users and clusters on the fly. This means that you can modify the cluster, users, and their settings without restarting the server.

[Original article](https://clickhouse.com/docs/en/operations/configuration-files/) <!--hide-->
