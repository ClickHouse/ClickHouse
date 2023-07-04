---
slug: /en/operations/configuration-files
sidebar_position: 50
sidebar_label: Configuration Files
---

# Configuration Files

The ClickHouse server can be configured with configuration files in XML or YAML syntax. In most installation types, the ClickHouse server runs with `/etc/clickhouse-server/config.xml` as default configuration file but it is also possible to specify the location of the configuration file manually at server startup using command line option `--config-file=` or `-C`. Additional configuration files may be placed into directory `config.d/` relative to the main configuration file, for example into directory `/etc/clickhouse-server/config.d/`. Files in this directory and the main configuration are merged in a preprocessing step before the configuration is applied in ClickHouse server. Configuration files are merged in alphabetical order. To simplify updates and improve modularization, it is best practice to keep the default `config.xml` file unmodified and place additional customization into `config.d/`.

It is possible to mix XML and YAML configuration files, for example you could have a main configuration file `config.xml` and additional configuration files `config.d/network.xml`, `config.d/timezone.yaml` and `config.d/keeper.yaml`. Mixing XML and YAML within a single configuration file is not supported. XML configuration files should use `<clickhouse>...</clickhouse>` as top-level tag. In YAML configuration files, `clickhouse:` is optional, the parser inserts it implicitly if absent.

## Overriding Configuration {#override}

The merge of configuration files behaves as one intuitively expects: The contents of both files are combined recursively, children with the same name are replaced by the element of the more specific configuration file. The merge can be customized using attributes `replace` and `remove`.
- Attribute `replace` means that the element is replaced by the specified one.
- Attribute `remove` means that the element is deleted.

To specify that a value of an element should be replaced by the value of an environment variable, you can use attribute `from_env`.

Example with `$MAX_QUERY_SIZE = 150000`:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

which is equal to

``` xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size/>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

## Substituting Configuration {#substitution}

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
