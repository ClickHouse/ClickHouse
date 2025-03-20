---
description: 'This page explains how ClickHouse server can be configured with configuration
  files in XML or YAML syntax.'
sidebar_label: 'Configuration Files'
sidebar_position: 50
slug: /operations/configuration-files
title: 'Configuration Files'
---

:::note
Please note that XML-based Settings Profiles and configuration files are currently not supported for ClickHouse Cloud. Therefore, in ClickHouse Cloud, you won't find a config.xml file. Instead, you should use SQL commands to manage settings through Settings Profiles.

For further details, see ["Configuring Settings"](/manage/settings)
:::

The ClickHouse server can be configured with configuration files in XML or YAML syntax.
In most installation types, the ClickHouse server runs with `/etc/clickhouse-server/config.xml` as default configuration file, but it is also possible to specify the location of the configuration file manually at server startup using command line option `--config-file` or `-C`.
Additional configuration files may be placed into directory `config.d/` relative to the main configuration file, for example into directory `/etc/clickhouse-server/config.d/`.
Files in this directory and the main configuration are merged in a preprocessing step before the configuration is applied in ClickHouse server.
Configuration files are merged in alphabetical order.
To simplify updates and improve modularization, it is best practice to keep the default `config.xml` file unmodified and place additional customization into `config.d/`.
The ClickHouse keeper configuration lives in `/etc/clickhouse-keeper/keeper_config.xml`.
Thus, additional files need to be placed in `/etc/clickhouse-keeper/keeper_config.d/`.


It is possible to mix XML and YAML configuration files, for example you could have a main configuration file `config.xml` and additional configuration files `config.d/network.xml`, `config.d/timezone.yaml` and `config.d/keeper.yaml`.
Mixing XML and YAML within a single configuration file is not supported.
XML configuration files should use `<clickhouse>...</clickhouse>` as top-level tag.
In YAML configuration files, `clickhouse:` is optional, if absent the parser inserts it automatically.

## Merging Configuration {#merging}

Two configuration files (usually the main configuration file and another configuration files from `config.d/`) are merged as follows:

- If a node (i.e. a path leading to an element) appears in both files and does not have attributes `replace` or `remove`, it is included in the merged configuration file and children from both nodes are included and merged recursively.
- If one of both nodes contains attribute `replace`, it is included in the merged configuration file but only children from the node with attribute `replace` are included.
- If one of both nodes contains attribute `remove`, the node is not included in the merged configuration file (if it exists already, it is deleted).

Example:


```xml
<!-- config.xml -->
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
    </config_a>
    <config_b>
        <setting_2>2</setting_2>
    </config_b>
    <config_c>
        <setting_3>3</setting_3>
    </config_c>
</clickhouse>
```

and

```xml
<!-- config.d/other_config.xml -->
<clickhouse>
    <config_a>
        <setting_4>4</setting_4>
    </config_a>
    <config_b replace="replace">
        <setting_5>5</setting_5>
    </config_b>
    <config_c remove="remove">
        <setting_6>6</setting_6>
    </config_c>
</clickhouse>
```

generates merged configuration file:

```xml
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
        <setting_4>4</setting_4>
    </config_a>
    <config_b>
        <setting_5>5</setting_5>
    </config_b>
</clickhouse>
```

### Substitution by Environment Variables and ZooKeeper Nodes {#from_env_zk}

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

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

The same is possible using `from_zk` (ZooKeeper node):

```xml
<clickhouse>
    <postgresql_port from_zk="/zk_configs/postgresql_port"/>
</clickhouse>
```

```shell
# clickhouse-keeper-client
/ :) touch /zk_configs
/ :) create /zk_configs/postgresql_port "9005"
/ :) get /zk_configs/postgresql_port
9005
```

which is equal to


```xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

#### Default Values {#default-values}

An element with `from_env` or `from_zk` attribute may additionally have attribute `replace="1"` (the latter must appear before `from_env`/`from_zk`).
In this case, the element may define a default value.
The element takes on the value of the environment variable or ZooKeeper node if set, otherwise the default value.

Previous example but assuming `MAX_QUERY_SIZE` is not set:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

Result:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

## Substitution with File Content {#substitution-with-file-content}

It is also possible to replace parts of the configuration by file contents. This can be done in two ways:

- *Substituting Values*: If an element has attribute `incl`, its value will be replaced by the content of the referenced file. By default, the path to the file with substitutions is `/etc/metrika.xml`. This can be changed in the [include_from](../operations/server-configuration-parameters/settings.md#include_from) element in the server config. The substitution values are specified in `/clickhouse/substitution_name` elements in this file. If a substitution specified in `incl` does not exist, it is recorded in the log. To prevent ClickHouse from logging missing substitutions, specify attribute `optional="true"` (for example, settings for [macros](../operations/server-configuration-parameters/settings.md#macros)).

- *Substituting elements*: If you want to replace the entire element with a substitution, use `include` as the element name. Element name `include` can be combined with attribute `from_zk = "/path/to/node"`. In this case, the element value is replaced by the contents of the ZooKeeper node at `/path/to/node`. This also works with you store an entire XML subtree as a Zookeeper node, it will be fully inserted into the source element.

Example:

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

If you want to merge the substituting content with the existing configuration instead of appending you can use attribute `merge="true"`, for example: `<include from_zk="/some_path" merge="true">`. In this case, the existing configuration will be merged with the content from the substitution and the existing configuration settings will be replaced with values from substitution.

## Encrypting and Hiding Configuration {#encryption}

You can use symmetric encryption to encrypt a configuration element, for example, a plaintext password or private key.
To do so, first configure the [encryption codec](../sql-reference/statements/create/table.md#encryption-codecs), then add attribute `encrypted_by` with the name of the encryption codec as value to the element to encrypt.

Unlike attributes `from_zk`, `from_env` and `incl`, or element `include`, no substitution (i.e. decryption of the encrypted value) is performed in the preprocessed file.
Decryption happens only at runtime in the server process.

Example:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex>00112233445566778899aabbccddeeff</key_hex>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

The attributes [from_env](#from_env_zk) and [from_zk](#from_env_zk) can also be applied to ```encryption_codecs```:
```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_env="CLICKHOUSE_KEY_HEX"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Encryption keys and encrypted values can be defined in either config file.

Example `config.xml`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

</clickhouse>
```

Example `users.xml`:

```xml
<clickhouse>

    <users>
        <test_user>
            <password encrypted_by="AES_128_GCM_SIV">96280000000D000000000030D4632962295D46C6FA4ABF007CCEC9C1D0E19DA5AF719C1D9A46C446</password>
            <profile>default</profile>
        </test_user>
    </users>

</clickhouse>
```

To encrypt a value, you can use the (example) program `encrypt_decrypt`:

Example:

```bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

```text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

Even with encrypted configuration elements, encrypted elements still appear in the preprocessed configuration file.
If this is a problem for your ClickHouse deployment, we suggest two alternatives: Either set file permissions of the preprocessed file to 600 or use attribute `hide_in_preprocessed`.

Example:

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

## User Settings {#user-settings}

The `config.xml` file can specify a separate config with user settings, profiles, and quotas. The relative path to this config is set in the `users_config` element. By default, it is `users.xml`. If `users_config` is omitted, the user settings, profiles, and quotas are specified directly in `config.xml`.

Users configuration can be split into separate files similar to `config.xml` and `config.d/`.
Directory name is defined as `users_config` setting without `.xml` postfix concatenated with `.d`.
Directory `users.d` is used by default, as `users_config` defaults to `users.xml`.

Note that configuration files are first [merged](#merging) taking into account settings, and includes are processed after that.

## XML example {#example}

For example, you can have separate config file for each user like this:

```bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

```xml
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

## YAML examples {#example-1}

Here you can see default config written in YAML: [config.yaml.example](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

There are some differences between YAML and XML formats in terms of ClickHouse configurations. Here are some tips for writing a configuration in YAML format.

An XML tag with a text value is represented by a YAML key-value pair
```yaml
key: value
```

Corresponding XML:
```xml
<key>value</key>
```

A nested XML node is represented by a YAML map:
```yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

Corresponding XML:
```xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

To create the same XML tag multiple times, use a YAML sequence:
```yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

Corresponding XML:
```xml
<seq_key>val1</seq_key>
<seq_key>val2</seq_key>
<seq_key>
    <key1>val3</key1>
</seq_key>
<seq_key>
    <map>
        <key2>val4</key2>
        <key3>val5</key3>
    </map>
</seq_key>
```

To provide an XML attribute, you can use an attribute key with a `@` prefix. Note that `@` is reserved by YAML standard, so must be wrapped in double quotes:
```yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

Corresponding XML:
```xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

It is also possible to use attributes in YAML sequence:
```yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

Corresponding XML:
```xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

The aforementioned syntax does not allow to express XML text nodes with XML attributes as YAML. This special case can be achieved using an
`#text` attribute key:
```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

Corresponding XML:
```xml
<map_key attr1="value1">value2</map>
```

## Implementation Details {#implementation-details}

For each config file, the server also generates `file-preprocessed.xml` files when starting. These files contain all the completed substitutions and overrides, and they are intended for informational use. If ZooKeeper substitutions were used in the config files but ZooKeeper is not available on the server start, the server loads the configuration from the preprocessed file.

The server tracks changes in config files, as well as files and ZooKeeper nodes that were used when performing substitutions and overrides, and reloads the settings for users and clusters on the fly. This means that you can modify the cluster, users, and their settings without restarting the server.
