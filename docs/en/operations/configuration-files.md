---
slug: /en/operations/configuration-files
sidebar_position: 50
sidebar_label: Configuration Files
---

# Configuration Files

The ClickHouse server can be configured with configuration files in XML or YAML syntax. In most installation types, the ClickHouse server runs with `/etc/clickhouse-server/config.xml` as default configuration file, but it is also possible to specify the location of the configuration file manually at server startup using command line option `--config-file=` or `-C`. Additional configuration files may be placed into directory `config.d/` relative to the main configuration file, for example into directory `/etc/clickhouse-server/config.d/`. Files in this directory and the main configuration are merged in a preprocessing step before the configuration is applied in ClickHouse server. Configuration files are merged in alphabetical order. To simplify updates and improve modularization, it is best practice to keep the default `config.xml` file unmodified and place additional customization into `config.d/`.
(The ClickHouse keeper configuration lives in `/etc/clickhouse-keeper/keeper_config.xml` and thus the additional files need to be placed in `/etc/clickhouse-keeper/keeper_config.d/` )


It is possible to mix XML and YAML configuration files, for example you could have a main configuration file `config.xml` and additional configuration files `config.d/network.xml`, `config.d/timezone.yaml` and `config.d/keeper.yaml`. Mixing XML and YAML within a single configuration file is not supported. XML configuration files should use `<clickhouse>...</clickhouse>` as top-level tag. In YAML configuration files, `clickhouse:` is optional, the parser inserts it implicitly if absent.

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

### Using from_env and from_zk

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
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

The same is possible using `from_zk`:

``` xml
<clickhouse>
    <postgresql_port from_zk="/zk_configs/postgresql_port"/>
</clickhouse>
```

```
# clickhouse-keeper-client
/ :) touch /zk_configs
/ :) create /zk_configs/postgresql_port "9005"
/ :) get /zk_configs/postgresql_port
9005
```

which is equal to


``` xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

#### Default values for from_env and from_zk attributes

It's possible to set the default value and substitute it only if the environment variable or zookeeper node is set using `replace="1"` (must be declared before from_env).

With previous example, but `MAX_QUERY_SIZE` is unset:

``` xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

will take the default value

``` xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

## Substituting Configuration {#substitution}

The config can define substitutions. There are two types of substitutions:

- If an element has the `incl` attribute, the corresponding substitution from the file will be used as the value. By default, the path to the file with substitutions is `/etc/metrika.xml`. This can be changed in the [include_from](../operations/server-configuration-parameters/settings.md#include_from) element in the server config. The substitution values are specified in `/clickhouse/substitution_name` elements in this file. If a substitution specified in `incl` does not exist, it is recorded in the log. To prevent ClickHouse from logging missing substitutions, specify the `optional="true"` attribute (for example, settings for [macros](../operations/server-configuration-parameters/settings.md#macros)).

- If you want to replace an entire element with a substitution, use `include` as the element name. Substitutions can also be performed from ZooKeeper by specifying attribute `from_zk = "/path/to/node"`. In this case, the element value is replaced with the contents of the Zookeeper node at `/path/to/node`. This also works with you store an entire XML subtree as a Zookeeper node, it will be fully inserted into the source element.

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

If you want to merge the substituting content with the existing configuration instead of appending you can use attribute `merge="true"`, for example: `<include from_zk="/some_path" merge="true">`. In this case, the existing configuration will be merged with the content from the substitution and the existing configuration settings will be replaced with values from substitution.

## Encrypting and Hiding Configuration {#encryption}

You can use symmetric encryption to encrypt a configuration element, for example, a plaintext password or private key. To do so, first configure the [encryption codec](../sql-reference/statements/create/table.md#encryption-codecs), then add attribute `encrypted_by` with the name of the encryption codec as value to the element to encrypt.

Unlike attributes `from_zk`, `from_env` and `incl` (or element `include`), no substitution, i.e. decryption of the encrypted value, is performed in the preprocessed file. Decryption happens only at runtime in the server process.

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

To encrypt a value, you can use the (example) program `encrypt_decrypt`:

Example:

``` bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

``` text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

Even with encrypted configuration elements, encrypted elements still appear in the preprocessed configuration file. If this is a problem for your ClickHouse deployment, we suggest two alternatives: either set file permissions of the preprocessed file to 600 or use the `hide_in_preprocessed` attribute.

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

An XML tag with a text value is represented by a YAML key-value pair
``` yaml
key: value
```

Corresponding XML:
``` xml
<key>value</key>
```

A nested XML node is represented by a YAML map:
``` yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

Corresponding XML:
``` xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

To create the same XML tag multiple times, use a YAML sequence:
``` yaml
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
``` yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

Corresponding XML:
``` xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

It is also possible to use attributes in YAML sequence:
``` yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

Corresponding XML:
``` xml
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
