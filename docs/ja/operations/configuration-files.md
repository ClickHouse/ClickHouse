---
slug: /ja/operations/configuration-files
sidebar_position: 50
sidebar_label: 設定ファイル
---

# 設定ファイル {#configuration-files}

ClickHouseサーバーは、XMLまたはYAML構文の設定ファイルで構成できます。ほとんどのインストールタイプでは、ClickHouseサーバーはデフォルトの設定ファイルとして `/etc/clickhouse-server/config.xml` で実行されますが、サーバーの起動時にコマンドラインオプション `--config-file=` または `-C` を使用して設定ファイルの場所を手動で指定することも可能です。追加の設定ファイルは、メインの設定ファイルの相対ディレクトリ `config.d/` に配置することができます。例えば、ディレクトリ `/etc/clickhouse-server/config.d/` に配置します。このディレクトリとメインの設定がClickHouseサーバーで設定を適用する前に前処理ステップでマージされます。設定ファイルはアルファベット順でマージされます。更新を簡素化し、モジュール化を改善するためには、デフォルトの `config.xml` ファイルを変更せず、追加のカスタマイズを `config.d/` に配置するのが最良のプラクティスです。
(ClickHouse keeper の設定は `/etc/clickhouse-keeper/keeper_config.xml` に存在するため、追加ファイルは `/etc/clickhouse-keeper/keeper_config.d/` に配置する必要があります。)

XML および YAML 設定ファイルを混在させることが可能です。例えば、メイン設定ファイル `config.xml` と追加の設定ファイル `config.d/network.xml`、`config.d/timezone.yaml`、`config.d/keeper.yaml` を持つことができます。単一の設定ファイル内で XML と YAML を混在させることはサポートされていません。XML 設定ファイルは `<clickhouse>...</clickhouse>` をトップレベルタグとして使用する必要があります。YAML 設定ファイルでは、`clickhouse:` はオプションであり、存在しない場合にはパーサーが自動的に補完します。

## 設定のマージ {#merging}

通常、2つの設定ファイル（メインの設定ファイルと `config.d/` からの他の設定ファイル）が次のようにマージされます：

- ノード（つまり、要素へのパス）が両方のファイルに存在し、`replace` または `remove` 属性を持たない場合、マージされた設定ファイルに含まれ、両方のノードの子が含まれて再帰的にマージされます。
- もしどちらかのノードが `replace` 属性を含んでいた場合、そのノードのみがマージされた設定ファイルに含まれますが、`replace` 属性を持つノードの子要素のみが含まれます。
- もしどちらかのノードが `remove` 属性を含んでいた場合、そのノードはマージされた設定ファイルには含まれません（既に存在する場合は削除されます）。

例：

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

および

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

生成されたマージされた設定ファイル：

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

### from_env と from_zk の使用

要素の値を環境変数の値に置き換えるために、属性 `from_env` を使用できます。

例：`$MAX_QUERY_SIZE = 150000` の場合：

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

これは次のようになります：

``` xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

同様に `from_zk` を使用することも可能です：

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

これも次のようになります：

``` xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

#### from_env と from_zk 属性のデフォルト値

デフォルト値を設定し、環境変数またはZooKeeperノードが設定されている場合にのみ置き換えることができます。`replace="1"` を使用してください（from_env より前に宣言する必要があります）。

前の例を使用し、`MAX_QUERY_SIZE` が設定されていない場合：

``` xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

デフォルト値を使用します：

``` xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

## 設定の置換 {#substitution}

設定ファイルは置換を定義できます。置換には2つのタイプがあります：

- 要素に `incl` 属性がある場合、ファイルから対応する置換をその値として使用します。デフォルトでは、置換が記述されたファイルへのパスは `/etc/metrika.xml` です。これはサーバー設定の [include_from](../operations/server-configuration-parameters/settings.md#include_from) 要素で変更可能です。置換値は、このファイル内の `/clickhouse/substitution_name` 要素で指定されます。 `incl` で指定された置換が存在しない場合、その内容がログに記録されます。ClickHouseが不足している置換をログに記録しないようにするには、`optional="true"` 属性を指定してください（例：[macros](../operations/server-configuration-parameters/settings.md#macros) の設定）。

- 要素全体を置換で置き換えたい場合は、要素名として `include` を使用してください。Zookeeperからの置換も、属性 `from_zk = "/path/to/node"` を指定することで行うことができます。この場合、要素の値は `/path/to/node` のZookeeperノードの内容に置き換えられます。XMLサブツリー全体をZookeeperノードとして保存している場合も同様に、ソース要素に完全に挿入されます。

XML置換の例：

```xml
<clickhouse>
    <!-- `<profiles>` 要素に `/profiles-in-zookeeper` ZK パスで見つかった XML サブツリーを追加します。 -->
    <profiles from_zk="/profiles-in-zookeeper" />

    <users>
        <!-- `/users-in-zookeeper` ZK パスで見つかったサブツリーで `include` 要素を置き換えます。 -->
        <include from_zk="/users-in-zookeeper" />
        <include from_zk="/other-users-in-zookeeper" />
    </users>
</clickhouse>
```

既存の構成と置換内容をマージしたい場合は、属性 `merge="true"` を使用することができます。例えば：`<include from_zk="/some_path" merge="true">`。この場合、既存の設定は置換の内容とマージされ、既存の設定が置換からの値に置き換えられます。

## 設定の暗号化と隠蔽 {#encryption}

構成要素、例えばプレーンテキストのパスワードや秘密鍵を暗号化するために対称暗号化を使用することができます。そのためには、まず[暗号化コーデック](../sql-reference/statements/create/table.md#encryption-codecs)を設定し、暗号化する要素に `encrypted_by` 属性と暗号化コーデックの名前を追加します。

`from_zk`、`from_env` および `incl` 属性（または要素 `include`）とは異なり、置換、すなわち暗号化された値の復号化は前処理ファイルで行われません。復号化はサーバープロセスで実行時にのみ行われます。

例：

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

値を暗号化するには、（例の）プログラム `encrypt_decrypt` を使用できます：

例：

``` bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

``` text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

暗号化された設定要素であっても、暗号化された要素は前処理済みの設定ファイルに表示されます。これがClickHouseの展開に問題を引き起こす場合は、二つの代替案を提案します：1. 前処理済みファイルのファイル権限を600に設定する、もしくは 2. `hide_in_preprocessed` 属性を使用することです。

例：

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

## ユーザー設定 {#user-settings}

`config.xml` ファイルには、ユーザー設定、プロファイル、およびクォータを含む別の設定を指定することができます。この設定への相対パスは `users_config` 要素に設定されます。デフォルトでは `users.xml` です。`users_config` が省略された場合は、ユーザー設定、プロファイル、およびクォータは `config.xml` に直接記載されます。

ユーザー設定は、`config.xml` および `config.d/` と同様に、別々のファイルに分割することができます。ディレクトリ名は、`.xml` 接尾辞を除いた `users_config` 設定と `.d` を連結して定義されます。デフォルトでは `users.d` が使用され、`users_config` は `users.xml` にデフォルト設定されています。

設定ファイルはまず [マージ](#merging) され、設定を考慮し、その後でインクルードが処理されることに留意してください。

## XML例 {#example}

たとえば、以下のように各ユーザー用に個別の設定ファイルを用意することができます：

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

## YAML例 {#example}

ここでは、YAMLで書かれたデフォルト設定を見ることができます：[config.yaml.example](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example)。

ClickHouse の設定に関して、YAML と XML フォーマットにはいくつかの違いがあります。ここでは、YAML フォーマットでの設定を書くためのいくつかのヒントを示します。

テキスト値を持つ XML タグは、YAML のキーと値のペアで表されます
``` yaml
key: value
```

対応する XML：
``` xml
<key>value</key>
```

ネストされた XML ノードは YAML マップで表されます：
``` yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

対応する XML：
``` xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

同じXMLタグを複数回作成するには、YAMLシーケンスを使用します：
``` yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

対応する XML：
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

XML属性を指定するには、`@` プレフィックスを付けた属性キーを使用できます。`@` は YAML 標準で予約されているため、ダブルクォートで囲む必要があります：
``` yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

対応する XML：
``` xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

YAML シーケンスに属性を使用することも可能です：
``` yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

対応する XML：
``` xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

前述の構文では、XMLテキストノードをXML属性としてYAMLで表現することはできません。この特殊なケースは、`#text` 属性キーを使用して実現できます：
```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

対応する XML：
```xml
<map_key attr1="value1">value2</map_key>
```

## 実装の詳細 {#implementation-details}

各設定ファイルについて、サーバーは起動時に `file-preprocessed.xml` ファイルも生成します。これらのファイルは、すべての置換とオーバーライドが完了した状態を含んでおり、情報提供用に意図されています。設定ファイルでZooKeeperの置換が使用されているがサーバー起動時にZooKeeperが利用できない場合、サーバーは前処理済みファイルから設定をロードします。

サーバーは設定ファイルの変更や、置換やオーバーライドを行う際に使用されたファイルおよびZooKeeperノードを追跡し、ユーザーやクラスターの設定を自動でリロードします。これにより、サーバーを再起動せずにクラスターやユーザーの設定を変更することが可能です。
