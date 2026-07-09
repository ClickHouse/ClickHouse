---
description: 'このページでは、ClickHouse server を XML または YAML 構文の設定ファイルで設定する方法について説明します。'
sidebar_label: '設定ファイル'
sidebar_position: 50
slug: /operations/configuration-files
title: '設定ファイル'
doc_type: 'guide'
---

:::note
XML ベースの設定プロファイルと設定ファイルは ClickHouse Cloud ではサポートされていません。そのため、ClickHouse Cloud には `config.xml` ファイルは存在しません。代わりに、設定プロファイルを通じて SQL コマンドで設定を管理してください。

詳細については、[&quot;設定の構成&quot;](/ja/manage/settings) を参照してください。
:::

ClickHouse server は、XML または YAML 構文の設定ファイルで設定できます。
ほとんどのインストール形態では、ClickHouse server は既定の設定ファイルとして `/etc/clickhouse-server/config.xml` を使用して実行されますが、サーバー起動時にコマンドラインオプション `--config-file` または `-C` を使用して、設定ファイルの場所を手動で指定することもできます。
追加の設定ファイルは、メイン設定ファイルからの相対パスである `config.d/` ディレクトリに配置できます。たとえば、`/etc/clickhouse-server/config.d/` ディレクトリです。
このディレクトリ内のファイルとメイン設定ファイルは、ClickHouse server に設定が適用される前の前処理段階でマージされます。
設定ファイルはアルファベット順にマージされます。
更新を容易にし、モジュール化を進めるため、既定の `config.xml` ファイルは変更せず、追加のカスタマイズは `config.d/` に配置するのがベストプラクティスです。
ClickHouse Keeper の構成は `/etc/clickhouse-keeper/keeper_config.xml` にあります。
同様に、Keeper 用の追加の設定ファイルは `/etc/clickhouse-keeper/keeper_config.d/` に配置する必要があります。

XML と YAML の設定ファイルは混在させることができます。たとえば、メイン設定ファイルを `config.xml` とし、追加の設定ファイルとして `config.d/network.xml`、`config.d/timezone.yaml`、`config.d/keeper.yaml` を配置できます。
1 つの設定ファイル内で XML と YAML を混在させることはサポートされていません。
XML 設定ファイルでは、最上位タグとして `<clickhouse>...</clickhouse>` を使用する必要があります。
YAML 設定ファイルでは、`clickhouse:` は省略可能で、省略した場合はパーサーが自動的に挿入します。

<div id="merging">
  ## 設定ファイルのマージ
</div>

2 つの設定ファイル (通常はメインの設定ファイルと `config.d/` 内の別の設定ファイル) は、次のルールでマージされます。

* ノード (つまり、要素に至るパス) が両方のファイルに存在し、属性 `replace` と `remove` のどちらも持たない場合、そのノードはマージ後の設定ファイルに含まれ、両方のノードの子要素も含めて再帰的にマージされます。
* 2 つのノードのいずれかに `replace` 属性がある場合、そのノードはマージ後の設定ファイルに含まれますが、含まれる子要素は `replace` 属性を持つノードのものだけです。
* 2 つのノードのいずれかに `remove` 属性がある場合、そのノードはマージ後の設定ファイルに含まれません (すでに存在する場合は削除されます) 。

たとえば、2 つの設定ファイルが次のように与えられているとします。

```xml title="config.xml"
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

と

```xml title="config.d/other_config.xml"
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

結果として生成される、マージ後の設定ファイルは次のとおりです。

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

<div id="from_env_zk">
  ### 環境変数およびZooKeeperノードによる置換
</div>

要素の値を環境変数の値で置き換えることを指定するには、属性 `from_env` を使用できます。

たとえば、環境変数 `$MAX_QUERY_SIZE = 150000` の場合:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

結果の設定は以下のようになります:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

同様に、`from_zk` (ZooKeeper node) も使用できます:

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

結果として、次のような設定になります。

```xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

<div id="default-values">
  #### デフォルト値
</div>

`from_env` または `from_zk` 属性を持つ要素には、追加で `replace="1"` 属性を指定することもできます (この属性は `from_env`/`from_zk` より前に記述する必要があります) 。
この場合、その要素にデフォルト値を定義できます。
要素の値には、設定されていれば環境変数または ZooKeeper ノードの値が使用され、設定されていない場合はデフォルト値が使用されます。

前の例を繰り返しますが、`MAX_QUERY_SIZE` は設定されていないものとします:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

次の設定となります:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

<div id="substitution-with-file-content">
  ## ファイル内容による置換
</div>

設定の一部をファイルの内容で置き換えることもできます。これには 2 つの方法があります。

* *値の置換*: 要素に属性 `incl` がある場合、その値は参照先ファイルの内容に置き換えられます。デフォルトでは、置換に使用するファイルのパスは `/etc/metrika.xml` です。これは、server config の [`include_from`](../operations/server-configuration-parameters/settings.md#include_from) 要素で変更できます。置換値は、このファイル内の `/clickhouse/substitution_name` 要素で指定します。`incl` で指定された置換が存在しない場合は、そのことがログに記録されます。存在しない置換を ClickHouse がログに記録しないようにするには、属性 `optional="true"` を指定してください (たとえば、[マクロ](../operations/server-configuration-parameters/settings.md#macros) の設定) 。
* *要素の置換*: 要素全体を置換で差し替えたい場合は、要素名として `include` を使用します。要素名 `include` は、属性 `from_zk = "/path/to/node"` と組み合わせて使用できます。この場合、要素の値は `/path/to/node` にある ZooKeeper ノードの内容に置き換えられます。また、XML サブツリー全体を ZooKeeper ノードとして保存しておけば、その内容をソース要素にそのまま完全に挿入することもできます。

この例を以下に示します。

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

追記ではなく、置換で取得した内容を既存の設定にマージしたい場合は、属性 `merge="true"` を使用できます。例: `<include from_zk="/some_path" merge="true">`。この場合、既存の設定は置換の内容とマージされ、既存の設定項目は置換の値で置き換えられます。

<div id="encryption">
  ## 設定の暗号化と秘匿
</div>

対称暗号化を使用して、設定要素 (たとえば、平文のパスワードや秘密鍵) を暗号化できます。
そのためには、まず[暗号化コーデック](../sql-reference/statements/create/table.md#encryption-codecs)を設定し、次に暗号化する要素に、暗号化コーデックの名前を値とする属性 `encrypted_by` を追加します。

属性 `from_zk`、`from_env`、`incl` や要素 `include` とは異なり、前処理済みファイルでは置換 (つまり暗号化された値の復号) は行われません。
復号は、サーバープロセスの実行時にのみ行われます。

たとえば:

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

[`from_env`](#from_env_zk) 属性と [`from_zk`](#from_env_zk) 属性は、`encryption_codecs` にも適用できます：

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

暗号鍵と暗号化された値は、いずれの設定ファイルでも定義できます。

`config.xml` の例を以下に示します。

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

</clickhouse>
```

`users.xml` の例を以下に示します。

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

値を暗号化するには、サンプルプログラム `encrypt_decrypt` を使用できます。

```bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

```text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

暗号化された設定要素を使用していても、暗号化された要素は前処理済み設定ファイルに引き続き表示されます。
これが ClickHouse のデプロイで問題になる場合は、2 つの対処方法があります。前処理済みファイルの権限を 600 に設定するか、属性 `hide_in_preprocessed` を使用してください。

たとえば:

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

<div id="user-settings">
  ## ユーザー設定
</div>

`config.xml` ファイルでは、ユーザー設定、プロファイル、クォータ用に別個の設定を指定できます。この設定への相対パスは `users_config` 要素で設定します。デフォルトでは `users.xml` です。`users_config` を省略した場合、ユーザー設定、プロファイル、クォータは `config.xml` に直接指定されます。

ユーザー設定は、`config.xml` や `config.d/` と同様に個別のファイルへ分割できます。
ディレクトリ名は、`.xml` 接尾辞を除いた `users_config` 設定に `.d` を連結したものとして定義されます。
デフォルトでは `users_config` が `users.xml` であるため、`users.d` ディレクトリが使用されます。

設定ファイルは、まず設定を考慮して[マージ](#merging)され、その後で include が処理されることに注意してください。

<div id="example">
  ## XMLの例
</div>

たとえば、次のようにユーザーごとに個別の設定ファイルを用意できます。

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

<div id="example-1">
  ## YAML の例
</div>

ここでは、YAML で記述されたデフォルトの設定を確認できます: [`config.yaml.example`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

ClickHouse の設定では、YAML フォーマットと XML フォーマットの間にいくつかの違いがあります。
以下に、YAML フォーマットで設定を記述する際のヒントを示します。

テキスト値を持つ XML タグは、YAML ではキー・バリューのペアとして表されます

```yaml
key: value
```

対応するXML：

```xml
<key>value</key>
```

入れ子になった XML ノードは、YAML のマップで表されます。

```yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

対応するXML:

```xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

同じXMLタグを複数回作成するには、YAMLシーケンスを使用します：

```yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

対応するXML:

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

XML属性を指定するには、属性キーの先頭に `@` プレフィックスを付けます。`@` は YAML 標準で予約されているため、必ず二重引用符で囲んでください。

```yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

対応するXML：

```xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

YAMLシーケンス内でも属性を使用できます:

```yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

対応するXML：

```xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

前述の構文では、XML 属性を持つ XML のテキストノードを YAML として表現できません。この特殊なケースは、
`#text` 属性キーを使用することで表現できます。

```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

対応する XML:

```xml
<map_key attr1="value1">value2</map>
```

<div id="implementation-details">
  ## 実装の詳細
</div>

各設定ファイルについて、サーバーは起動時に `file-preprocessed.xml` ファイルも生成します。これらのファイルには、適用済みのすべての置換とオーバーライドが含まれており、情報参照用として使用されます。設定ファイルで ZooKeeper の置換が使用されていて、サーバー起動時に ZooKeeper を利用できない場合、サーバーは前処理済みファイルから設定を読み込みます。

サーバーは、設定ファイルの変更に加え、置換やオーバーライドの実行時に使用されたファイルおよび ZooKeeper ノードの変更も追跡し、ユーザーとクラスターの設定を動的に再読み込みします。つまり、サーバーを再起動しなくても、クラスター、ユーザー、およびそれらの設定を変更できます。