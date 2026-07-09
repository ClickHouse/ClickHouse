---
title: ログソース外のサーバー設定
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

ClickHouse Cloud デプロイメントではデフォルトで有効です。

お使いの環境でこの設定がデフォルトで有効になっていない場合は、ClickHouse のインストール方法に応じて、以下の手順で有効または無効にできます。

**有効化**

非同期メトリックログの履歴収集 [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md) を手動で有効にするには、次の内容で `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` を作成します。

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**無効化**

`asynchronous_metric_log` 設定を無効にするには、以下の内容で `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` ファイルを作成します。

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

プロキシ経由で接続するクライアントの認証に、接続元アドレスを使用します。

:::note
転送されたアドレスは容易に偽装できるため、この設定の使用には特に注意が必要です。このような認証を受け付けるサーバーには直接アクセスせず、必ず信頼できるプロキシ経由のみにしてください。
:::

<div id="backups">
  ## バックアップ
</div>

[`BACKUP` と `RESTORE`](/ja/operations/backup/overview) 文の実行時に使用する、バックアップの設定です。

以下の設定はサブタグで設定できます。

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','同じホスト上で複数のバックアップ操作を同時実行できるかどうかを指定します。', 'true'),
    ('allow_concurrent_restores', 'Bool', '同じホスト上で複数のリストア操作を同時実行できるかどうかを指定します。', 'true'),
    ('allowed_disk', 'String', '`File()` を使用する際のバックアップ先ディスクです。`File` を使用するには、この設定を指定する必要があります。', ''),
    ('allowed_path', 'String', '`File()` を使用する際のバックアップ先パスです。`File` を使用するには、この設定を指定する必要があります。', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', '収集したメタデータの比較後に不整合があった場合、待機に入るまでにメタデータ収集を試行する回数です。', '2'),
    ('collect_metadata_timeout', 'UInt64', 'バックアップ中にメタデータを収集する際の Milliseconds 単位のタイムアウトです。', '600000'),
    ('compare_collected_metadata', 'Bool', 'true の場合、バックアップ中に変更されていないことを確認するために、収集したメタデータを既存のメタデータと比較します。', 'true'),
    ('create_table_timeout', 'UInt64', 'リストア中にテーブルを作成する際の Milliseconds 単位のタイムアウトです。', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', '協調バックアップ/リストア中に不正なバージョンエラーが発生した後の再試行の最大回数です。', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', '次にメタデータ収集を試行するまでの最大待機時間（Milliseconds 単位）です。', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', '次にメタデータ収集を試行するまでの最小待機時間（Milliseconds 単位）です。', '5000'),
    ('remove_backup_files_after_failure', 'Bool', '`BACKUP` コマンドが失敗した場合、ClickHouse は失敗前にバックアップへすでにコピーされたファイルの削除を試みます。そうでない場合は、コピー済みのファイルをそのまま残します。', 'true'),
    ('sync_period_ms', 'UInt64', '協調バックアップ/リストアにおける同期周期（Milliseconds 単位）です。', '5000'),
    ('test_inject_sleep', 'Bool', 'テスト用の待機設定です。', 'false'),
    ('test_randomize_order', 'Bool', 'true の場合、テスト目的で特定の操作の順序をランダム化します。', 'false'),
    ('zookeeper_path', 'String', '`ON CLUSTER` 句を使用する際に、バックアップおよびリストアのメタデータを保存する ZooKeeper 内のパスです。', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
  */ }

| 設定                                                  | 型      | 説明                                                                                              | デフォルト                 |
| :-------------------------------------------------- | :----- | :---------------------------------------------------------------------------------------------- | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | 同じホスト上で複数のバックアップ操作を同時実行できるかどうかを指定します。                                                           | `true`                |
| `allow_concurrent_restores`                         | Bool   | 同じホスト上で複数のリストア操作を同時実行できるかどうかを指定します。                                                             | `true`                |
| `allowed_disk`                                      | String | `File()` の使用時にバックアップ先として使用するディスクです。`File` を使用するには、この設定を指定する必要があります。                             | &#96;&#96;            |
| `allowed_path`                                      | String | `File()` の使用時にバックアップ先として使用するパスです。`File` を使用するには、この設定を指定する必要があります。                               | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | 収集したメタデータを比較した結果、不整合があった場合に、スリープに入る前にメタデータ収集を試みる回数です。                                           | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | バックアップ中にメタデータを収集する際のタイムアウト時間 (ミリ秒) です。                                                          | `600000`              |
| `compare_collected_metadata`                        | Bool   | true の場合、バックアップ中にメタデータが変更されていないことを確認するため、収集したメタデータを既存のメタデータと比較します。                              | `true`                |
| `create_table_timeout`                              | UInt64 | リストア中にテーブルを作成する際のタイムアウト時間 (ミリ秒) です。                                                             | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | 協調バックアップ/リストア中に不正なバージョンのエラーが発生した後の再試行の最大回数です。                                                   | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | 次にメタデータ収集を試みるまでの最大スリープ時間 (ミリ秒) です。                                                              | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | 次にメタデータ収集を試みるまでの最小スリープ時間 (ミリ秒) です。                                                              | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | `BACKUP` コマンドが失敗した場合、ClickHouse は失敗前にバックアップへすでにコピーされたファイルの削除を試みます。そうでない場合は、コピー済みのファイルをそのまま残します。 | `true`                |
| `sync_period_ms`                                    | UInt64 | 協調バックアップ/リストアの同期周期 (ミリ秒) です。                                                                    | `5000`                |
| `test_inject_sleep`                                 | Bool   | テスト用のスリープ設定です。                                                                                  | `false`               |
| `test_randomize_order`                              | Bool   | true の場合、テスト目的で特定の操作の順序をランダム化します。                                                               | `false`               |
| `zookeeper_path`                                    | String | `ON CLUSTER` 句を使用する際に、バックアップおよびリストアのメタデータを保存する ZooKeeper 内のパスです。                                | `/clickhouse/backups` |

この設定のデフォルト構成は次のとおりです。

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

さまざまなバックグラウンドプールを通じて実行される、すべてのバックグラウンドタスクに関する情報が含まれています。

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

[Bcrypt algorithm](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/) を使用する `bcrypt_password` 認証タイプのワークファクターです。
ワークファクターは、ハッシュの計算とパスワードの検証に必要な計算量と時間を定義します。

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
認証頻度の高いアプリケーションでは、
bcrypt はワークファクターが高いほど計算負荷が大きくなるため、
別の認証方式の採用を検討してください。
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

`true` に設定すると、特定のエンジンでテーブルを作成する際に、grant が必要になります (例: `GRANT TABLE ENGINE ON TinyLog to user`) 。

:::note
デフォルトでは、後方互換性のため、特定の table engine を指定したテーブルの作成時には grant は無視されますが、これを `true` に設定することでこの動作を変更できます。
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

組み込みDictionaryを再読み込みするまでの間隔を秒単位で指定します。

ClickHouse は x 秒ごとに組み込みDictionaryを再読み込みします。これにより、サーバーを再起動せずにDictionaryを「その場で」編集できます。

**例**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## 圧縮
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) エンジンのテーブルにおけるデータ圧縮の設定です。

:::note
ClickHouse を使い始めたばかりの場合は、これを変更しないことをおすすめします。
:::

**設定テンプレート**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**`<case>` フィールド**:

* `min_part_size` – データパートの最小サイズ。
* `min_part_size_ratio` – データパートのサイズの、テーブルサイズに対する比率。
* `method` – 圧縮方式。有効な値: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`。
* `level` – 圧縮レベル。[Codecs](/ja/sql-reference/statements/create/table#general-purpose-codecs)を参照してください。

:::note
複数の `<case>` セクションを設定できます。
:::

**条件が満たされた場合のアクション**:

* データパートが条件セットに一致した場合、ClickHouse は指定された圧縮方式を使用します。
* データパートが複数の条件セットに一致した場合、ClickHouse は最初に一致した条件セットを使用します。

:::note
データパートに対してどの条件にも一致しない場合、ClickHouse は `lz4` 圧縮を使用します。
:::

**例**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

<div id="encryption">
  ## encryption
</div>

[暗号化コーデック](/ja/sql-reference/statements/create/table#encryption-codecs)で使用する秘密鍵を取得するコマンドを設定します。秘密鍵 (複数可) は、環境変数に格納するか、設定ファイルで指定する必要があります。

秘密鍵には、16バイト長の16進値または文字列を使用できます。

**Example**

configから読み込む場合:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
秘密鍵を設定ファイルに保存することは推奨されません。安全ではないためです。秘密鍵は安全なディスク上の別の設定ファイルに移し、その設定ファイルへのシンボリックリンクを `config.d/` フォルダーに配置できます。
:::

秘密鍵が16進数の場合に設定から読み込む例:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

環境変数から秘密鍵を読み込み中:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

ここで、`current_key_id` は暗号化に使用する現在のキーを設定し、指定したすべてのキーは復号に使用できます。

これらの各メソッドは、複数のキーに適用できます。

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

ここで `current_key_id` は、暗号化に使用されている現在の鍵を示します。

また、ユーザーは 12 バイト長の nonce を追加できます (デフォルトでは、暗号化および復号の処理に、ゼロバイトで構成された nonce が使用されます) :

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

または、16進数で指定することもできます:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
上記の内容はすべて `aes_256_gcm_siv` にも適用できます (ただし、秘密鍵は32バイト長である必要があります) 。
:::

<div id="error_log">
  ## error_log
</div>

これはデフォルトで無効です。

**有効化**

[`system.error_log`](../../operations/system-tables/error_log.md) の error history collection を手動で有効にするには、次の内容で `/etc/clickhouse-server/config.d/error_log.xml` を作成します。

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**無効化**

`error_log` 設定を無効にするには、以下の内容で `/etc/clickhouse-server/config.d/disable_error_log.xml` ファイルを作成します。

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

[custom settings](/ja/operations/settings/query-level#custom_settings) で使用するプレフィックスの一覧です。
複数のプレフィックスはカンマで区切ります。

**例**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**関連項目**

* [カスタム設定](/ja/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

コアダンプファイルのサイズに対するソフトリミットを設定します。

:::note
ハードリミットはシステムツールで設定します
:::

**例**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

デフォルトの設定プロファイルです。設定プロファイルは、`user_config` 設定で指定されたファイルに格納されています。

**例**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

Dictionary 用の設定ファイルへのパスです。

パス:

* 絶対パス、またはサーバー設定ファイルからの相対パスを指定します。
* パスにはワイルドカード `*` と `?` を含めることができます。

関連項目:

* &quot;[Dictionary](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**例**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

実行可能なユーザー定義関数の設定ファイルのパスです。

パス:

* 絶対パス、またはサーバーの設定ファイルを基準とする相対パスを指定します。
* パスにはワイルドカード * および ? を含めることができます。

関連項目:

* &quot;[Executable User Defined Functions](/ja/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**例**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

[Graphite](https://github.com/graphite-project) にデータを送信します。

設定:

* `host` – Graphite サーバーのホスト。
* `port` – Graphite サーバーのポート。
* `interval` – 送信間隔 (秒) 。
* `timeout` – データ送信のタイムアウト (秒) 。
* `root_path` – キーのプレフィックス。
* `metrics` – [system.metrics](/ja/operations/system-tables/metrics) テーブルからデータを送信します。
* `events` – [system.events](/ja/operations/system-tables/events) テーブルから、一定期間に蓄積された差分データを送信します。
* `events_cumulative` – [system.events](/ja/operations/system-tables/events) テーブルから累積データを送信します。
* `asynchronous_metrics` – [system.asynchronous&#95;metrics](/ja/operations/system-tables/asynchronous_metrics) テーブルからデータを送信します。

複数の `<graphite>` 要素を設定できます。たとえば、異なる種類のデータを異なる間隔で送信する場合に使用できます。

**例**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<div id="graphite_rollup">
  ## graphite_rollup
</div>

Graphite のデータを間引くための設定です。

詳細については、[GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md) を参照してください。

**例**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

<div id="http_handlers">
  ## http_handlers
</div>

カスタム HTTP ハンドラーを使用できます。
新しい http ハンドラーを追加するには、新しい `<rule>` を追加するだけです。
ルールは定義された順に上から下へチェックされ、
最初に一致したもののハンドラーが実行されます。
一致条件のないルール (`handler` のみ) はすべての HTTPリクエストに一致します。ルールは順番にチェックされるため、
このようなルールは最後に配置するフォールバックとしてのみ有用です。

以下の設定はサブタグで構成できます (これらのサブタグは、`handler` を除きすべて任意です) :

| Sub-tags             | Definition                                                                                                                                                                             |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | リクエスト URL のパスに一致させるために使用します。照合時には query string は無視されます                                                                                                                                 |
| `url_prefix`         | リクエスト URL のパスをベースパスと照合するために使用します。パス自体、またはその配下の任意のパスをパスセグメント境界で一致させます (例: &#39;/api/v1&#39; は /api/v1、/api/v1/、/api/v1/write には一致しますが、/api/v1beta には一致しません) 。照合時には query string は無視されます |
| `url_regexp`         | リクエスト URL のパスを正規表現と照合するために使用します。照合時には query string は無視されます                                                                                                                             |
| `full_url`           | 完全なリクエスト URL `scheme://host:port/path` に一致させるために使用します。照合時には query string は無視され、host には `Host` header ではなく接続先の IP アドレスが使用されます                                                           |
| `full_url_prefix`    | 完全なリクエスト URL `scheme://host:port/path` を、base URL `scheme://host:port/base_path` とパスセグメント境界で照合するために使用します (`url_prefix` を参照) 。照合時には query string は無視されます                                |
| `full_url_regexp`    | 完全なリクエスト URL `scheme://host:port/path` を正規表現と照合するために使用します。照合時には query string は無視されます                                                                                                   |
| `methods`            | HTTP メソッドに一致させるために使用します。複数のメソッドを一致対象にするにはカンマで区切ります                                                                                                                                     |
| `headers`            | リクエストヘッダーに一致させるために使用します。各子要素を照合します (子要素名はヘッダー名です)                                                                                                                                      |
| `headers_regexp`     | `headers` と同様ですが、各子要素の値を正規表現で照合します                                                                                                                                                     |
| `empty_query_string` | URL に query string が存在しないことを確認します                                                                                                                                                      |
| `handler`            | リクエストハンドラー (必須)                                                                                                                                                                        |

:::note
`url_regexp`、`full_url_regexp`、`headers_regexp` の代わりに、`url`、`full_url`、`headers` 内で `regex:` プレフィックスを使って正規表現を記述することもできます (例: `<url>regex:/api/.*</url>`) 。これは 後方互換性 のため現在もサポートされていますが、廃止された方法です。専用の `url_regexp`、`full_url_regexp`、`headers_regexp` サブタグを使用することを推奨します。
:::

`handler` には次の設定があり、サブタグで構成できます:

| Sub-tags           | Definition                                                                                                                                            |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | リダイレクト先の location                                                                                                                                     |
| `type`             | サポートされる type: static, dynamic&#95;query&#95;handler, predefined&#95;query&#95;handler, redirect                                                       |
| `status`           | static type で使用します。レスポンスのステータスコード                                                                                                                     |
| `query_param_name` | dynamic&#95;query&#95;handler type で使用します。HTTPリクエスト params 内の `<query_param_name>` に対応する値を抽出して実行します                                                   |
| `query`            | predefined&#95;query&#95;handler type で使用します。ハンドラーが呼び出されたときにクエリを実行します                                                                                 |
| `content_type`     | static type で使用します。レスポンスの content-type                                                                                                                |
| `response_content` | static type で使用します。クライアントに送信される Response の内容です。プレフィックス &#39;file://&#39; または &#39;config://&#39; を使用する場合は、ファイルまたは configuration から内容を取得してクライアントに送信します |

ルールの一覧に加えて、すべてのデフォルトハンドラーを有効にする `<defaults/>` を指定できます。

例:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

<div id="http_server_default_response">
  ## http_server_default_response
</div>

ClickHouse HTTP(s) サーバーにアクセスしたときにデフォルトで表示されるページです。
デフォルト値は &quot;Ok.&quot; です (末尾に改行が付きます) 。

**例**

`http://localhost: http_port` にアクセスすると、`https://tabix.io/` が開きます。

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

`OPTIONS` HTTPリクエストのレスポンスにヘッダーを追加するために使用されます。
`OPTIONS` メソッドは、CORSプリフライトリクエストを行う際に使用されます。

詳細については、[OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS) を参照してください。

例:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

<div id="hsts_max_age">
  ## hsts_max_age
</div>

HSTS の有効期間 (秒) 。

:::note
値を `0` にすると、ClickHouse は HSTS を無効にします。正の数を設定すると HSTS が有効になり、max-age には設定した値が使用されます。
:::

**例**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

ClickHouse サーバー間でデータを交換できるホストを制限します。
Keeper を使用している場合、この制限は異なる Keeper インスタンス間の通信にも適用されます。

:::note
デフォルトでは、この値は [`listen_host`](#listen_host) 設定と同じです。
:::

**例**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

タイプ:

デフォルト:

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

[レプリケーション](../../engines/table-engines/mergetree-family/replication.md)中に他のサーバーへ接続するために使用するユーザー名とパスワードです。さらに、サーバーはこれらの認証情報を使って他のレプリカを認証します。
したがって、`interserver_http_credentials` はクラスター内のすべてのレプリカで同一である必要があります。

:::note

* デフォルトでは、`interserver_http_credentials` セクションを省略すると、レプリケーション時に認証は使用されません。
* `interserver_http_credentials` の設定は、ClickHouse client の認証情報[設定](../../interfaces/client.md#configuration_files)とは関係ありません。
* これらの認証情報は、`HTTP` および `HTTPS` によるレプリケーションで共通です。
  :::

以下の設定はサブタグで構成できます。

* `user` — ユーザー名。
* `password` — パスワード。
* `allow_empty` — `true` の場合、認証情報が設定されていても、他のレプリカは認証なしで接続できます。`false` の場合、認証なしの接続は拒否されます。デフォルト: `false`。
* `old` — 認証情報のローテーション時に使用する古い `user` と `password` を含みます。複数の `old` セクションを指定できます。

**認証情報のローテーション**

ClickHouse は、設定更新のためにすべてのレプリカを同時に停止することなく、動的なサーバー間認証情報ローテーションをサポートしています。認証情報は複数の手順で変更できます。

認証を有効にするには、`interserver_http_credentials.allow_empty` を `true` に設定し、認証情報を追加します。これにより、認証ありと認証なしの両方の接続が可能になります。

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

すべてのレプリカの設定が完了したら、`allow_empty` を `false` に設定するか、この設定自体を削除してください。これにより、新しい認証情報での認証が必須になります。

既存の認証情報を変更するには、ユーザー名とパスワードを `interserver_http_credentials.old` セクションに移動し、`user` と `password` を新しい値に更新してください。この時点で、サーバーは新しい認証情報を使ってほかのレプリカに接続し、新旧どちらの認証情報による接続も受け付けます。

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

すべてのレプリカに新しい認証情報が適用された後は、古い認証情報を削除できます。

<div id="ldap_servers">
  ## ldap_servers
</div>

ここでは、LDAP サーバーとその接続パラメータを次の用途のために一覧します。

* `'password'` の代わりに `'ldap'` 認証メカニズムが指定された専用ローカルユーザーの認証に使用する
* リモートのユーザーディレクトリとして使用する

以下の設定はサブタグで構成できます。

| Setting                        | Description                                                                                                                                                                                                                                               |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | バインドに使用する DN を構築するためのテンプレートです。生成される DN は、認証の試行ごとに、テンプレート内のすべての `\{user_name\}` 部分文字列を実際のユーザー名に置き換えて構築されます。                                                                                                                                                |
| `enable_tls`                   | LDAP サーバーへのセキュアな接続を使用するかどうかを指定するフラグです。プレーンテキストの (`ldap://`) プロトコルには `no` を指定します (非推奨) 。SSL/TLS 上の LDAP (`ldaps://`) プロトコルには `yes` を指定します (推奨、既定値) 。従来の StartTLS プロトコルには `starttls` を指定します (プレーンテキスト (`ldap://`) プロトコルを TLS にアップグレードします) 。                 |
| `host`                         | LDAP サーバーのホスト名または IP です。このパラメータは必須で、空にできません。                                                                                                                                                                                                              |
| `port`                         | LDAP サーバーのポートです。`enable_tls` が true に設定されている場合の既定値は 636、それ以外は `389` です。                                                                                                                                                                                   |
| `tls_ca_cert_dir`              | CA 証明書を含むディレクトリへのパス。                                                                                                                                                                                                                                      |
| `tls_ca_cert_file`             | CA 証明書ファイルへのパス。                                                                                                                                                                                                                                           |
| `tls_cert_file`                | 証明書ファイルへのパス。                                                                                                                                                                                                                                              |
| `tls_cipher_suite`             | 使用を許可する暗号スイート (OpenSSL 表記) 。                                                                                                                                                                                                                              |
| `tls_key_file`                 | 証明書の秘密鍵ファイルへのパス。                                                                                                                                                                                                                                          |
| `tls_minimum_protocol_version` | SSL/TLS の最小プロトコルバージョンです。指定できる値は `ssl2`、`ssl3`、`tls1.0`、`tls1.1`、`tls1.2` (既定値) です。                                                                                                                                                                        |
| `tls_require_cert`             | SSL/TLS ピアの証明書検証の動作です。指定できる値は `never`、`allow`、`try`、`demand` (既定値) です。                                                                                                                                                                                    |
| `user_dn_detection`            | バインドされたユーザーの実際のユーザー DN を検出するための LDAP 検索パラメータを含むセクションです。これは主に、サーバーが Active Directory の場合に、追加のロールマッピングのための検索フィルタで使用されます。生成されたユーザー DN は、使用可能な箇所で `\{user_dn\}` 部分文字列を置き換える際に使用されます。既定では user DN は bind DN と同じ値に設定されますが、検索が実行されると、実際に検出された user DN の値に更新されます。 |
| `verification_cooldown`        | バインドの試行が成功した後、LDAP サーバーに問い合わせることなく、以降の連続するすべてのリクエストでユーザーが正常に認証済みであるとみなされる秒単位の期間です。キャッシュを無効にし、認証リクエストごとに LDAP サーバーへ問い合わせるようにするには `0` (既定値) を指定します。                                                                                                         |

`user_dn_detection` は次のサブタグで構成できます。

| Setting         | Description                                                                                                                                                                                |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `base_dn`       | LDAP 検索のベース DN を構築するためのテンプレートです。生成される DN は、LDAP 検索中に、テンプレート内のすべての `\{user_name\}` および `\{bind_dn\}` 部分文字列を実際のユーザー名と bind DN に置き換えて構築されます。                                                  |
| `scope`         | LDAP 検索のスコープです。指定できる値は `base`、`one_level`、`children`、`subtree` (既定値) です。                                                                                                                   |
| `search_filter` | LDAP 検索の検索フィルタを構築するためのテンプレートです。生成されるフィルタは、LDAP 検索中に、テンプレート内のすべての `\{user_name\}`、`\{bind_dn\}`、`\{base_dn\}` 部分文字列を実際のユーザー名、bind DN、base DN に置き換えて構築されます。なお、特殊文字は XML 内で適切にエスケープする必要があります。 |

例:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

例 (後続のロールマッピングのためにユーザー DN の検出を設定した、一般的な Active Directory の構成) :

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

<div id="listen_host">
  ## listen_host
</div>

リクエストの送信元として許可するホストの制限です。サーバーがそれらすべてに応答するようにするには、`::` を指定します。

例:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## ロガー
</div>

ログメッセージの出力先とフォーマット。

**キー**:

| Key                          | Description                                                                                                                                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | `true` (デフォルト) の場合、ログは非同期で記録されます (出力チャネルごとにバックグラウンドスレッド 1 つ) 。それ以外の場合は、`LOG` を呼び出したスレッド内でログが記録されます                                                                                  |
| `async_queue_max_size`       | 非同期ログ使用時に、flush を待つ間キューに保持されるメッセージの最大数です。超過したメッセージは破棄されます                                                                                                                           |
| `console`                    | コンソールへのログ出力を有効にします。有効にするには `1` または `true` を設定します。ClickHouse がデーモンモードで実行されていない場合のデフォルトは `1`、それ以外は `0` です。                                                                            |
| `console_log_level`          | コンソール出力のログレベルです。デフォルトは `level` です。                                                                                                                                                  |
| `console_shutdown_log_level` | Shutdown level は、server の Shutdown 時にコンソールのログレベルを設定するために使用されます。                                                                                                                     |
| `console_startup_log_level`  | Startup level は、server の起動時にコンソールのログレベルを設定するために使用されます。起動後、ログレベルは `console_log_level` 設定に戻ります                                                                                        |
| `count`                      | Rotation policy: ClickHouse が保持する履歴ログファイルの最大数です。                                                                                                                                    |
| `errorlog`                   | error log file のパスです。                                                                                                                                                               |
| `formatting.type`            | コンソール出力のログフォーマットです。現在は `json` のみサポートされています                                                                                                                                          |
| `level`                      | ログレベルです。有効な値: `none` (ログを無効化) 、`fatal`、`critical`、`error`、`warning`、`notice`、`information`,`debug`、`trace`、`test`                                                                   |
| `log`                        | log file のパスです。                                                                                                                                                                     |
| `rotation`                   | Rotation policy: ログファイルをローテーションするタイミングを制御します。ローテーションはサイズ、時間、またはその両方の組み合わせに基づいて行えます。例: 100M、daily、100M,daily。ログファイルが指定したサイズを超えるか、指定した時間間隔に達すると、ファイルはリネームされてアーカイブされ、新しいログファイルが作成されます。 |
| `shutdown_level`             | Shutdown level は、server の Shutdown 時にルートロガーのログレベルを設定するために使用されます。                                                                                                                    |
| `size`                       | Rotation policy: ログファイルの最大サイズ (バイト単位) です。ログファイルのサイズがこのしきい値を超えると、ファイルはリネームされてアーカイブされ、新しいログファイルが作成されます。                                                                               |
| `startup_level`              | Startup level は、server の起動時にルートロガーのログレベルを設定するために使用されます。起動後、ログレベルは `level` 設定に戻ります                                                                                                   |
| `stream_compress`            | LZ4 を使用してログメッセージを圧縮します。有効にするには `1` または `true` を設定します。                                                                                                                               |
| `syslog_level`               | syslog へ出力する際のログレベルです。                                                                                                                                                              |
| `use_syslog`                 | ログ出力を syslog にも転送します。                                                                                                                                                               |

**ログフォーマット指定子**

`log` および `errorLog` パス内のファイル名では、生成されるファイル名に対して以下のフォーマット指定子を使用できます (ディレクトリ部分では使用できません) 。

「Example」カラムには、`2023-07-06 18:32:07` 時点での出力が示されています。

| 指定子  | 説明                                                                                                                | 例                          |
| ---- | ----------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%` | 文字どおりの %                                                                                                          | `%`                        |
| `%n` | 改行文字                                                                                                              |                            |
| `%t` | 水平タブ文字                                                                                                            |                            |
| `%Y` | 年を10進数で表したもの (例: 2017)                                                                                            | `2023`                     |
| `%y` | 年の下2桁を10進数で表したもの (範囲 [00,99])                                                                                     | `23`                       |
| `%C` | 年の上2桁を10進数で表したもの (範囲 [00,99])                                                                                     | `20`                       |
| `%G` | 4 桁の[ISO 8601 週ベース年](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)。つまり、指定した週を含む年です。通常は `%V` と組み合わせた場合にのみ有用です | `2023`                     |
| `%g` | [ISO 8601 週ベース年](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)の下2桁。つまり、指定した週を含む年です。                          | `23`                       |
| `%b` | 省略形の月名 (例: Oct)  (ロケール依存)                                                                                         | `Jul`                      |
| `%h` | %b の同義語                                                                                                           | `Jul`                      |
| `%B` | 完全な月名 (例: October)  (ロケール依存)                                                                                      | `July`                     |
| `%m` | 月を10進数で表したもの (範囲 [01,12])                                                                                         | `07`                       |
| `%U` | 年内の週を10進数で表したもの (日曜日を週の最初の日とする)  (範囲 [00,53])                                                                     | `27`                       |
| `%W` | 年内の週を10進数で表したもの (月曜日を週の最初の日とする)  (範囲 [00,53])                                                                     | `27`                       |
| `%V` | ISO 8601 の週番号 (範囲 [01,53])                                                                                        | `27`                       |
| `%j` | 年内の通算日を10進数で表したもの (範囲 [001,366])                                                                                  | `187`                      |
| `%d` | 日をゼロ埋めした10進数で表したもの (範囲 [01,31]) 。1 桁の場合は先頭に 0 が付きます。                                                              | `06`                       |
| `%e` | 日を空白埋めした10進数で表したもの (範囲 [1,31]) 。1 桁の場合は先頭に空白が付きます。                                                                | `&nbsp; 6`                 |
| `%a` | 省略形の曜日名 (例: Fri)  (ロケール依存)                                                                                        | `Thu`                      |
| `%A` | 完全な曜日名 (例: Friday)  (ロケール依存)                                                                                      | `Thursday`                 |
| `%w` | 曜日を整数で表したもの。日曜日を 0 とします (範囲 [0-6])                                                                                | `4`                        |
| `%u` | 曜日を10進数で表したもの。月曜日を 1 とします (ISO 8601 形式)  (範囲 [1-7])                                                               | `4`                        |
| `%H` | 時を10進数で表したもの (24 時間制)  (範囲 [00-23])                                                                               | `18`                       |
| `%I` | 時を10進数で表したもの (12 時間制)  (範囲 [01,12])                                                                               | `06`                       |
| `%M` | 分を10進数で表したもの (範囲 [00,59])                                                                                         | `32`                       |
| `%S` | 秒を10進数で表したもの (範囲 [00,60])                                                                                         | `07`                       |
| `%c` | 標準的な日付と時刻の文字列 (例: Sun Oct 17 04:41:13 2010)  (ロケール依存)                                                             | `Thu Jul  6 18:32:07 2023` |
| `%x` | ローカライズされた日付表現 (ロケール依存)                                                                                            | `07/06/23`                 |
| `%X` | ローカライズされた時刻表現 (例: 18:40:20 または 6:40:20 PM)  (ロケール依存)                                                              | `18:32:07`                 |
| `%D` | 短い MM/DD/YY 形式の日付。%m/%d/%y と同等です                                                                                  | `07/06/23`                 |
| `%F` | 短い形式の YYYY-MM-DD 日付。%Y-%m-%d と同等                                                                                  | `2023-07-06`               |
| `%r` | ローカライズされた 12 時間制の時刻 (ロケール依存)                                                                                      | `06:32:07 PM`              |
| `%R` | &quot;%H:%M&quot; と同等                                                                                             | `18:32`                    |
| `%T` | &quot;%H:%M:%S&quot; と同等 (ISO 8601 の時刻フォーマット)                                                                     | `18:32:07`                 |
| `%p` | ローカライズされた午前/午後の指定 (ロケール依存)                                                                                        | `PM`                       |
| `%z` | ISO 8601 フォーマットでの UTC からのオフセット (例: -0430) 。タイムゾーン情報が利用できない場合は文字を出力しません                                            | `+0800`                    |
| `%Z` | ロケール依存のタイムゾーン名または略称。タイムゾーン情報が利用できない場合は文字を出力しません                                                                   | `Z AWST `                  |

**例**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

コンソールのみにログメッセージを出力するには：

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**レベル別のオーバーライド**

個々のログ名ごとにログレベルをオーバーライドできます。たとえば、ロガー &quot;Backup&quot; と &quot;RBAC&quot; のすべてのメッセージを抑止する場合です。

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

ログメッセージを syslog にも書き込むには:

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

`<syslog>` のキー:

| Key        | Description                                                                                                                                                                                                                |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | `host\[:port\]` 形式の syslog アドレスです。省略した場合は、ローカルデーモンが使用されます。                                                                                                                                                                 |
| `hostname` | ログの送信元ホスト名です (省略可能) 。                                                                                                                                                                                                      |
| `facility` | syslog の [facility キーワード](https://en.wikipedia.org/wiki/Syslog#Facility) です。`LOG_USER`、`LOG_DAEMON`、`LOG_LOCAL3` などのように、`LOG_` プレフィックス付きの大文字で指定する必要があります。デフォルトは、`address` が指定されている場合は `LOG_USER`、それ以外の場合は `LOG_DAEMON` です。 |
| `format`   | ログメッセージのフォーマットです。設定可能な値: `bsd` と `syslog.`                                                                                                                                                                                 |

**ログフォーマット**

コンソールログに出力するログフォーマットを指定できます。現在サポートされているのは JSON のみです。

**例**

以下は、出力される JSON ログの例です:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

JSON 形式のログ出力を有効にするには、次のスニペットを使用します。

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**JSONログのキー名を変更する**

キー名は、`<names>` タグ内のタグの値を変更することで変更できます。たとえば、`DATE_TIME` を `MY_DATE_TIME` に変更するには、`<date_time>MY_DATE_TIME</date_time>` を使用します。

**JSONログのキーを省略する**

ログのプロパティは、そのプロパティをコメントアウトすることで省略できます。たとえば、ログに `query_id` を出力したくない場合は、`<query_id>` タグをコメントアウトします。

<div id="send_crash_reports">
  ## send_crash_reports
</div>

ClickHouse コア開発チームにクラッシュレポートを送信するための設定です。

これを有効にしていただけると、特にプレプロダクション環境では大変助かります。

キー:

| Key                   | Description                                                                                             |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| `enabled`             | この機能を有効にするブール値フラグです。デフォルトは `true` です。クラッシュレポートを送信しないようにするには、`false` に設定します。                             |
| `endpoint`            | クラッシュレポートの送信先となるエンドポイント URL を上書きできます。                                                                   |
| `send_logical_errors` | `LOGICAL_ERROR` は `assert` のようなもので、ClickHouse のバグを示します。このブール値フラグを有効にすると、これらの例外も送信されます (デフォルト: `true`) 。 |

**推奨される使用方法**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

ホスト鍵の公開部分は、最初の接続時に SSH クライアント側の known&#95;hosts ファイルに書き込まれます。

Host Key Configurations はデフォルトで無効になっています。
Host Key Configurations を有効にするには、該当する設定のコメントアウトを外し、それぞれの SSH鍵へのパスを指定します。

Example:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

ユーザーが PTY 上の埋め込みクライアントを使用して対話的に接続し、クエリを実行できるようにする SSH サーバーのポートです。

例:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## ストレージ構成
</div>

ストレージを複数のディスクで構成できます。

ストレージ構成は次の構造に従います。

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

<div id="configuration-of-disks">
  ### `disks` の設定
</div>

`disks` の設定は、以下の構造に従います。

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

上記のサブタグは、`disks` に対して以下の設定を定義します。

| Setting                 | Description                                                      |
| ----------------------- | ---------------------------------------------------------------- |
| `<disk_name_N>`         | ディスク名です。一意である必要があります。                                            |
| `path`                  | サーバーデータを保存するパスです (`data` および `shadow` カタログ) 。末尾は `/` で終わる必要があります |
| `keep_free_space_bytes` | ディスク上に確保しておく空き領域のサイズです。                                          |

:::note
ディスクの順序は関係ありません。
:::

<div id="configuration-of-policies">
  ### ポリシーの設定
</div>

上記のサブタグは、`policies` に対して次の設定を定義します。

| Setting                      | Description                                                                                                                                                                                                                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | ポリシー名です。ポリシー名は一意である必要があります。                                                                                                                                                                                                                                                                         |
| `volume_name_N`              | ボリューム名です。ボリューム名は一意である必要があります。                                                                                                                                                                                                                                                                       |
| `disk`                       | ボリューム内にあるディスクです。                                                                                                                                                                                                                                                                                    |
| `max_data_part_size_bytes`   | このボリューム内のいずれかのディスクに格納できるデータ `chunk` の最大サイズです。マージの結果、`chunk` のサイズが `max_data_part_size_bytes` を超えると見込まれる場合、その `chunk` は次のボリュームに書き込まれます。つまり、この機能を使うと、新しい / 小さい `chunk` はホットな (SSD) ボリュームに保存し、大きくなったらコールドな (HDD) ボリュームへ移動できます。ポリシーにボリュームが 1 つしかない場合は、このオプションを使用しないでください。                               |
| `move_factor`                | ボリューム上の利用可能な空き容量の割合です。空き容量がこの値を下回ると、次のボリュームがある場合、データはそのボリュームへの移動を開始します。移動時には、`chunk` はサイズの大きい順 (降順) にソートされ、合計サイズが `move_factor` の条件を満たすのに十分な `chunk` が選択されます。すべての `chunk` の合計サイズでも不足する場合は、すべての `chunk` が移動されます。                                                                                     |
| `perform_ttl_move_on_insert` | INSERT 時に、有効期限 (TTL) が切れたデータの移動を無効にします。デフォルトでは (有効な場合)、TTL の move ルールに従ってすでに期限切れになっているデータ片を insert すると、そのデータは move ルールで指定されたボリューム / ディスクに直ちに移動されます。対象のボリューム / ディスクが低速な場合 (例: S3)、これにより insert が大幅に遅くなる可能性があります。無効にした場合、期限切れのデータ部分はいったんデフォルトのボリュームに書き込まれ、その後すぐに、期限切れの TTL に対するルールで指定されたボリュームに移動されます。 |
| `load_balancing`             | ディスクの負荷分散ポリシーです。`round_robin` または `least_used` を指定します。                                                                                                                                                                                                                                              |
| `least_used_ttl_ms`          | すべてのディスクの利用可能容量を更新するためのタイムアウト (ミリ秒単位) を設定します (`0` - 常に更新、`-1` - 更新しない、デフォルト値は `60000`)。なお、ディスクが ClickHouse のみで使用され、実行中にファイルシステムのリサイズが行われない場合は、`-1` を使用できます。それ以外の場合は、最終的に容量割り当てが不正確になるため、推奨されません。                                                                                                    |
| `prefer_not_to_merge`        | このボリューム上でのデータ パーツのマージを無効にします。注: これは悪影響を及ぼす可能性があり、速度低下の原因になりえます。この設定を有効にすると (しないでください)、このボリューム上でのデータのマージが禁止されます (望ましくありません)。これにより、ClickHouse が低速なディスクをどのように扱うかを制御できます。これを使用しないことを推奨します。                                                                                                              |
| `volume_priority`            | ボリュームが使用される優先順位 (順序) を定義します。値が小さいほど優先度は高くなります。パラメーター値は自然数である必要があり、1 から N まで (N は指定された最大のパラメーター値) をギャップなくすべて含んでいる必要があります。                                                                                                                                                                            |

`volume_priority` について:

* すべてのボリュームにこのパラメーターがある場合、指定された順序で優先順位が付けられます。
* *一部の* ボリュームにのみこのパラメーターがある場合、これを持たないボリュームの優先度は最も低くなります。これを持つボリュームはタグの値に従って優先順位が付けられ、残りのボリューム同士の優先度は、設定ファイル内の記述順で決まります。
* *どの* ボリュームにもこのパラメーターが指定されていない場合、それらの順序は設定ファイル内の記述順で決まります。
* ボリュームの優先度が同じであってはなりません。

<div id="macros">
  ## macros
</div>

レプリケートテーブル向けのパラメータ置換です。

レプリケートテーブルを使用しない場合は省略できます。

詳細については、[レプリケートテーブルの作成](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables)の節を参照してください。

**例**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Replicated データベースのレプリカグループ名です。

Replicated データベースによって作成されるクラスターは、同じグループ内のレプリカで構成されます。
DDL クエリが待機するのは、同じグループ内のレプリカのみです。

デフォルトでは空です。

**例**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

セッションの最大タイムアウト時間 (Seconds) 。

例:

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルのチューニングに関する設定です。

詳細については、MergeTreeSettings.h ヘッダーファイルを参照してください。

**例**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

デフォルトでは無効です。

**有効化**

メトリクス履歴の収集 [`system.metric_log`](../../operations/system-tables/metric_log.md) を手動で有効にするには、以下の内容で `/etc/clickhouse-server/config.d/metric_log.xml` を作成します。

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**無効化**

`metric_log` 設定を無効にするには、以下の内容でファイル `/etc/clickhouse-server/config.d/disable_metric_log.xml` を作成します。

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

[ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md) のテーブル向けの詳細設定です。この設定はより優先されます。

詳細については、MergeTreeSettings.h ヘッダーファイルを参照してください。

**例**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

[`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md) システムテーブルの設定です。

<SystemLogParameters />

例:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

<div id="openSSL">
  ## openSSL
</div>

SSL クライアント/サーバー設定。

SSL のサポートは `libpoco` ライブラリによって提供されています。使用可能な設定オプションについては [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h) で説明されています。デフォルト値は [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp) で確認できます。

サーバー/クライアント設定のキー:

| オプション                         | 説明                                                                                                                                                                                                                                                                                                                                                   | デフォルト値                                                                                     |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | セッションキャッシュを有効または無効にします。`sessionIdContext` と組み合わせて使用する必要があります。有効な値: `true`, `false`。                                                                                                                                                                                                                                                                  | `false`                                                                                    |
| `caConfig`                    | 信頼された CA 証明書を含むファイルまたはディレクトリへのパスです。これがファイルを指す場合は、PEM フォーマットである必要があり、複数の CA 証明書を含めることができます。これがディレクトリを指す場合は、CA 証明書ごとに 1 つの `.pem` ファイルが含まれている必要があります。ファイル名は、CA の subject name のハッシュ値を基に検索されます。詳細については、[SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html) の man ページを参照してください。 |                                                                                            |
| `certificateFile`             | PEM形式のクライアント/サーバー証明書ファイルのパス。`privateKeyFile` に証明書が含まれている場合は省略できます。                                                                                                                                                                                                                                                                                   |                                                                                            |
| `cipherList`                  | OpenSSLでサポートされる暗号方式。                                                                                                                                                                                                                                                                                                                                 | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | 使用を許可しないプロトコル。                                                                                                                                                                                                                                                                                                                                       |                                                                                            |
| `extendedVerification`        | 有効にすると、証明書の CN または SAN がピアのホスト名と一致するかどうかを検証します。                                                                                                                                                                                                                                                                                                      | `false`                                                                                    |
| `fips`                        | OpenSSL の FIPS モードを有効にします。ライブラリの OpenSSL バージョンが FIPS をサポートしている場合に利用できます。                                                                                                                                                                                                                                                                             | `false`                                                                                    |
| `invalidCertificateHandler`   | 無効な証明書を検証するためのクラス (CertificateHandler のサブクラス) 。たとえば：`<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>`。                                                                                                                                                                                                | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | OpenSSL の組み込み CA 証明書を使用するかどうか。ClickHouse は、組み込みの CA 証明書がファイル `/etc/ssl/cert.pem` (またはディレクトリ `/etc/ssl/certs`) 、あるいは環境変数 `SSL_CERT_FILE` (または `SSL_CERT_DIR`) で指定されたファイル (またはディレクトリ) にあるものと想定します。                                                                                                                                                     | `true`                                                                                     |
| `preferServerCiphers`         | クライアント優先のサーバー暗号スイート。                                                                                                                                                                                                                                                                                                                                 | `false`                                                                                    |
| `privateKeyFile`              | PEM証明書の秘密鍵を含むファイルへのパス。ファイルには秘密鍵と証明書を同時に含めることもできます。                                                                                                                                                                                                                                                                                                   |                                                                                            |
| `privateKeyPassphraseHandler` | 秘密鍵にアクセスするためのパスフレーズを要求するクラス (PrivateKeyPassphraseHandler のサブクラス) 。例: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`。                                                                                                                               | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | TLSv1 接続を必須とします。有効な値: `true`, `false`。                                                                                                                                                                                                                                                                                                               | `false`                                                                                    |
| `requireTLSv1_1`              | TLSv1.1接続を必須にします。有効な値: `true`, `false`。                                                                                                                                                                                                                                                                                                              | `false`                                                                                    |
| `requireTLSv1_2`              | TLSv1.2 接続を必須にします。有効な値: `true`, `false`。                                                                                                                                                                                                                                                                                                             | `false`                                                                                    |
| `sessionCacheSize`            | サーバーがキャッシュするセッションの最大数です。値が `0` の場合、セッション数は無制限です。                                                                                                                                                                                                                                                                                                     | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | サーバーが生成する各識別子に付加する、一意のランダムな文字列セットです。文字列の長さは `SSL_MAX_SSL_SESSION_ID_LENGTH` を超えてはなりません。サーバーがセッションをキャッシュする場合にも、client がキャッシュを要求した場合にも問題の回避に役立つため、この parameter は常に設定することが推奨されます。                                                                                                                                                                       | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | サーバーでセッションをキャッシュする時間 (時間単位) 。                                                                                                                                                                                                                                                                                                                        | `2`                                                                                        |
| `verificationDepth`           | 検証チェーンの最大長。この設定値を証明書チェーンの長さが超えると、検証は失敗します。                                                                                                                                                                                                                                                                                                           | `9`                                                                                        |
| `verificationMode`            | ノードの証明書の検証方法です。詳細は [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) クラスの説明を参照してください。設定可能な値: `none`, `relaxed`, `strict`, `once`。                                                                                                                                                           | `relaxed`                                                                                  |

**設定例:**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

<div id="part_log">
  ## part_log
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) に関連するイベントを記録するログです。たとえば、データの追加やマージなどです。このログを使うと、マージアルゴリズムをシミュレートして、その特性を比較できます。マージプロセスを可視化することもできます。

クエリは別のファイルではなく、[system.part&#95;log](/ja/operations/system-tables/part_log) テーブルに記録されます。このテーブルの名前は、`table` パラメータで設定できます (以下を参照) 。

<SystemLogParameters />

**例**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

<div id="processors_profile_log">
  ## processors_profile_log
</div>

[`processors_profile_log`](../system-tables/processors_profile_log.md) システムテーブル の設定です。

<SystemLogParameters />

デフォルト設定は次のとおりです。

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

<div id="prometheus">
  ## prometheus
</div>

[Prometheus](https://prometheus.io) によるスクレイピング用にメトリクスデータを公開します。

設定:

* `endpoint` – Prometheus サーバーがメトリクスをスクレイピングするための HTTP エンドポイントです。`'/'` で始めます。
* `port` – `endpoint` のポートです。
* `metrics` – [system.metrics](/ja/operations/system-tables/metrics) テーブルのメトリクスを公開します。
* `events` – [system.events](/ja/operations/system-tables/events) テーブルのメトリクスを公開します。
* `asynchronous_metrics` – [system.asynchronous&#95;metrics](/ja/operations/system-tables/asynchronous_metrics) テーブルの現在のメトリクス値を公開します。
* `errors` - 前回のサーバー再起動以降に発生した、error codes ごとのエラー数を公開します。この情報は [system.errors](/ja/operations/system-tables/errors) からも取得できます。

**例**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

確認してください (`127.0.0.1` を ClickHouse server の IP アドレスまたはホスト名に置き換えてください) :

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

[log&#95;queries=1](../../operations/settings/settings.md) が設定されている場合に受信したクエリのログ記録に関する設定です。

クエリは別個のファイルではなく、[system.query&#95;log](/ja/operations/system-tables/query_log) テーブルに記録されます。テーブル名は `table` パラメータで変更できます (以下を参照) 。

<SystemLogParameters />

テーブルが存在しない場合、ClickHouse が作成します。ClickHouse server の更新時に query log の構造が変更された場合は、古い構造のテーブルがリネームされ、新しいテーブルが自動的に作成されます。

**例**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

<div id="query_metric_log">
  ## query_metric_log
</div>

デフォルトでは無効になっています。

**有効化**

メトリクス履歴の収集 [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md) を手動で有効にするには、以下の内容で `/etc/clickhouse-server/config.d/query_metric_log.xml` を作成します。

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**無効化**

`query_metric_log` 設定を無効にするには、次の内容で `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` ファイルを作成します。

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

[クエリキャッシュ](../query-cache.md)の設定です。

使用可能な設定は次のとおりです。

| Setting                   | Description                                       | Default Value |
| ------------------------- | ------------------------------------------------- | ------------- |
| `max_entries`             | cache に保存される `SELECT` クエリ結果の最大数。                  | `1024`        |
| `max_entry_size_in_bytes` | cache に保存できる `SELECT` クエリ結果の最大サイズ (バイト単位) 。       | `1048576`     |
| `max_entry_size_in_rows`  | cache に保存できる `SELECT` クエリ結果の最大行数。                 | `30000000`    |
| `max_size_in_bytes`       | cache の最大サイズ (バイト単位) 。`0` はクエリキャッシュが無効であることを示します。 | `1073741824`  |

:::note

* 変更した設定は即座に反映されます。
* クエリキャッシュのデータは DRAM に割り当てられます。メモリが不足している場合は、`max_size_in_bytes` に小さい値を設定するか、クエリキャッシュを無効にしてください。
  :::

**例**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

[log&#95;query&#95;threads=1](/ja/operations/settings/settings#log_query_threads) 設定で受信したクエリのスレッドをログに記録するための設定です。

クエリは別個のファイルではなく、[system.query&#95;thread&#95;log](/ja/operations/system-tables/query_thread_log) テーブルに記録されます。テーブル名は `table` パラメータで変更できます (以下を参照) 。

<SystemLogParameters />

テーブルが存在しない場合、ClickHouse が作成します。ClickHouse server の更新時にクエリスレッドログの構造が変更された場合は、古い構造のテーブルはリネームされ、新しいテーブルが自動的に自動的に作成されます。

**例**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

<div id="query_views_log">
  ## query_views_log
</div>

[log&#95;query&#95;views=1](/ja/operations/settings/settings#log_query_views) 設定で受信したクエリに応じて、ビュー (ライブ、マテリアライズドなど) をログに記録するための設定です。

クエリは個別のファイルではなく、[system.query&#95;views&#95;log](/ja/operations/system-tables/query_views_log) テーブルに記録されます。テーブル名は `table` パラメーターで変更できます (以下を参照) 。

<SystemLogParameters />

テーブルが存在しない場合、ClickHouse が作成します。ClickHouse server の更新時にクエリビューログの構造が変更された場合は、古い構造を持つテーブルはリネームされ、新しいテーブルが自動的に作成されます。

**Example**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

<div id="text_log">
  ## text_log
</div>

テキストメッセージを記録する [text&#95;log](/ja/operations/system-tables/text_log) システムテーブルの設定です。

<SystemLogParameters />

さらに:

| 設定      | 説明                                        | デフォルト値  |
| ------- | ----------------------------------------- | ------- |
| `level` | テーブルに保存される最大メッセージレベルです (デフォルトは `Trace`) 。 | `Trace` |

**例**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

<div id="trace_log">
  ## trace_log
</div>

[trace&#95;log](/ja/operations/system-tables/trace_log) システムテーブルの操作に関する設定です。

<SystemLogParameters />

デフォルトのサーバー設定ファイル `config.xml` には、次の設定セクションがあります。

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

非同期 INSERT を記録する [asynchronous&#95;insert&#95;log](/ja/operations/system-tables/asynchronous_insert_log) システムテーブルの設定です。

<SystemLogParameters />

**例**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

<div id="crash_log">
  ## crash_log
</div>

[crash&#95;log](../../operations/system-tables/crash_log.md) システムテーブル の設定です。

以下の設定はサブタグで指定できます。

| Setting                            | Description                                                                                                                | Default             | Note                                                                                 |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------ |
| `buffer_size_rows_flush_threshold` | 行数のしきい値です。しきい値に達すると、ログをディスクにフラッシュする処理がバックグラウンドで開始されます。                                                                     | `max_size_rows / 2` |                                                                                      |
| `database`                         | データベース名です。                                                                                                                 |                     |                                                                                      |
| `engine`                           | システムテーブル用の[MergeTree エンジン定義](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table)です。 |                     | `partition_by` または `order_by` が定義されている場合は使用できません。指定しない場合は、デフォルトで `MergeTree` が選択されます |
| `flush_interval_milliseconds`      | メモリ上のバッファからテーブルへデータをフラッシュする間隔です。                                                                                           | `7500`              |                                                                                      |
| `flush_on_crash`                   | クラッシュ時にログをディスクへ書き出すかどうかを設定します。                                                                                             | `false`             |                                                                                      |
| `max_size_rows`                    | ログの最大行数です。未フラッシュのログ数が `max_size_rows` に達すると、ログがディスクに書き出されます。                                                               | `1024`              |                                                                                      |
| `order_by`                         | システムテーブル用の[カスタムソートキー](/ja/engines/table-engines/mergetree-family/mergetree#order_by)です。`engine` が定義されている場合は使用できません。           |                     | システムテーブルに `engine` を指定する場合、`order_by` パラメータは `engine` 内で直接指定する必要があります                |
| `partition_by`                     | システムテーブル用の[カスタムパーティションキー](/ja/engines/table-engines/mergetree-family/custom-partitioning-key.md)です。                           |                     | システムテーブルに `engine` を指定する場合、`partition_by` パラメータは `engine` 内で直接指定する必要があります            |
| `reserved_size_rows`               | ログ用に事前確保されるメモリサイズ (行数) です。                                                                                                 | `1024`              |                                                                                      |
| `settings`                         | MergeTree の動作を制御する[追加パラメータ](/ja/engines/table-engines/mergetree-family/mergetree/#settings)です (省略可能) 。                        |                     | システムテーブルに `engine` を指定する場合、`settings` パラメータは `engine` 内で直接指定する必要があります                |
| `storage_policy`                   | テーブルに使用するストレージポリシー名です (省略可能) 。                                                                                             |                     | システムテーブルに `engine` を指定する場合、`storage_policy` パラメータは `engine` 内で直接指定する必要があります          |
| `table`                            | システムテーブル名です。                                                                                                               |                     |                                                                                      |
| `ttl`                              | テーブルの[有効期限 (TTL)](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)を指定します。                     |                     | システムテーブルに `engine` を指定する場合、`ttl` パラメータは `engine` 内で直接指定する必要があります                     |

デフォルトのサーバー設定ファイル `config.xml` には、次の設定セクションが含まれています。

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

この設定は、カスタム (SQL から作成された) cached ディスクのキャッシュパスを指定します。
`custom_cached_disks_base_directory` はカスタムディスクにおいて `filesystem_caches_path` (`filesystem_caches_path.xml` にあります) より優先され、
前者が存在しない場合は後者が使用されます。
ファイルシステムキャッシュの設定パスはそのディレクトリ内になければならず、
そうでない場合は、ディスクが作成されないよう例外がスローされます。

:::note
これは、server のアップグレード前の古いバージョンで作成されたディスクには影響しません。
この場合、server が正常に起動できるよう、例外はスローされません。
:::

例:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

`BACKUP` および `RESTORE` の操作をログに記録する [backup&#95;log](../../operations/system-tables/backup_log.md) システムテーブルの設定です。

<SystemLogParameters />

**例**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

<div id="blob_storage_log">
  ## blob_storage_log
</div>

[`blob_storage_log`](../system-tables/blob_storage_log.md) システムテーブルの設定です。

<SystemLogParameters />

例:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

正規表現ベースのルールで、クエリおよびすべてのログメッセージに対し、サーバーログ、
[`system.query_log`](/ja/operations/system-tables/query_log)、[`system.text_log`](/ja/operations/system-tables/text_log)、[`system.processes`](/ja/operations/system-tables/processes) テーブル、ならびにクライアントへ送信されるログに保存される前に適用されます。これにより、
名前、メールアドレス、個人識別子、クレジットカード番号など、SQLクエリに含まれる機密データがログへ漏えいするのを防ぐことができます。

**例**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**設定フィールド**:

| Setting   | Description                         |
| --------- | ----------------------------------- |
| `name`    | ルール名 (任意)                           |
| `regexp`  | RE2 互換の正規表現 (必須)                    |
| `replace` | 機密データ用の置換文字列 (任意。デフォルトはアスタリスク 6 個)  |

マスキングルールはクエリ全体に適用されます (不正な形式のクエリや解析できないクエリから機密データが漏えいするのを防ぐためです) 。

[`system.events`](/ja/operations/system-tables/events) テーブルには `QueryMaskingRulesMatch` というカウンターがあり、クエリマスキングルールの一致回数の総数を示します。

分散クエリでは、各サーバーを個別に設定する必要があります。そうしないと、他の
ノードに渡されたサブクエリがマスキングされないまま保存されます。

<div id="remote_servers">
  ## remote_servers
</div>

[Distributed](../../engines/table-engines/special/distributed.md) テーブルエンジンおよび `cluster` テーブル関数で使用されるクラスターの設定。

**例**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

`incl` 属性の値については、「[設定ファイル](/ja/operations/configuration-files)」のセクションを参照してください。

**関連項目**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [Cluster Discovery](../../operations/cluster-discovery.md)
* [Replicated データベースエンジン](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

URL 関連のストレージエンジンおよびテーブル関数での使用を許可するホストの一覧です。

`<host>` XMLタグを使ってホストを追加する場合:

* 名前は DNS 解決の前にチェックされるため、URL 内と完全に同じ形で指定する必要があります。例: `<host>clickhouse.com</host>`
* URL でポートが明示的に指定されている場合は、host:port 全体がまとめてチェックされます。例: `<host>clickhouse.com:80</host>`
* ホストをポートなしで指定した場合、そのホストの任意のポートが許可されます。例: `<host>clickhouse.com</host>` を指定した場合、`clickhouse.com:20` (FTP)、`clickhouse.com:80` (HTTP)、`clickhouse.com:443` (HTTPS) などが許可されます。
* ホストを IP アドレスとして指定した場合は、URL で指定されたとおりにチェックされます。例: `[2a02:6b8:a::a]`。
* リダイレクトがあり、リダイレクトのサポートが有効になっている場合は、各リダイレクト (location フィールド) がチェックされます。

例:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

サーバーのタイムゾーン。

UTC のタイムゾーンまたは地理的な場所を表す IANA 識別子として指定します (例: Africa/Abidjan) 。

タイムゾーンは、DateTime フィールドをテキストフォーマットに出力する場合 (画面またはファイルに表示する場合) や、文字列から DateTime を取得する場合に、String と DateTime の間で変換を行うために必要です。さらに、日時を扱う Functions で入力パラメータとしてタイムゾーンが渡されなかった場合にも、タイムゾーンが使用されます。

**例**

```xml
<timezone>Asia/Istanbul</timezone>
```

**関連項目**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

TCPプロトコル経由でクライアントと通信するためのポートです。

**例**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

クライアントとのセキュアな通信に使用する TCP ポートです。[OpenSSL](#openssl) の設定と併用します。

**デフォルト値**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

MySQLプロトコルでクライアントと通信するためのポートです。

:::note

* 正の整数を指定すると、そのポート番号で待ち受けます
* 空の値を指定すると、MySQLプロトコルでのクライアントとの通信は無効になります。
  :::

**例**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

PostgreSQL プロトコルでクライアントと通信するためのポートです。

:::note

* 正の整数で、待ち受けるポート番号を指定します
* 空の値を指定すると、PostgreSQL プロトコルでのクライアントとの通信は無効になります。
  :::

**例**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

短縮形またはシンボリックなURLプレフィックスを完全なURLに変換するための設定。

例:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

<div id="user_defined_path">
  ## user_defined_path
</div>

ユーザー定義ファイルを格納するディレクトリです。SQL User Defined Functions [SQL User Defined Functions](/ja/sql-reference/functions/udf) で使用されます。

**例**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

以下を含むファイルのパス:

* ユーザー設定
* アクセス権
* 設定プロファイル
* クォータ設定

**例**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

アクセス制御システムのオプションの改善に関する設定です。

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                              | Default |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `on_cluster_queries_require_cluster_grant`      | `ON CLUSTER` クエリに `CLUSTER` 権限が必要かどうかを設定します。                                                                                                                                                                                                                                                                                                             | `true`  |
| `role_cache_expiration_time_seconds`            | 最後にアクセスされてからロールをロールキャッシュに保持する秒数を設定します。                                                                                                                                                                                                                                                                                                                   | `600`   |
| `select_from_information_schema_requires_grant` | `SELECT * FROM information_schema.<table>` の実行に何らかの権限が必要かどうか、および任意のユーザーが実行できるかどうかを設定します。true に設定すると、このクエリの実行には通常のテーブルと同様に `GRANT SELECT ON information_schema.<table>` が必要になります。                                                                                                                                                                         | `true`  |
| `select_from_system_db_requires_grant`          | `SELECT * FROM system.<table>` の実行に何らかの権限が必要かどうか、および任意のユーザーが実行できるかどうかを設定します。true に設定すると、このクエリの実行には非システムテーブルと同様に `GRANT SELECT ON system.<table>` が必要になります。例外: 一部のシステムテーブル (`tables`、`columns`、`databases`、および `one`、`contributors` などの定数テーブル) は引き続き誰でもアクセスできます。また、`SHOW` 権限 (たとえば `SHOW USERS`) が付与されている場合は、対応するシステムテーブル (つまり `system.users`) にアクセスできます。 | `true`  |
| `settings_constraints_replace_previous`         | ある設定に対する settings profile 内の制約が、その設定に対する以前の制約 (他の profile で定義されたもの) の動作を、新しい制約で設定されていないフィールドも含めて打ち消すかどうかを設定します。また、`changeable_in_readonly` 制約タイプも有効にします。                                                                                                                                                                                                 | `true`  |
| `table_engines_require_grant`                   | 特定の table engine を使用してテーブルを作成する際に権限が必要かどうかを設定します。                                                                                                                                                                                                                                                                                                        | `false` |
| `throw_on_unmatched_row_policies`               | テーブルに行ポリシーが存在するものの、そのいずれも current user 向けではない場合に、そのテーブルの読み取り時に例外をスローするかどうかを設定します                                                                                                                                                                                                                                                                         | `false` |
| `users_without_row_policies_can_read_rows`      | 許可型の行ポリシーを持たないユーザーが、`SELECT` クエリで引き続き行を読み取れるかどうかを設定します。たとえば、ユーザー A と B がいて、行ポリシーが A に対してのみ定義されている場合、この設定が true ならユーザー B はすべての行を参照できます。この設定が false なら、ユーザー B には行が一切表示されません。                                                                                                                                                                               | `true`  |

例:

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

<div id="s3queue_log">
  ## s3queue_log
</div>

`s3queue_log` システムテーブルの設定です。

<SystemLogParameters />

デフォルトの設定は次のとおりです。

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

&#39;dead&#95;letter&#95;queue&#39; システムテーブルの設定です。

<SystemLogParameters />

デフォルトの設定は次のとおりです。

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

[ZooKeeper](http://zookeeper.apache.org/) クラスターと ClickHouse を連携させるための設定が含まれています。ClickHouse は、レプリケートテーブルを使用する場合、レプリカのメタデータを保存するために ZooKeeper を使用します。レプリケートテーブルを使用しない場合、このパラメータセクションは省略できます。

以下の設定はサブタグで構成できます。

| Setting                                         | Description                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`                                          | ZooKeeper のエンドポイントです。複数のエンドポイントを設定できます。例: `<node index="1"><host>example_host</host><port>2181</port></node>`。`index` 属性は、ZooKeeper クラスターへの接続を試みる際の node の順序を指定します。                                                                                                                          |
| `operation_timeout_ms`                          | 1 回の操作に対する最大タイムアウト (ミリ秒) 。                                                                                                                                                                                                                                                                   |
| `session_timeout_ms`                            | クライアントセッションの最大タイムアウト (ミリ秒) 。                                                                                                                                                                                                                                                                 |
| `root` (optional)                               | ClickHouse server が使用する znode のルートとして使われる znode。                                                                                                                                                                                                                                             |
| `fallback_session_lifetime.min` (optional)      | プライマリが利用できない場合に fallback node へ接続する zookeeper session の存続時間の最小値 (ロードバランシング用) 。秒単位で設定します。デフォルト: 3 時間。                                                                                                                                                                                         |
| `fallback_session_lifetime.max` (optional)      | プライマリが利用できない場合に fallback node へ接続する zookeeper session の存続時間の最大値 (ロードバランシング用) 。秒単位で設定します。デフォルト: 6 時間。                                                                                                                                                                                         |
| `identity` (optional)                           | 要求された znode にアクセスするために ZooKeeper が必要とするユーザー名とパスワード。                                                                                                                                                                                                                                          |
| `use_compression` (optional)                    | `true` に設定すると Keeper プロトコルで圧縮を有効にします。                                                                                                                                                                                                                                                        |
| `use_xid_64` (optional)                         | 64 ビットの transaction ID を有効にします。拡張 transaction ID フォーマットを有効にするには `true` を設定します。デフォルト: `false`。                                                                                                                                                                                                |
| `pass_opentelemetry_tracing_context` (optional) | Keeper requests への OpenTelemetry tracing context の伝播を有効にします。有効にすると、Keeper 操作に対して tracing spans が作成され、ClickHouse と Keeper をまたいだ distributed tracing が可能になります。詳細は [Tracing ClickHouse Keeper Requests](/ja/operations/opentelemetry#tracing-clickhouse-keeper-requests) を参照してください。デフォルト: `false`。 |

また、`zookeeper_load_balancing` 設定 (任意) を使用すると、ZooKeeper node を選択するアルゴリズムを指定できます。

| Algorithm Name                   | Description                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------- |
| `random`                         | ZooKeeper node の 1 つをランダムに選択します。                                                            |
| `in_order`                       | 最初の ZooKeeper node を選択し、利用できない場合は 2 番目、その次は 3 番目、という順に選択します。                                |
| `nearest_hostname`               | server の hostname に最も近い hostname を持つ ZooKeeper node を選択します。hostname は name のプレフィックスで比較されます。 |
| `hostname_levenshtein_distance`  | `nearest_hostname` と同様ですが、hostname をレーベンシュタイン距離で比較します。                                      |
| `hostname_longest_common_prefix` | `nearest_hostname` と同様ですが、server の hostname と最長共通プレフィックスを共有する hostname を持つ node を優先します。     |
| `hostname_longest_common_suffix` | `nearest_hostname` と同様ですが、server の hostname と最長共通接尾辞を共有する hostname を持つ node を優先します。         |
| `first_or_random`                | 最初の ZooKeeper node を選択し、利用できない場合は残りの ZooKeeper nodes から 1 つをランダムに選択します。                     |
| `round_robin`                    | 最初の ZooKeeper node を選択し、再接続が発生した場合は次の node を選択します。                                          |

**設定例**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**関連項目**

* [レプリケーション](../../engines/table-engines/mergetree-family/replication.md)
* [ZooKeeperプログラマーズガイド](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [ClickHouse と Zookeeper 間のオプションのセキュア通信](/ja/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

ZooKeeper におけるデータパーツヘッダーの保存方法です。この設定は [`MergeTree`](/ja/engines/table-engines/mergetree-family) ファミリーにのみ適用されます。指定方法は次のとおりです。

**`config.xml` ファイルの [merge&#95;tree](#merge_tree) セクションでグローバルに指定**

ClickHouse は、この設定をサーバー上のすべてのテーブルに適用します。この設定はいつでも変更できます。既存のテーブルも、設定が変更されると動作が変わります。

**各テーブルごとに指定**

テーブル作成時に、対応する[エンジン設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)を指定します。この設定を持つ既存のテーブルの動作は、グローバル設定が変更されても変わりません。

**設定可能な値**

* `0` — 機能は無効です。
* `1` — 機能は有効です。

[`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper) の場合、[replicated](../../engines/table-engines/mergetree-family/replication.md) レプリケートテーブルでは、単一の `znode` を使用してデータパーツのヘッダーを compact に保存します。テーブルに多数のカラムがある場合、この保存方法により ZooKeeper に保存されるデータ量を大幅に削減できます。

:::note
`use_minimalistic_part_header_in_zookeeper = 1` を適用すると、この設定をサポートしていないバージョンへ ClickHouse サーバー をダウングレードすることはできません。クラスター内のサーバーで ClickHouse をアップグレードする際は注意してください。すべてのサーバーを一度にアップグレードしないでください。新しいバージョンの ClickHouse は、テスト環境またはクラスター内の数台のサーバーだけで検証するほうが安全です。

この設定ですでに保存されたデータパーツヘッダーは、以前の (compact ではない) 形式には復元できません。
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

クラスター上での[distributed DDL queries](../../sql-reference/distributed-ddl.md) (`CREATE`、`DROP`、`ALTER`、`RENAME`) の実行を管理します。
[ZooKeeper](/ja/operations/server-configuration-parameters/settings#zookeeper) が有効な場合にのみ機能します。

`<distributed_ddl>` 内で設定可能な項目は次のとおりです。

| Setting                | Description                                                                        | Default Value                  |
| ---------------------- | ---------------------------------------------------------------------------------- | ------------------------------ |
| `cleanup_delay_period` | 前回のクリーンアップから `cleanup_delay_period` 秒以上経過している場合、新しい node イベントを受信した後にクリーンアップを開始します。 | `60` 秒                         |
| `max_tasks_in_queue`   | queue 内に保持できる task の最大数です。                                                         | `1,000`                        |
| `path`                 | DDL queries の `task_queue` に対応する Keeper 内の path                                    |                                |
| `pool_size`            | 同時に実行できる `ON CLUSTER` queries の数です。                                                |                                |
| `profile`              | DDL queries の実行に使用する profile です。                                                   |                                |
| `task_max_lifetime`    | 経過時間がこの値を超えた node を削除します。                                                          | `7 * 24 * 60 * 60` (1 週間分の秒数)  |

**Example**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

<div id="access_control_path">
  ## access_control_path
</div>

SQL コマンドで作成されたユーザーおよびロールの設定を ClickHouse サーバーが保存するフォルダーのパスです。

**関連項目**

* [Access Control and Account Management](/ja/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

平文パスワード型 (安全ではない) を許可するかどうかを設定します。

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

安全ではない `no_password` パスワードタイプを許可するかどうかを設定します。

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

&#39;IDENTIFIED WITH no&#95;password&#39; が明示的に指定されていない限り、パスワードを設定しないユーザーの作成を禁止します。

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

デフォルトのセッションタイムアウト。単位は秒。

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

`CREATE USER u IDENTIFIED BY 'p'` のようなクエリで自動的に設定されるパスワードの種類を指定します。

指定できる値は次のとおりです。

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

設定を含む設定ファイルのセクションです。

* 事前定義されたユーザーが記述された設定ファイルへのパス。
* SQLコマンドで作成されたユーザーが保存されるフォルダーへのパス。
* SQLコマンドで作成されたユーザーが保存され、レプリケートされるZooKeeperノードのパス。

このセクションが指定されている場合、[users&#95;config](/ja/operations/server-configuration-parameters/settings#users_config) と [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path) のパスは使用されません。

`user_directories` セクションには任意の数の項目を含めることができ、項目の順序がその優先順位を表します (上にある項目ほど優先順位が高くなります) 。

**例**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

ユーザー、ロール、行ポリシー、クォータ、プロファイルは、ZooKeeper に保存することもできます。

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

`memory` セクション (情報をディスクに書き込まず、メモリ内にのみ保存する) と `ldap` セクション (情報を LDAP サーバー上に保存する) を定義することもできます。

ローカルで定義されていないユーザー用のリモートユーザーディレクトリとして LDAP サーバーを追加するには、次の設定を含む `ldap` セクションを 1 つ定義します。

| Setting  | Description                                                                                                                                                        |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `roles`  | LDAP サーバーから取得した各ユーザーに割り当てる、ローカルで定義されたロールの一覧を含むセクションです。ロールが指定されていない場合、ユーザーは認証後にいかなる操作も実行できません。一覧内のいずれかのロールが認証時点でローカルに定義されていない場合、認証の試行は、指定されたパスワードが誤っていた場合と同様に失敗します。 |
| `server` | `ldap_servers` config セクションで定義された LDAP サーバー名のいずれかです。このパラメータは必須で、空にすることはできません。                                                                                      |

**例**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

追加するカスタムのトップレベルドメインのリストを定義します。各エントリは `<name>/path/to/file</name>` という形式です。

例:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

関連項目:

* 関数 [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) およびその派生形。
  これらはカスタム TLD リスト名を受け取り、トップレベルのサブドメインから最初の主要なサブドメインまでを含むドメイン部分を返します。

<div id="proxy">
  ## プロキシ
</div>

HTTP および HTTPS リクエスト用のプロキシサーバーを定義します。現在は、S3 ストレージ、S3 テーブル関数、URL 関数でサポートされています。

プロキシサーバーを定義する方法は 3 つあります。

* 環境変数
* プロキシリスト
* リモートプロキシリゾルバ。

`no_proxy` を使用すると、特定のホストではプロキシサーバーを経由しないようにすることもできます。

**環境変数**

`http_proxy` および `https_proxy` 環境変数を使用すると、指定した
プロトコルのプロキシサーバーを指定できます。システムで設定されていれば、そのまま問題なく動作します。

これは、あるプロトコルに対して
プロキシサーバーが 1 つしかなく、そのプロキシサーバーが変わらない場合に最も簡単な方法です。

**プロキシリスト**

この方法では、あるプロトコルに対して 1 つ以上の
プロキシサーバーを指定できます。複数のプロキシサーバーが定義されている場合、
ClickHouse はそれらのプロキシをラウンドロビン方式で使用し、
サーバー間で負荷を分散します。これは、あるプロトコルに対して
複数のプロキシサーバーがあり、かつそのリストが変わらない場合に最も簡単な方法です。

**設定テンプレート**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

以下のタブで親フィールドを選択すると、対応する子フィールドを表示できます。

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | フィールド     | 説明                    |
    | --------- | --------------------- |
    | `<http>`  | 1 つ以上の HTTP プロキシのリスト  |
    | `<https>` | 1 つ以上の HTTPS プロキシのリスト |
  </TabItem>

  <TabItem value="http_https" label="<http> と <https>">
    | フィールド   | 説明        |
    | ------- | --------- |
    | `<uri>` | プロキシの URI |
  </TabItem>
</Tabs>

**リモートプロキシリゾルバ**

プロキシサーバーが動的に変わる場合は、リゾルバのエンドポイントを定義できます。ClickHouse はそのエンドポイントに空の GET リクエストを送信し、リモートリゾルバはプロキシホストを返します。ClickHouse はその情報を使って、次のテンプレートでプロキシ URI を組み立てます: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**設定テンプレート**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

以下のタブで親フィールドを選択すると、対応する子要素を表示できます：

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Field     | Description         |
    | --------- | ------------------- |
    | `<http>`  | 1 つ以上のリゾルバ*のリスト |
    | `<https>` | 1 つ以上のリゾルバ*のリスト |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | Field        | Description           |
    | ------------ | --------------------- |
    | `<resolver>` | リゾルバのエンドポイントおよびその他の詳細 |

    :::note
    複数の `<resolver>` 要素を指定できますが、特定のプロトコルで使用されるのは最初の
    `<resolver>` のみです。そのプロトコルに対するほかの `<resolver>`
    要素は無視されます。つまり、負荷分散が必要な場合は、
    リモートリゾルバ側で実装する必要があります。
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | Field                | Description                                                                                                     |
    | -------------------- | --------------------------------------------------------------------------------------------------------------- |
    | `<endpoint>`         | プロキシリゾルバの URI                                                                                                   |
    | `<proxy_scheme>`     | 最終的なプロキシ URI のプロトコルです。`http` または `https` のいずれかを指定できます。                                                          |
    | `<proxy_port>`       | プロキシリゾルバのポート番号                                                                                                  |
    | `<proxy_cache_time>` | リゾルバから取得した値を ClickHouse がキャッシュする時間を秒単位で指定します。この値を `0` に設定すると、ClickHouse は HTTP または HTTPS のリクエストごとにリゾルバへ問い合わせます。 |
  </TabItem>
</Tabs>

**優先順位**

プロキシ設定は、次の順序で決定されます：

| Order | Setting      |
| ----- | ------------ |
| 1.    | リモートプロキシリゾルバ |
| 2.    | プロキシリスト      |
| 3.    | 環境変数         |

ClickHouse は、リクエストのプロトコルに対して最も優先度の高いリゾルバの種類を確認します。定義されていない場合は、
環境リゾルバに到達するまで、次に優先度の高いリゾルバの種類を順に確認します。
これにより、複数のリゾルバの種類を混在させて使用することもできます。

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

デフォルトでは、`HTTP` プロキシ経由で `HTTPS` リクエストを送信する際に、トンネリング (つまり `HTTP CONNECT`) が使用されます。この設定を使用すると、これを無効にできます。

**no&#95;proxy**

デフォルトでは、すべてのリクエストがプロキシを経由します。特定のホストに対してこれを無効にするには、`no_proxy` 変数を設定する必要があります。
これは、list リゾルバおよび remote リゾルバでは `<proxy>` 句内に、environment リゾルバでは環境変数として設定できます。
IP アドレス、ドメイン、サブドメイン、および完全にバイパスするための `'*'` ワイルドカードをサポートしています。先頭のドットは、curl と同様に削除されます。

**例**

以下の設定では、`clickhouse.cloud` とそのすべてのサブドメイン (例: `auth.clickhouse.cloud`) へのリクエストでプロキシをバイパスします。
先頭にドットが付いている GitLab についても同様です。`gitlab.com` と `about.gitlab.com` はどちらもプロキシをバイパスします。

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

<div id="workload_path">
  ## workload_path
</div>

すべての `CREATE WORKLOAD` および `CREATE RESOURCE` クエリの保存先となるディレクトリです。デフォルトでは、サーバーのワーキングディレクトリ配下の `/workload/` フォルダーが使用されます。

**例**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**関連項目**

* [ワークロード階層](/ja/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

すべての`CREATE WORKLOAD`および`CREATE RESOURCE`クエリの保存先として使用される、ZooKeeperノードへのパスです。一貫性を保つため、すべてのSQL定義はこの単一のznodeの値として保存されます。デフォルトではZooKeeperは使用されず、定義は[ディスク](#workload_path)に保存されます。

**例**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**関連項目**

* [ワークロード階層](/ja/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

[`zookeeper_log`](/ja/operations/system-tables/zookeeper_log) システムテーブルの設定です。

以下の設定はサブタグで指定できます。

<SystemLogParameters />

**例**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```