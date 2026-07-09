---
description: 'ClickHouse のテストとテストスイートの実行ガイド'
sidebar_label: 'テスト'
sidebar_position: 40
slug: /development/tests
title: 'ClickHouse のテスト'
doc_type: 'guide'
---

<div id="test-types">
  ## テストの種類
</div>

ClickHouse には、次のテストがあります。

* [機能テスト](#functional-tests) - 以下のように一部が重複するサブセットを含む、クエリとスクリプトのセット
  * [Fast test](#running-fast-tests) - 最小のサブセット
  * [ステートレステスト](#running-stateless-tests) - データベースにデータを投入する必要がないもの
  * 並列に実行できない逐次テスト
* [結合テスト](#integration-tests) - クラスターで `pytest` によって実行されます
* [単体テスト](#unit-tests)
* [パフォーマンステスト](#performance-tests)
* [ビルドテスト](#build-tests)
* [サニタイザ](#sanitizers)
* [ファザー](#fuzzing)
  そのほかにもあります。詳しくは以下の各セクションを参照してください。

<div id="functional-tests">
  ## 機能テスト
</div>

機能テストは、最もシンプルで使いやすいテストです。
ClickHouse の機能の大半は機能テストで検証でき、その方法でテスト可能な ClickHouse のコード変更では、必ず機能テストを使用する必要があります。

各機能テストでは、動作中の ClickHouseサーバーに 1 つまたは複数のクエリを送信し、その結果をリファレンスと比較します。

テストは `./tests/queries` ディレクトリにあります。

各テストは、`.sql` と `.sh` の 2 種類のいずれかです。

* `.sql` テストは、`clickhouse-client` にパイプされるシンプルな SQL スクリプトです。
* `.sh` テストは、それ自体で実行されるスクリプトです。

一般的には、`.sh` テストよりも SQL テストを優先してください。
`.sh` テストを使うのは、入力データを `clickhouse-client` にパイプしたり、`clickhouse-local` をテストしたりする場合のように、純粋な SQL だけでは検証できない機能をテストしなければならないときだけにしてください。

:::note
データ型 `DateTime` と `DateTime64` をテストする際によくある誤りは、サーバーが特定のタイムゾーン (例: &quot;UTC&quot;) を使用していると想定してしまうことです。実際にはそうではなく、CI のテスト実行ではタイムゾーンは
意図的にランダム化されています。最も簡単な回避策は、たとえば `toDateTime64(val, 3, 'Europe/Amsterdam')` のように、テスト値のタイムゾーンを明示的に指定することです。
:::

<div id="running-a-test-locally">
  ### テストをローカルで実行する
</div>

デフォルトのポート (9000) で待ち受けるように、ClickHouse server をローカルで起動します。
たとえば、テスト `01428_hash_set_nan_key` を実行するには、リポジトリのフォルダーに移動して、次のコマンドを実行します。

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

テスト結果 (`stderr` と `stdout`) は、テスト自体と同じ場所にある `01428_hash_set_nan_key.[stderr|stdout]` ファイルに書き込まれます (`queries/0_stateless/foo.sql` の場合、出力は `queries/0_stateless/foo.stdout` に書き込まれます) 。

`clickhouse-test` のすべてのオプションについては、`tests/clickhouse-test --help` を参照してください。
すべてのテストを実行することも、テスト名のフィルターを指定して一部のテストだけを実行することもできます: `./clickhouse-test substring`。
また、テストを並列に実行したり、ランダムな順序で実行したりするためのオプションもあります。

<div id="running-tests-on-macos">
  #### macOS (Darwin) でテストを実行する
</div>

多くの機能テストでは、GNU のコマンドラインユーティリティ (`timeout`、`head`、`sed`、`grep`、`date` など) をシェル経由で実行します。macOS にはこれらのツールの BSD 版が標準で含まれていますが、動作やオプションが異なります (たとえば、BSD `head` は `head -c 1G` を受け付けず、BSD `ps` には `--` のロングオプションがなく、`timeout` 自体も存在しません) 。BSD 版のツールでテストを実行すると、実際には問題がないのに失敗することがあります。

macOS の CI ランナーでは、Homebrew で GNU ツールをインストールし、`PATH` で BSD 版よりも優先されるようにしています。ローカルでも同じ構成を再現してください。

```sh
brew install coreutils gnu-sed grep
export PATH="$(brew --prefix)/opt/coreutils/libexec/gnubin:$(brew --prefix)/opt/gnu-sed/libexec/gnubin:$(brew --prefix)/opt/grep/libexec/gnubin:$PATH"
```

`coreutils` は GNU の `timeout`、`head`、`date` などのコマンドを提供します。`gnu-sed` と `grep` は GNU の `sed` と `grep` を提供します。これで、`which timeout head sed grep` は `gnubin` のパスを指すようになるはずです。

<div id="running-fast-tests">
  ### Fast test の実行
</div>

テストの一部 (「Fast test」) を実行するには、ある程度の性能を持つマシンが必要になる場合があります。以下の手順は、100 GB のストレージを備えた `t3.2xlarge` AWS amd64 Ubuntu インスタンスで動作します。

1. 必要な前提パッケージをインストールし、再ログインします。

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
```

2. ソースコードを取得します。

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. コードをビルドし、&quot;fast tests&quot; を実行します。

```sh
python -m ci.praktika run fast
```

次のように表示されます

```sh
Failed: 0, Passed: 7394, Skipped: 1795
```

実行をそのまま放置する場合は、`ssh` 接続が切れた後も動作を継続できるように、`nohup` または `disown` を使用できます。

<div id="running-stateless-tests">
  ### ステートレステストの実行
</div>

ステートレステストを実行するには、ある程度性能の高いマシンが必要になる場合があります。以下の手順は、200 GB のストレージを備えた `m7i.8xlarge` の AWS amd64 Ubuntu インスタンスで動作します。

1. 必要な前提ツールをインストールし、再ログインします。

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

2. ソースコードを取得します。

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. コードをビルドします。

```sh
python -m ci.praktika run build_debug
cp ci/tmp/build/programs/clickhouse ci/tmp
```

4. 並列実行できるステートレステストを実行します。

```sh
python -m ci.praktika run functional
```

次のように表示されるはずです

```sh
Failed: 0, Passed: 8497, Skipped: 103
```

注。`python -m ci.praktika run` を実行すると、特定のCIジョブが実行されます。ClickHouse CI の詳細は[こちら](continuous-integration.md#running-stateless-tests)を参照してください。

<div id="adding-a-new-test">
  ### 新しいテストの追加
</div>

新しいテストを追加するには、まず `queries/0_stateless` ディレクトリに `.sql` または `.sh` ファイルを作成します。
次に、`clickhouse-client < 12345_test.sql > 12345_test.reference` または `./12345_test.sh > ./12345_test.reference` を使って、対応する `.reference` ファイルを生成します。

テストでは、あらかじめ自動作成される database `test` 内の table に対してのみ、create、drop、select などを行ってください。
一時テーブルを使用してもかまいません。

CI と同じ環境をローカルで再現するには、テスト設定をインストールしてください (Zookeeper のモック実装を使用し、いくつかの設定を調整します) 

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
テストは次のようにすべきです

* 最小限であること: 必要最小限のテーブル、カラム、複雑さだけを作成する
* 高速であること: 数秒以上かからないこと (できれば 1 秒未満) 
* 正確かつ決定論的であること: テスト対象の機能が動作していない場合に、かつその場合にのみ失敗する
* 分離されている / 無状態であること: 環境やタイミングに依存しない
* 網羅的であること: 0、NULL値、空集合、例外のようなコーナーケースをカバーする (ネガティブテストには構文 `-- { serverError xyz }` および `-- { clientError xyz }` を使用する) 
* テストの最後にテーブルをクリーンアップする (残りがある場合に備えて) 
* 他のテストで同じ内容をテストしていないことを確認する (つまり、まず grep する) 。
  :::

<div id="templated-tests-with-jinja">
  ### Jinja を使ったテンプレート化テスト
</div>

`.sql` テストは、ファイル名に `.j2` 接尾辞を付けることで、[Jinja2](https://jinja.palletsprojects.com/) テンプレートとして記述できます。つまり、`foo.sql` は `foo.sql.j2` になります。テストを実行する前に、`clickhouse-test` がテンプレートを通常の `.sql` スクリプトにレンダリングし、その結果を実行します。

これは、同じクエリを少しずつ変えながら繰り返し使うテストで便利です。ループを使えば、各クエリを手作業で書く代わりに、簡潔なテンプレートからクエリを生成できます。特によく使われる構文は次のとおりです。

* ブロックを繰り返す `{% for ... %} ... {% endfor %}`,
* 出力に値を埋め込む `{{ expression }}`,
* 生成されるスクリプトをすっきり保つため、隣接する空白を削除する `-%}` と `{%-`。

たとえば、次のテンプレートがあります。

```sql
{% for type in ['UInt8', 'UInt16', 'UInt32'] -%}
SELECT toTypeName(0::{{ type }});
{% endfor -%}
```

次のように表示されます:

```sql
SELECT toTypeName(0::UInt8);
SELECT toTypeName(0::UInt16);
SELECT toTypeName(0::UInt32);
```

期待される出力は、完全に展開された結果を含む通常の `<name>.reference` ファイルとして指定することも、比較前に `clickhouse-test` によって同様にレンダリングされる `<name>.reference.j2` テンプレートとして指定することもできます。期待される出力も繰り返しパターンに従う場合は、テンプレート形式を使用してください。その他の例については、`tests/queries/0_stateless/` にある既存の `*.sql.j2` ファイルを参照してください。

<div id="restricting-test-runs">
  ### テスト実行の制限
</div>

テストには、CI でどのコンテキストで実行するかを制限する *タグ* を 0 個以上付けることができます。

`.sql` テストでは、タグは 1 行目に SQL コメントとして記述します。

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

`.sh` テストでは、タグは2行目にコメントとして記述します：

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

利用可能なタグの一覧:

| Tag name                       | What it does                                                 | Usage example                                        |
| ------------------------------ | ------------------------------------------------------------ | ---------------------------------------------------- |
| `disabled`                     | テストは実行されません                                                  |                                                      |
| `long`                         | テストの実行時間が 1 分から 10 分に延長されます                                  |                                                      |
| `deadlock`                     | テストが長時間ループで実行されます                                            |                                                      |
| `race`                         | `deadlock` と同じです。`deadlock` の使用を推奨します                        |                                                      |
| `shard`                        | server が `127.0.0.*` で待ち受けている必要があります                         |                                                      |
| `distributed`                  | `shard` と同じです。`shard` の使用を推奨します                              |                                                      |
| `global`                       | `shard` と同じです。`shard` の使用を推奨します                              |                                                      |
| `zookeeper`                    | テストの実行には Zookeeper または ClickHouse Keeper が必要です               | テストで `ReplicatedMergeTree` を使用する                     |
| `replica`                      | `zookeeper` と同じです。`zookeeper` の使用を推奨します                      |                                                      |
| `no-fasttest`                  | テストは [Fast test](#test-types) では実行されません                      | テストで `MySQL` テーブルエンジンを使用しており、Fast test では無効になっている    |
| `fasttest-only`                | テストは [Fast test](#test-types) でのみ実行されます                      |                                                      |
| `no-[asan, tsan, msan, ubsan]` | [サニタイザ](#sanitizers) を有効にしたビルドではテストを無効にします                   | テストは QEMU 上で実行されますが、QEMU はサニタイザに対応していません             |
| `no-replicated-database`       | デフォルトデータベースで `ReplicatedDatabaseEngine` を使用している場合はテストを無効にします |                                                      |
| `no-ordinary-database`         | デフォルトデータベースエンジンが `Ordinary` の場合はテストを無効にします                   |                                                      |
| `no-parallel`                  | このテストとほかのテストを並列に実行しないようにします                                  | テストが `system` テーブルを読み取るため、不変条件が崩れる可能性があります           |
| `no-parallel-replicas`         | 並列レプリカが有効な場合はテストを無効にします                                      |                                                      |
| `no-debug`                     | Debug ビルドではテストを無効にします                                        |                                                      |
| `no-release`                   | Release ビルドではテストを無効にします                                      |                                                      |
| `no-darwin`                    | macOS (Darwin) ではテストを無効にします                                  | テストが分散クエリ、`procfs`、HTTP server などの Linux 固有機能に依存している |

次のオプションもサポートされています: `no-polymorphic-parts`, `no-random-settings`, `no-random-merge-tree-settings`, `no-backward-compatibility-check`, `no-cpu-x86_64`, `no-cpu-aarch64`, `no-cpu-ppc64le`, `no-s3-storage`.

上記の設定に加えて、特定の ClickHouse 機能の使用有無を定義するために、`system.build_options` の `USE_*` フラグも使用できます。
たとえば、テストで MySQL テーブルを使用する場合は、`use-mysql` タグを追加する必要があります。

<div id="specifying-limits-for-random-settings">
  ### ランダム設定の制限を指定する
</div>

テストでは、実行中にランダム化される設定に対して、許容される最小値と最大値を指定できます。

`.sh` テストでは、制限はタグのある行の次の行にコメントとして記述します。タグが指定されていない場合は、2 行目に記述します。

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

`.sql` テストでは、タグは `tags` のある行の次の行、または先頭行に SQL コメントとして記述します:

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

片方の制限だけを指定する必要がある場合は、もう一方に `None` を使用できます。

<div id="choosing-the-test-name">
  ### テスト名の選択
</div>

テスト名は、`00422_hash_function_constexpr.sql` のように、5桁のプレフィックスの後に説明的な名前を付ける形式です。
プレフィックスを選ぶには、ディレクトリ内ですでに使われている最大のプレフィックスを見つけて、1 つ増やします。

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

その間に、同じ数値プレフィックスを持つ別のテストが追加されることもありますが、これは問題なく、支障もないため、後から変更する必要はありません。

<div id="checking-for-an-error-that-must-occur">
  ### 発生すべきエラーの確認
</div>

誤ったクエリに対してサーバーエラーが発生することを確認したい場合があります。そのために、SQL テストでは次の形式の特別なアノテーションをサポートしています。

```sql
SELECT x; -- { serverError 49 }
```

このテストは、未知のカラム `x` について、サーバーがコード 49 のエラーを返すことを確認します。
エラーが発生しない場合、または別のエラーが返された場合、このテストは失敗します。
クライアント側でエラーが発生することを確認したい場合は、代わりに `clientError` アノテーションを使用してください。

エラーメッセージの特定の文言は確認しないでください。将来変更される可能性があり、そのせいで不要にテストが壊れるおそれがあります。
確認するのはエラーコードだけにしてください。
既存のエラーコードが要件に対して十分に正確でない場合は、新しいものを追加することを検討してください。

<div id="testing-a-distributed-query">
  ### 分散クエリのテスト
</div>

機能テストで分散クエリを使用する場合は、`remote` テーブル関数と `127.0.0.{1..2}` アドレスを使って、サーバー自身に対してクエリを実行できます。あるいは、`test_shard_localhost` のように、サーバー設定ファイルで事前定義されたテスト用クラスターを使用することもできます。
サーバーが分散クエリをサポートするよう設定された適切な構成で CI により実行されるよう、テスト名には `shard` または `distributed` という単語を含めてください。

<div id="working-with-temporary-files">
  ### 一時ファイルの扱い
</div>

シェルテストでは、その場でファイルを作成して使う必要が生じることがあります。
一部のCIチェックではテストが並列に実行されるため、一意でない名前の一時ファイルをスクリプト内で作成または削除すると、Flaky などのCIチェックが失敗する原因になることがあります。
これを避けるには、環境変数 `$CLICKHOUSE_TEST_UNIQUE_NAME` を使って、一時ファイルに実行中のテストごとに一意な名前を付けてください。
そうすることで、セットアップ時に作成するファイルやクリーンアップ時に削除するファイルが、そのテストだけで使われているものであり、並列に実行されている別のテストで使われているものではないことを確実にできます。

<div id="known-bugs">
  ## 既知のバグ
</div>

機能テストで容易に再現できる既知のバグについては、あらかじめ用意した機能テストを `tests/queries/bugs` ディレクトリに配置します。
これらのテストは、バグが修正されると `tests/queries/0_stateless` に移動されます。

<div id="integration-tests">
  ## 結合テスト
</div>

結合テストでは、クラスター構成の ClickHouse や、MySQL、Postgres、MongoDB などの他のサーバーと ClickHouse の連携をテストできます。
これらのテストは、ネットワーク分断やパケットドロップなどをエミュレートするのに役立ちます。
これらのテストは Docker 上で実行され、各種ソフトウェアを含む複数のコンテナーを作成します。

これらのテストの実行方法については、`tests/integration/README.md` を参照してください。

ClickHouse とサードパーティ製ドライバーとのインテグレーションはテストされていない点に注意してください。
また、現時点では、JDBC ドライバーおよび ODBC ドライバとの結合テストもありません。

<div id="unit-tests">
  ## 単体テスト
</div>

単体テストは、ClickHouse 全体ではなく、独立した単一のライブラリやクラスをテストしたい場合に有用です。
テストのビルドは、CMake オプション `ENABLE_TESTS` で有効または無効にできます。
単体テスト (およびその他のテストプログラム) は、コード内の各所にある `tests` サブディレクトリに配置されています。
単体テストを実行するには、`ninja test` と入力します。
テストによっては `gtest` を使用しますが、単にテスト失敗時に非ゼロの終了コードを返すプログラムもあります。

コードがすでに機能テストでカバーされている場合は、単体テストは必須ではありません (通常、機能テストの方がはるかに簡単に使えます) 。

個別の gtest チェックは、実行可能ファイルを直接呼び出して実行できます。たとえば次のとおりです。

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

<div id="performance-tests">
  ## パフォーマンステスト
</div>

パフォーマンステストでは、合成クエリを使って ClickHouse の特定の独立した部分の性能を測定し、比較できます。
パフォーマンステストは `tests/performance/` にあります。
各テストは、テストケースの説明を記述した `.xml` ファイルで表現されます。
テストは `docker/test/performance-comparison` ツールで実行します。実行方法については readme ファイルを参照してください。

各テストでは、1 つまたは複数のクエリ (必要に応じて parameter の組み合わせを含む) をループで実行します。

特定のシナリオで ClickHouse の性能を改善したい場合、その改善を単純なクエリで確認できるのであれば、パフォーマンステストを作成することを強く推奨します。
また、比較的独立していて複雑すぎない SQL 関数を追加または変更する場合にも、パフォーマンステストを作成することを推奨します。
テスト中は、`perf top` やその他の `perf` ツールを使うのが常に有効です。

<div id="test-tools-and-scripts">
  ## テストツールとスクリプト
</div>

`tests` ディレクトリ内の一部のプログラムは、あらかじめ用意されたテストではなく、テスト用ツールです。
たとえば、`Lexer` には、stdin をトークン化して、その結果を色付きで stdout に書き出すだけのツール `src/Parsers/tests/lexer` があります。
この種のツールは、コード例として利用したり、調査や手動テストに使ったりできます。

<div id="miscellaneous-tests">
  ## その他のテスト
</div>

`tests/external_models` には機械学習モデル向けのテストがあります。
これらのテストは更新されておらず、結合テストへ移行する必要があります。

クォーラム insert 用の個別のテストもあります。
このテストでは、ClickHouse クラスターを別々のサーバー上で実行し、さまざまな障害ケースをエミュレートします。たとえば、ネットワーク分断、パケットドロップ (ClickHouse ノード間、ClickHouse と ZooKeeper 間、ClickHouse server と client 間など) 、`kill -9`、`kill -STOP`、`kill -CONT` です。[Jepsen](https://aphyr.com/tags/Jepsen) のような形です。続いてこのテストは、確認応答されたすべての insert が書き込まれており、拒否されたすべての insert は書き込まれていないことを確認します。

<div id="manual-testing">
  ## 手動テスト
</div>

新しい機能を開発したら、手動でもテストするのが妥当です。
次の手順で実施できます。

ClickHouse をビルドします。ターミナルから ClickHouse を実行するには、`programs/clickhouse-server` に移動して `./clickhouse-server` を実行します。デフォルトでは、現在のディレクトリにある設定 (`config.xml`、`users.xml`、および `config.d` と `users.d` ディレクトリ内のファイル) が使用されます。ClickHouse server に接続するには、`programs/clickhouse-client/clickhouse-client` を実行します。

すべての clickhouse ツール (server、client など) は、実際には `clickhouse` という単一の binary へのシンボリックリンクにすぎない点に注意してください。
この binary は `programs/clickhouse` にあります。
また、すべてのツールは `clickhouse-tool` ではなく `clickhouse tool` として起動することもできます。

別の方法として、ClickHouse package をインストールすることもできます。ClickHouse repository から stable release をインストールするか、ClickHouse sources root で `./release` を実行して自分で package をビルドできます。
その後、`sudo clickhouse start` で server を起動します (停止するには `stop` を使用します) 。
logs は `/etc/clickhouse-server/clickhouse-server.log` で確認してください。

システムに ClickHouse がすでにインストールされている場合は、新しい `clickhouse` binary をビルドして既存の binary と置き換えることができます:

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

また、システムの clickhouse-server を停止し、同じ構成でログを端末に出力する独自の clickhouse-server を実行することもできます。

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

gdb を使った例:

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

システムの clickhouse-server がすでに実行中で、停止したくない場合は、`config.xml` のポート番号を変更するか、 (または `config.d` ディレクトリ内のファイルで上書きし) 、適切なデータパスを指定して実行できます。

`clickhouse` バイナリは依存関係がほとんどなく、幅広い Linux ディストリビューションで動作します。
サーバー上で変更内容を手早く簡易的にテストするには、ビルドしたばかりの `clickhouse` バイナリを `scp` でサーバーにコピーし、上記の例と同様に実行するだけです。

<div id="build-tests">
  ## ビルドテスト
</div>

ビルドテストでは、さまざまな代替構成や一部の異なるシステム上で、ビルドが壊れていないことを確認できます。
これらのテストも自動化されています。

例:

* Darwin x86&#95;64 (macOS) 向けのクロスコンパイル
* FreeBSD x86&#95;64 向けのクロスコンパイル
* Linux AArch64 向けのクロスコンパイル
* システムパッケージのライブラリを使用した Ubuntu 上でのビルド (非推奨) 
* ライブラリを共有リンクするビルド (非推奨) 

たとえば、システムパッケージを使ったビルドは、システムにどのバージョンのパッケージが入っているかを正確に保証できないため、望ましい方法ではありません。
しかし、これは Debian のメンテナーにとっては本当に必要です。
そのため、少なくともこのビルド方法はサポートしなければなりません。
別の例として、共有リンクはよく問題の原因になりますが、一部の愛好家には必要です。

すべてのビルド方式ですべてのテストを実行することはできませんが、少なくともさまざまなビルド方式が壊れていないことは確認したいと考えています。
この目的のために、ビルドテストを使用します。

また、コンパイルに時間がかかりすぎたり、RAM を必要としすぎたりする翻訳単位がないこともテストしています。

さらに、大きすぎるスタックフレームがないこともテストしています。

<div id="testing-for-protocol-compatibility">
  ## プロトコル互換性のテスト
</div>

ClickHouseのネットワークプロトコルを拡張する際には、古いclickhouse-clientが新しいclickhouse-serverで動作し、新しいclickhouse-clientが古いclickhouse-serverで動作することを手動でテストします (対応するパッケージのバイナリを実行するだけです) 。

また、いくつかのケースについては統合テストで自動的に検証します。

* 古いバージョンのClickHouseで書き込まれたデータを、新しいバージョンで正常に読み取れるか。
* 異なるClickHouseバージョンが混在するクラスターで、分散クエリが機能するか。

<div id="help-from-the-compiler">
  ## コンパイラの支援
</div>

ClickHouse のメインコード (`src` ディレクトリに配置されています) は、`-Wall -Wextra -Werror` に加え、いくつかの追加警告も有効にしてビルドされます。
ただし、これらのオプションはサードパーティライブラリには適用されません。

Clang にはさらに便利な警告があり、`-Weverything` でそれらを確認して、デフォルトビルドに取り込むものを選べます。

ClickHouse のビルドには、開発環境でも本番環境でも常に clang を使用しています。
手元のマシンでは debug mode でビルドしても構いません (ノート PC のバッテリーを節約するため) が、制御フローやプロシージャ間解析がより適切に行われるため、コンパイラは `-O3` でより多くの警告を生成できる点に注意してください。
clang で debug mode のビルドを行うと、`libc++` のデバッグ版が使われるため、実行時により多くのエラーを検出できます。

<div id="sanitizers">
  ## サニタイザ
</div>

:::note
プロセス (ClickHouse server またはクライアント) をローカルで実行した際、起動時にクラッシュする場合は、アドレス空間配置のランダム化を無効にする必要があるかもしれません: `sudo sysctl kernel.randomize_va_space=0`
:::

<div id="address-sanitizer">
  ### Address サニタイザ
</div>

コミットごとに、ASan を有効にした状態で機能テスト、インテグレーション、ストレステスト、単体テストを実行しています。

<div id="thread-sanitizer">
  ### Thread サニタイザ
</div>

コミットごとに、TSan を使用して機能テスト、インテグレーション、ストレステスト、単体テストを実行しています。

<div id="memory-sanitizer">
  ### メモリサニタイザ
</div>

各コミットごとに、MSan を使用して機能テスト、結合テスト、ストレステスト、単体テストを実行しています。

<div id="undefined-behaviour-sanitizer">
  ### 未定義動作サニタイザ
</div>

コミットごとに、UBSan を有効にして機能テスト、インテグレーション、ストレステスト、および単体テストを実行しています。
一部のサードパーティライブラリのコードには、UB に対するサニタイズが適用されていません。

<div id="valgrind-memcheck">
  ### Valgrind (memcheck)
</div>

以前は夜間に Valgrind 上で機能テストを実行していましたが、現在は行っていません。
完了までに数時間かかります。
現在、`re2` ライブラリでは既知の誤検知が 1 件あります。詳しくは [こちらの記事](https://research.swtch.com/sparse) を参照してください。

<div id="fuzzing">
  ## ファジング
</div>

ClickHouse のファジングは、[libFuzzer](https://llvm.org/docs/LibFuzzer.html) とランダムな SQL クエリの両方を用いて実装されています。
すべてのファズテストは、サニタイザ (Address と Undefined) を有効にして実行する必要があります。

libFuzzer は、ライブラリコードを個別にファズテストするために使用されます。
ファザーはテストコードの一部として実装され、名前の末尾に &quot;&#95;fuzzer&quot; が付きます。
ファザーの例は `src/Parsers/fuzzers/lexer_fuzzer.cpp` にあります。
libFuzzer 固有の設定、辞書、およびコーパスは `tests/fuzz` に保存されています。
ユーザー入力を扱うあらゆる機能に対して、ファズテストを書くことを推奨します。

ファザーはデフォルトではビルドされません。
ファザーをビルドするには、`-DENABLE_FUZZING=1` と `-DENABLE_TESTS=1` の両方のオプションを設定する必要があります。
ファザーのビルド時には Jemalloc を無効にすることを推奨します。
ClickHouse のファジングを
Google OSS-Fuzz に統合するための設定は `docker/fuzz` にあります。

また、ランダムな SQL クエリを生成し、サーバーがそれらの実行中にクラッシュしないことを確認するシンプルなファズテストも使用しています。
これは `00746_sql_fuzzy.pl` にあります。
このテストは継続的に (夜通し、あるいはそれ以上) 実行する必要があります。

さらに、大量のエッジケースを見つけられる高度な AST ベースのクエリファザーも使用しています。
これはクエリ AST に対してランダムな並べ替えや置換を行います。
以前のテストの AST ノードを記憶し、後続のテストをランダムな順序で処理しながら、そのファジングに利用します。
このファザーの詳細は、[このブログ記事](https://clickhouse.com/blog/fuzzing-click-house)で確認できます。

<div id="stress-test">
  ## ストレステスト
</div>

ストレステストは、ファジングの一種です。
単一のサーバーで、すべての機能テストをランダムな順序で並列に実行します。
テスト結果は確認しません。

代わりに、次の点を確認します。

* サーバーがクラッシュせず、Debug または サニタイザ のトラップがトリガーされないこと。
* デッドロックが発生しないこと。
* データベース構造の整合性が保たれていること。
* テスト後にサーバーを正常に停止でき、さらに例外なしで再起動できること。

バリアントは 5 つあります (Debug、ASan、TSan、MSan、UBSan) 。

<div id="thread-fuzzer">
  ## Thread ファザー
</div>

Thread ファザー (Thread Sanitizer と混同しないでください) は、スレッドの実行順序をランダム化できる、別種の fuzzing です。
より多くの特殊なケースを見つけるのに役立ちます。

<div id="security-audit">
  ## セキュリティ監査
</div>

当社のセキュリティチームは、セキュリティの観点からClickHouseの機能について基本的なレビューを行いました。

<div id="static-analyzers">
  ## 静的アナライザ
</div>

`clang-tidy` はコミットごとに実行しています。
`clang-static-analyzer` のチェックも有効です。
`clang-tidy` は一部のスタイルチェックにも使用しています。

これまでに `clang-tidy`、`Coverity`、`cppcheck`、`PVS-Studio`、`tscancode`、`CodeQL` を評価しています。
使用方法については、`tests/instructions/` ディレクトリ内の手順を参照してください。

IDE に `CLion` を使用している場合は、いくつかの `clang-tidy` チェックをそのまま利用できます。

また、シェルスクリプトの静的解析には `shellcheck` も使用しています。

<div id="hardening">
  ## ハードニング
</div>

デバッグビルドでは、ユーザーレベルの割り当てに ASLR を適用するカスタムアロケータを使用しています。

また、割り当て後は readonly になることが想定されるメモリ領域を手動で保護しています。

デバッグビルドではさらに、libc にカスタマイズを加え、「有害な」 (廃止された、安全でない、thread-safe でない) 関数が呼び出されないようにしています。

デバッグ用アサーションも広範に使用されています。

デバッグビルドでは、「論理エラー」コードの例外 (bug を意味します) が発生すると、プログラムは即座に終了します。
これにより、リリースビルドでは例外を使用しつつ、デバッグビルドではそれをアサーションとして扱えます。

jemalloc のデバッグ版がデバッグビルドで使用されます。
libc++ のデバッグ版がデバッグビルドで使用されます。

<div id="runtime-integrity-checks">
  ## 実行時整合性チェック
</div>

ディスクに保存されたデータにはチェックサムが付けられます。
MergeTree テーブル内のデータには、3 つの方法で同時にチェックサムが付けられます* (圧縮データブロック、非圧縮データブロック、ブロック全体に対する総チェックサム) 。
クライアントとサーバーの間、またはサーバー間でネットワーク経由で転送されるデータにもチェックサムが付けられます。
レプリケーションにより、レプリカ上のデータがビット単位で同一であることが保証されます。

これは、不良ハードウェア (ストレージ媒体でのビット腐敗、サーバーの RAM でのビット反転、ネットワークコントローラーの RAM でのビット反転、ネットワークスイッチの RAM でのビット反転、クライアントの RAM でのビット反転、伝送中のビット反転) から保護するために必要です。
ビット反転は珍しいものではなく、ECC RAM を使用していても、また TCP チェックサムがあっても発生し得ることに注意してください (毎日それぞれ PB 級のデータを処理する数千台のサーバーを運用している場合) 。
[ビデオを見る (ロシア語) ](https://www.youtube.com/watch?v=ooBAQIe0KlQ)。

ClickHouse は、運用エンジニアが不良ハードウェアを特定するのに役立つ診断機能を提供します。

* しかも低速ではありません。

<div id="code-style">
  ## コードスタイル
</div>

コードスタイルのルールについては[こちら](style.md)を参照してください。

一般的なスタイル違反をいくつかチェックするには、`utils/check-style` スクリプトを使用できます。

コードのスタイルを正しく整えるには、`clang-format` を使用できます。
`.clang-format` ファイルはソースルートにあります。
これは実際のコードスタイルとおおむね一致しています。
ただし、既存のファイルに `clang-format` を適用するとフォーマットがかえって悪くなるため、推奨されません。
代わりに、clang のソースリポジトリにある `clang-format-diff` ツールを使用できます。

または、コードを再フォーマットするために `uncrustify` ツールを試すこともできます。
設定ファイルはソースルートの `uncrustify.cfg` にあります。
こちらは `clang-format` ほど十分にテストされていません。

`CLion` には独自のコードフォーマッタがありますが、私たちのコードスタイルに合わせて調整する必要があります。

<div id="test-coverage">
  ## テストカバレッジ
</div>

テストカバレッジも追跡していますが、対象は機能テストとclickhouse-serverに限られます。
これは毎日実施されています。

<div id="tests-for-tests">
  ## テストを検証するためのテスト
</div>

不安定なテストを検出するための自動チェックがあります。
これは、すべての新しいテストを100回 (機能テストの場合) または10回 (結合テストの場合) 実行します。
テストが1回でも失敗した場合、そのテストは不安定と見なされます。

<div id="test-automation">
  ## テスト自動化
</div>

テストは [GitHub Actions](https://github.com/features/actions) で実行しています。

ビルドジョブとテストは、コミットごとに Sandbox 上で実行されます。
生成されたパッケージとテスト結果は GitHub で公開され、直接リンクからダウンロードできます。
アーティファクトは数か月間保存されます。
GitHub でプルリクエストを送ると、「can be tested」タグが付き、CI システムが ClickHouse パッケージ (リリース、デバッグ、アドレスサニタイザー付きなど) をビルドします。