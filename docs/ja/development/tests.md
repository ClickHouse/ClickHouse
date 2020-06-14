---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 69
toc_title: "ClickHouse\u30C6\u30B9\u30C8\u306E\u5B9F\u884C\u65B9\u6CD5"
---

# ClickHouseのテスト {#clickhouse-testing}

## 機能テスト {#functional-tests}

機能テストは、最も簡単で使いやすいです。 ClickHouseの機能のほとんどは機能テストでテストすることができ、そのようにテストできるClickHouseコードのすべての変更に使用することが必須です。

各機能テストは、実行中のClickHouseサーバーに一つまたは複数のクエリを送信し、結果を参照と比較します。

テストは `queries` ディレクトリ。 サブディレクトリは二つあります: `stateless` と `stateful`. ステートレステストは、プリロードされたテストデータなしでクエリを実行します。 状態での検査が必要とな予圧試験データからのYandex.Metricaおよび一般に利用できない。 私たちは使用する傾向があります `stateless` テストと新しい追加を避ける `stateful` テストだ

それぞれの試験できるの種類: `.sql` と `.sh`. `.sql` testは、パイプ処理される単純なSQLスクリプトです `clickhouse-client --multiquery --testmode`. `.sh` testは、それ自体で実行されるスクリプトです。

すべてのテストを実行するには、 `clickhouse-test` ツール。 見て！ `--help` 可能なオプションのリスト。 できるだけ実行すべての試験または実行のサブセットの試験フィルター部分文字列の試験名: `./clickhouse-test substring`.

機能テストを呼び出す最も簡単な方法は、コピーすることです `clickhouse-client` に `/usr/bin/`,run `clickhouse-server` そして、実行 `./clickhouse-test` 独自のディレクトリから。

新しいテストを追加するには、 `.sql` または `.sh` ファイル `queries/0_stateless` ディレクトリでチェックを手動でその生成 `.reference` 次の方法でファイル: `clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` または `./00000_test.sh > ./00000_test.reference`.

テストでは、テーブルのみを使用（create、dropなど）する必要があります `test` また、テストでは一時テーブルを使用することもできます。

機能テストで分散クエリを使用する場合は、以下を利用できます `remote` テーブル関数 `127.0.0.{1..2}` または、サーバー設定ファイルで次のように定義済みのテストクラスタを使用できます `test_shard_localhost`.

いくつかのテストには `zookeeper`, `shard` または `long` 彼らの名前で。
`zookeeper` ZooKeeperを使用しているテスト用です。 `shard` そのテストのためです
サーバーにリッスンが必要 `127.0.0.*`; `distributed` または `global` 同じを持っている
意味だ `long` 少し長く実行されるテストのためのものです。 あなたはできる
disableこれらのグループの試験を使用 `--no-zookeeper`, `--no-shard` と
`--no-long` オプション、それぞれ。

## 既知のバグ {#known-bugs}

機能テストで簡単に再現できるいくつかのバグがわかっている場合は、準備された機能テストを `tests/queries/bugs` ディレクトリ。 これらのテストは `tests/queries/0_stateless` バグが修正されたとき。

## 統合テスト {#integration-tests}

統合テストでは、クラスター化された構成でClickHouseをテストし、Mysql、Postgres、MongoDBなどの他のサーバーとClickHouseの相互作用をテストできます。 これらをエミュレートするネットワーク分割、パケットの落下など。 これらの試験する方向に作用しDockerを複数の容器を様々なソフトウェアです。

見る `tests/integration/README.md` これらのテストを実行する方法について。

この統合ClickHouse第三者によるドライバーではない。 また、現在、JDBCおよびODBCドライバとの統合テストはありません。

## 単体テスト {#unit-tests}

単体テストは、ClickHouse全体ではなく、単一の孤立したライブラリまたはクラスをテストする場合に便利です。 テストのビルドを有効または無効にするには `ENABLE_TESTS` CMakeオプション。 単体テスト(およびその他のテストプログラム)は `tests` コード全体のサブディレクトリ。 単体テストを実行するには、 `ninja test`. 一部のテストでは `gtest` しかし、いくつかは、テストの失敗でゼロ以外の終了コードを返すプログラムです。

コードがすでに機能テストでカバーされている場合は、必ずしも単体テストを持つとは限りません（機能テストは通常ははるかに簡単です）。

## 性能テスト {#performance-tests}

パフォーマ テストは `tests/performance`. それぞれの試験に代表される `.xml` テストケースの説明を持つファイル。 テストは以下で実行されます `clickhouse performance-test` ツール(埋め込まれている `clickhouse` バイナリ）。 見る `--help` 呼び出し用。

それぞれの試験実行または複数のクエリ(このパラメータの組み合わせ）のループ条件のための停止など “maximum execution speed is not changing in three seconds” 測定一部の指標につクエリの性能など “maximum execution speed”). いくつかの試験を含むことができ前提条件に予圧試験データを得る。

いくつかのシナリオでClickHouseのパフォーマンスを向上させたい場合や、単純なクエリで改善が見られる場合は、パフォーマンステストを作成することを強 いう意味があるのに使用 `perf top` またはあなたのテストの間の他のperf用具。

## テストツールとスクリプ {#test-tools-and-scripts}

一部のプログラム `tests` ディレク 例えば、 `Lexer` ツールがあります `src/Parsers/tests/lexer` それはstdinのトークン化を行い、色付けされた結果をstdoutに書き込みます。 これらの種類のツールは、コード例として、また探索と手動テストに使用できます。

でも一対のファイル `.sh` と `.reference` いくつかの事前定義された入力でそれを実行するためのツールと一緒に-その後、スクリプトの結果は `.reference` ファイル これらの種類のテストは自動化されていません。

## その他のテスト {#miscellaneous-tests}

外部辞書のテストは次の場所にあります `tests/external_dictionaries` そして機械学んだモデルのために `tests/external_models`. これらのテストは更新されず、統合テストに転送する必要があります。

クォーラム挿入には別のテストがあります。 このテストでは、ネットワーク分割、パケットドロップ（ClickHouseノード間、ClickHouseとZooKeeper間、ClickHouseサーバーとクライアント間など）など、さまざまな障害ケースをエミュレートします。), `kill -9`, `kill -STOP` と `kill -CONT` 例えば [ジェプセン](https://aphyr.com/tags/Jepsen). その後、試験チェックすべての認識を挿入したすべて拒否された挿入しました。

定足数を緩和試験の筆に別々のチーム前ClickHouseしたオープン達した. このチームはClickHouseでは動作しなくなりました。 テストは誤ってJavaで書かれました。 これらのことから、決議の定足数テストを書き換え及び移転統合。

## 手動テスト {#manual-testing}

新しい機能を開発するときは、手動でもテストするのが妥当です。 これを行うには、次の手順を実行します:

ClickHouseを構築します。 ターミナルからClickHouseを実行します。 `programs/clickhouse-server` そして、それを実行します `./clickhouse-server`. それは構成を使用します (`config.xml`, `users.xml` そして内のファイル `config.d` と `users.d` ディレクトリ)から、現在のディレクトリがデフォルトです。 ClickHouseサーバーに接続するには、以下を実行します `programs/clickhouse-client/clickhouse-client`.

これらのclickhouseツール（サーバ、クライアント、などだそうでsymlinks単一のバイナリ名 `clickhouse`. このバイナリは `programs/clickhouse`. すべてのツ `clickhouse tool` 代わりに `clickhouse-tool`.

またインストールすることができClickHouseパッケージは安定したリリースからのYandexリポジトリあるいはすることで作ることができるパッケージで `./release` ClickHouseソースルートで. 次に、サーバーを起動します `sudo service clickhouse-server start` (または停止してサーバーを停止します)。 ログを探す `/etc/clickhouse-server/clickhouse-server.log`.

時ClickHouseでに既にインストールされているシステムを構築できる新しい `clickhouse` 既存のバイナリを置き換えます:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

また、システムclickhouse-serverを停止し、同じ構成ではなく端末にログインして独自のものを実行することもできます:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Gdbの例:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

システムclickhouse-serverがすでに実行されていて、それを停止したくない場合は、次のポート番号を変更できます `config.xml` （または、ファイル内でそれらを上書きする `config.d` ディレクトリ）、適切なデータパスを提供し、それを実行します。

`clickhouse` バイナリーはほとんどない依存関係の作品を広い範囲のLinuxディストリビューション. サーバー上で変更を迅速かつ汚いテストするには、次のことができます `scp` あなたの新鮮な構築 `clickhouse` あなたのサーバーにバイナリし、上記の例のように実行します。

## テスト環境 {#testing-environment}

リリースを安定版として公開する前に、テスト環境に展開します。 テスト環境は1/39の部分を処理する集りです [Yandex.メトリカ](https://metrica.yandex.com/) データ テスト環境をYandexと共有しています。メトリカ-チーム ClickHouseは既存のデータの上にダウンタイムなしで改善される。 私たちは、データがリアルタイムから遅れることなく正常に処理され、複製が動作し続け、Yandexに見える問題はないことを最初に見ています。メトリカ-チーム 最初のチェックは、次の方法で行うことができます:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

市場、クラウドなど：いくつかのケースでは、我々はまた、Yandexの中で私たちの友人チームのテスト環境に展開します また、開発目的で使用されるハードウェアサーバーもあります。

## 負荷テスト {#load-testing}

後の展開を試験環境を実行負荷テストクエリから生産ます。 これは手動で行われます。

有効にしていることを確認します `query_log` 運用クラスター上。

一日以上のクエリログを収集する:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

これは複雑な例です。 `type = 2` 正常に実行されたクエリをフィルタ処理します。 `query LIKE '%ym:%'` Yandexから関連するクエリを選択することです。メトリカ `is_initial_query` ClickHouse自体ではなく、クライアントによって開始されたクエリのみを選択することです（分散クエリ処理の一部として）。

`scp` このログをテストクラスタに記録し、次のように実行します:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

（おそらくあなたはまた、 `--user`)

それから夜または週末のためにそれを残し、残りを取る行きなさい。

きることを確認 `clickhouse-server` なクラッシュメモリのフットプリントは有界性なつ品位を傷つける。

クエリと環境の変動が大きいため、正確なクエリ実行タイミングは記録されず、比較されません。

## ビルドテスト {#build-tests}

構築を試験できることを確認の構築においても様々な代替構成されており、外国のシステム。 テストは `ci` ディレクトリ。 Docker、Vagrant、時には以下のようなソースからビルドを実行します `qemu-user-static` ドッカー内部。 これらのテストは開発中であり、テストの実行は自動化されません。

動機:

通常、ClickHouse buildの単一のバリアントですべてのテストをリリースして実行します。 しかし、徹底的にテストされていない別のビルド変種があります。 例:

-   FreeBSD上でビルド;
-   をDebianを対象として図書館システムのパッケージ;
-   ライブラリの共有リンクでビルド;
-   AArch64プラットフォ;
-   PowerPcプラットフォーム上で構築。

たとえば、システムパッケージを使用したビルドは悪い習慣です。 しかし、これは本当にDebianメンテナに必要です。 このため、少なくともこのビルドの変種をサポートする必要があります。 別の例：共有リンクは一般的な問題の原因ですが、一部の愛好家にとって必要です。

ができませんので実行した全試験はすべての変異体を構築し、チェックしたい少なくとも上記に記載された各種の構築異な破となりました。 この目的のためにビルドテストを使用します。

## プロトコル互換性のテスト {#testing-for-protocol-compatibility}

ClickHouse network protocolを拡張すると、古いclickhouse-clientが新しいclickhouse-serverで動作し、新しいclickhouse-clientが古いclickhouse-serverで動作することを手動でテストします（対応するパッケージからバイナリを

## コンパイラからのヘルプ {#help-from-the-compiler}

メインクリックハウスコード（にある `dbms` ディレクトリ)は `-Wall -Wextra -Werror` そして、いくつかの追加の有効な警告と。 これらのオプションは有効になっていないためにサードパーティーのライブラリ.

Clangにはさらに便利な警告があります。 `-Weverything` デフォルトのビルドに何かを選ぶ。

本番ビルドでは、gccが使用されます（clangよりもやや効率的なコードが生成されます）。 開発のために、clangは通常、使用する方が便利です。 あなたは（あなたのラップトップのバッテリーを節約するために）デバッグモードで自分のマシン上で構築することができますが、コンパイラがでより `-O3` よりよい制御フローおよびinter-procedure分析が原因で。 Clangでビルドする場合, `libc++` の代わりに使用されます。 `libstdc++` そして、デバッグモードでビルドするとき、 `libc++` 使用可能にするにはより誤差があります。.

## サニタイザー {#sanitizers}

**アドレスsanitizer**.
私たちは、コミットごとにASanの下で機能テストと統合テストを実行します。

**ヴァルグリンド(曖昧さ回避)**.
私たちは一晩Valgrindの下で機能テストを実行します。 数時間かかります。 現在知られている偽陽性があります `re2` 図書館、参照 [この記事](https://research.swtch.com/sparse).

**未定義の動作のサニタイザー。**
私たちは、コミットごとにASanの下で機能テストと統合テストを実行します。

**糸のsanitizer**.
私たちは、コミットごとにTSanの下で機能テストを実行します。 コミットごとにTSanの下で統合テストを実行することはまだありません。

**メモリサニタイザー**.
現在、我々はまだMSanを使用していません。

**デバッグアロケータ。**
デバッグバージョン `jemalloc` デバッグビルドに使用されます。

## ファジング {#fuzzing}

ClickHouseファジングは、両方を使用して実装されます [libFuzzer](https://llvm.org/docs/LibFuzzer.html) とランダムSQLクエリ。
すべてのファズテストは、サニタイザー（アドレスと未定義）で実行する必要があります。

LibFuzzerは、ライブラリコードの分離ファズテストに使用されます。 ファザーはテストコードの一部として実装され “\_fuzzer” 名前の接尾辞。
Fuzzerの例はで見つけることができます `src/Parsers/tests/lexer_fuzzer.cpp`. LibFuzzer固有の設定、辞書、およびコーパスは次の場所に格納されます `tests/fuzz`.
ご協力をお願いいたし書きファズ試験べての機能を取り扱うユーザー入力します。

ファザーはデフォルトではビルドされません。 両方のファザーを構築するには `-DENABLE_FUZZING=1` と `-DENABLE_TESTS=1` 選択は置かれるべきである。
ファザーのビルド中にJemallocを無効にすることをお勧めします。 ClickHouseファジングを統合するために使用される設定
Google OSS-Fuzzは次の場所にあります `docker/fuzz`.

また簡単なファズ試験をランダムなSQLクエリーやことを確認するにはサーバーにな金型を実行します。
それを見つけることができる `00746_sql_fuzzy.pl`. このテストは、継続的に実行する必要があります（一晩と長い）。

## セキュリティ監査 {#security-audit}

人からのYandexセキュリティチームはいくつかの基本的な概要ClickHouse力からのセキュリティの観点から.

## 静的アナライザ {#static-analyzers}

私たちは走る `PVS-Studio` コミットごと。 私達は評価しました `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`. 使用のための指示をで見つけます `tests/instructions/` ディレクトリ。 また読むことができます [ロシア語の記事](https://habr.com/company/yandex/blog/342018/).

を使用する場合 `CLion` IDEとして、いくつかを活用できます `clang-tidy` 箱から出してチェックします。

## 硬化 {#hardening}

`FORTIFY_SOURCE` デフォルトで使用されます。 それはほとんど役に立たないですが、まれに理にかなっており、それを無効にしません。

## コードスタイル {#code-style}

コードのスタイルのルールを記述 [ここに](https://clickhouse.tech/docs/en/development/style/).

チェックのための、共通したスタイル違反、利用できる `utils/check-style` スクリプト

コードの適切なスタイルを強制するには、次のようにします `clang-format`. ファイル `.clang-format` ソースルートにあります。 実際のコードスタイルにほとんど対応しています。 しかし、適用することはお勧めしません `clang-format` 既存のファイルへの書式設定が悪化するためです。 以下を使用できます `clang-format-diff` clangソースリポジトリで見つけることができるツール。

あるいは、 `uncrustify` コードを再フォーマットするツール。 設定は次のとおりです `uncrustify.cfg` ソースルートで。 それはより少なくテストさ `clang-format`.

`CLion` 独自のコードをフォーマッタしていると見ることができる調整のためのコードです。

## Metrica B2Bテスト {#metrica-b2b-tests}

各ClickHouseリリースはYandex MetricaとAppMetricaエンジンでテストされます。 ClickHouseのテスト版と安定版はVmにデプロイされ、入力データの固定サンプルを処理するMetrica engineの小さなコピーで実行されます。 次に，Ｍｅｔｒｉｃａエンジンの二つのインスタンスの結果を比較した。

これらの試験により自動化されており、別のチームです。 可動部分の高い数が原因で、テストは把握し非常ににくい完全に無関係な理由によって失敗ほとんどの時間です。 がこれらの試験は負の値です。 しかしこれらの試験することが明らかとなったが有用である一又は二倍の数百名

## テスト範囲 {#test-coverage}

2018年現在、テストカバーは行っていない。

## テスト自動化 {#test-automation}

Yandex内部CIとジョブ自動化システムという名前のテストを実行します “Sandbox”.

ビルドジョブとテストは、コミットごとにSandboxで実行されます。 結果のパッケージとテスト結果はGitHubに公開され、直接リンクでダウンロードできます。 成果物は永遠に保存されます。 GitHubでプルリクエストを送信すると、次のようにタグ付けします “can be tested” そして私達のCIシステムはあなたのためのClickHouseのパッケージ（住所sanitizerの解放、デバッグ、等）を造ります。

時間と計算能力の限界のため、Travis CIは使用しません。
ジェンキンスは使わない 以前は使用されていましたが、今はJenkinsを使用していません。

[元の記事](https://clickhouse.tech/docs/en/development/tests/) <!--hide-->
