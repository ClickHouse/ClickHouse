---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 69
toc_title: "ClickHouse\u30C6\u30B9\u30C8\u3092\u5B9F\u884C\u3059\u308B\u65B9\u6CD5"
---

# ClickHouse試験 {#clickhouse-testing}

## 機能テスト {#functional-tests}

機能テストは最も簡単で使いやすいものです。 clickhouseの機能のほとんどは、機能テストでテストすることができ、彼らはそのようにテストすることができclickhouseコード内のすべての変更のために使用する

各機能テストは、実行中のclickhouseサーバーに一つまたは複数のクエリを送信し、参照と結果を比較します。

テストは `queries` ディレクトリ。 つのサブディレクトリがあります: `stateless` と `stateful`. ステートレステストでは、プリロードされたテストデータを使用せずにクエリを実行します。 ステートフルテストでは、Yandexのテストデータが必要です。メトリカと一般市民には利用できません。 我々は唯一の使用する傾向があります `stateless` テストと新しい追加を避ける `stateful` テスト

それぞれの試験できるの種類: `.sql` と `.sh`. `.sql` testは、パイプ処理される単純なSQLスクリプトです `clickhouse-client --multiquery --testmode`. `.sh` テストは、単独で実行されるスクリプトです。

すべてのテストを実行するには、 `clickhouse-test` ツール。 見て！ `--help` 可能なオプションのリストについて。 できるだけ実行すべての試験または実行のサブセットの試験フィルター部分文字列の試験名: `./clickhouse-test substring`.

機能テストを呼び出す最も簡単な方法は、コピーすることです `clickhouse-client` に `/usr/bin/`、実行 `clickhouse-server` そして、実行 `./clickhouse-test` 独自のディレクトリから。

新しいテストを追加するには、 `.sql` または `.sh` ファイル `queries/0_stateless` ディレクトリは、手動でチェックしてから生成 `.reference` 次の方法でファイル: `clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` または `./00000_test.sh > ./00000_test.reference`.

テストでは、（create、dropなど）テーブルのみを使用する必要があります `test` テストでは一時テーブルを使用することもできます。

機能テストで分散クエリを使用する場合は、次のようにします `remote` テーブル機能との `127.0.0.{1..2}` または、サーバー構成ファイル内の事前定義されたテストクラスターを次のように使用できます `test_shard_localhost`.

いくつかのテストは `zookeeper`, `shard` または `long` 彼らの名前で。
`zookeeper` ZooKeeperを使用しているテストのためのものです。 `shard` そのテストのためです
サーバーのリッスンが必要 `127.0.0.*`; `distributed` または `global` 同じを持っている
意味は… `long` 少し長く走るテストのためのものです。 あなたはできる
次のテストグループを無効にする `--no-zookeeper`, `--no-shard` と
`--no-long` オプション、それぞれ。

## 既知のバグ {#known-bugs}

機能テストで簡単に再現できるいくつかのバグを知っていれば、準備された機能テストを `tests/queries/bugs` ディレクトリ。 これらのテストはに移動されます `tests/queries/0_stateless` バグが修正されたとき。

## 統合テスト {#integration-tests}

統合テストでは、クラスター化された設定でclickhouseをテストし、mysql、postgres、mongodbのような他のサーバーとのclickhouseの相互作用を可能にします。 それらはネットワークの割れ目、包みの低下、等を競争して有用である。 これらの試験する方向に作用しdockerを複数の容器を様々なソフトウェアです。

見る `tests/integration/README.md` これらのテストを実行する方法について。

ClickHouseとサードパーティドライバの統合はテストされていません。 また、現在、JDBCおよびODBCドライバとの統合テストはありません。

## 単体テスト {#unit-tests}

単体テストは、clickhouse全体ではなく、単一の孤立したライブラリまたはクラスをテストする場合に便利です。 テストのビルドを有効または無効にするには `ENABLE_TESTS` CMakeオプション。 単体テスト（およびその他のテストプログラム）は、 `tests` コード全体のサブディレクトリ。 単体テストを実行するには `ninja test`. いくつかのテストは `gtest` しかし、テストの失敗でゼロ以外の終了コードを返すプログラムだけです。

コードがすでに機能テストでカバーされている場合は、単体テストを行う必要はありません（機能テストは通常ははるかに簡単に使用できます）。

## 性能テスト {#performance-tests}

性能試験を測定して比較の一部の縁の一部clickhouse合成ます。 試験は `tests/performance`. 各テストは `.xml` テストケースの説明を含むファイル。 テストは以下で実行されます `clickhouse performance-test` ツール（埋め込まれていること `clickhouse` バイナリ）。 見る `--help` 呼び出しのため。

各試験の実行はmiltiple索の可能性のある組み合わせのパラメータ)のループ条件のための停止など “maximum execution speed is not changing in three seconds” 測定一部の指標につクエリの性能など “maximum execution speed”). いくつかの試験を含むことができ前提条件に予圧試験データを得る。

いくつかのシナリオでclickhouseのパフォーマンスを向上させたい場合や、単純なクエリで改善が見られる場合は、パフォーマンステストを作成することを強 それは常に使用する意味があります `perf top` またはあなたのテスト中に他のperfツール。

## テストツール、スクリプト {#test-tools-and-scripts}

の一部のプログラム `tests` directoryは準備されたテストではなく、テストツールです。 たとえば、 `Lexer` ツールがあります `dbms/Parsers/tests/lexer` これはstdinのトークン化を行い、結果をstdoutに色付けします。 これらの種類のツールをコード例として、また調査と手動テストに使用できます。

でも一対のファイル `.sh` と `.reference` のツールであるかの定義済みの入力-その後スクリプトの結果と比較することができ `.reference` ファイル。 この種のテストは自動化されていません。

## Miscellanous試験 {#miscellanous-tests}

外部辞書のテストは次の場所にあります `tests/external_dictionaries` そして機械学ばれたモデルのために `tests/external_models`. これらのテストは更新されず、統合テストに転送する必要があります。

定足数の挿入には個別のテストがあります。 ネットワーク分割、パケットドロップ（clickhouseノード間、clickhouseとzookeeper間、clickhouseサーバーとクライアント間など）など、さまざまな障害ケースをエミュレートします。), `kill -9`, `kill -STOP` と `kill -CONT` 、のように [Jepsen](https://aphyr.com/tags/Jepsen). その後、試験チェックすべての認識を挿入したすべて拒否された挿入しました。

定足数を緩和試験の筆に別々のチーム前clickhouseしたオープン達した. このチームは、もはやclickhouseで動作しません。 テストはaccidentially javaで書かれました。 これらのことから、決議の定足数テストを書き換え及び移転統合。

## 手動テスト {#manual-testing}

新しい機能を開発するときは、手動でテストすることも合理的です。 次の手順で行うことができます:

ClickHouseをビルドします。 ターミナルからClickHouseを実行します。 `programs/clickhouse-server` そして、それを実行します `./clickhouse-server`. それは構成を使用します (`config.xml`, `users.xml` と内のファイル `config.d` と `users.d` ディレクトリ)から、現在のディレクトリがデフォルトです。 ClickHouseサーバーに接続するには、以下を実行します `programs/clickhouse-client/clickhouse-client`.

これらのclickhouseツール（サーバ、クライアント、などだそうでsymlinks単一のバイナリ名 `clickhouse`. このバイナリは次の場所にあります `programs/clickhouse`. すべてのツ `clickhouse tool` 代わりに `clickhouse-tool`.

または、yandexリポジトリからの安定したリリースか、あなた自身のためのパッケージを構築することができます `./release` ClickHouseのソースのルートで. 次に、 `sudo service clickhouse-server start` （またはサーバーを停止するために停止）。 でログを探します `/etc/clickhouse-server/clickhouse-server.log`.

ClickHouseが既にシステムにインストールされている場合は、新しい `clickhouse` バイナリと既存のバイナリを交換:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

また、システムclickhouse-serverを停止し、同じ設定でターミナルにログインして独自に実行することもできます:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Gdbの例:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

システムクリックハウスサーバーが既に実行されていて、それを停止したくない場合は、ポート番号を変更することができます `config.xml` （またはファイル内でそれらを上書きする `config.d` ディレクトリ)を指定し、適切なデータパスを指定して実行します。

`clickhouse` バイナリーはほとんどない依存関係の作品を広い範囲のLinuxディストリビューション. サーバー上の変更をすばやく汚れてテストするには、次のようにします `scp` あなたの新鮮な内蔵 `clickhouse` サーバーへのバイナリを作成し、上記の例のように実行します。

## テスト環境 {#testing-environment}

安定版としてリリースを公開する前に、テスト環境に展開します。 テスト環境は1/39の部分を処理するクラスタです [Yandexの。Metrica](https://metrica.yandex.com/) データ。 テスト環境をYandexと共有します。メトリカチーム。 ﾂづﾂつｿﾂづｫﾂづｱﾂ鳴ｳﾂ猟ｿﾂづﾂつｷﾂ。 まずデータを処理しなが遅れから、オシロスコープのリアルタイムレプリケーションの継続作業とな問題に見えるYandex.メトリカチーム。 最初のチェックは次の方法で行うことができます:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

場合によっては、yandex：market、cloudなどの友人チームのテスト環境にも展開します。 また、開発目的で使用されるハードウェアサーバーもあります。

## 負荷テスト {#load-testing}

テスト環境に展開した後、本番クラスターからのクエリで負荷テストを実行します。 これは手動で行われます。

有効にしていることを確認します `query_log` あなたの生産の集りで。

一日以上のクエリログを収集する:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

これは複雑な例です。 `type = 2` 正常に実行されたクエリをフィルタ処理します。 `query LIKE '%ym:%'` Yandexのから関連するクエリを選択することです。メトリカ `is_initial_query` ClickHouse自体ではなく、クライアントによって開始されたクエリのみを選択することです（分散クエリ処理の一部として）。

`scp` このログを試験クラスターとして以下の:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

（おそらく、あなたはまた、指定したいです `--user`)

それから夜か週末の間それを残し、取得残りを行きなさい。

きることを確認 `clickhouse-server` なクラッシュメモリのフットプリントは有界性なつ品位を傷つける。

正確なクエリ実行タイミングは記録されず、クエリと環境の変動が大きいため比較されません。

## ビルドテスト {#build-tests}

構築を試験できることを確認の構築においても様々な代替構成されており、外国のシステム。 試験は `ci` ディレクトリ。 彼らはDocker、Vagrantの中のソースからビルドを実行し、時には `qemu-user-static` ドッカー内部。 これらのテストは開発中であり、テスト実行は自動化されていません。

動機づけ:

通常、clickhouseビルドの単一のバリアントですべてのテストをリリースして実行します。 しかし、徹底的にテストされていない代替ビルドの変種があります。 例:

-   FreeBSD上でのビルド;
-   システムパッケージのライブ;
-   ライブラリの共有リンク付きビルド;
-   AArch64プラットフォーム上に構築;
-   PowerPcプラットフォーム上に構築。

たとえば、構築システムのパッケージが悪い練習ができませんので保証ものに版のパッケージシステムです。 しかし、これは本当にdebianのメンテナに必要です。 このため、少なくともこのビルドの変種をサポートする必要があります。 別の例：共有リンクは一般的なトラブルの原因ですが、一部の愛好家には必要です。

ができませんので実行した全試験はすべての変異体を構築し、チェックしたい少なくとも上記に記載された各種の構築異な破となりました。 この目的のためにビルドテストを使用します。

## プロトコル互換性のテスト {#testing-for-protocol-compatibility}

我々はclickhouseのネットワークプロトコルを拡張するとき,我々は、古いclickhouse-クライアントが新しいclickhouse-serverで動作し、新しいclickhouse-clientが古いclickhouse-serverで動作することを手動で

## コンパイラからの助け {#help-from-the-compiler}

メインクリックハウスコード `dbms` ディレクトリ)は `-Wall -Wextra -Werror` そして、いくつかの追加の有効な警告と。 これらのオプションは有効になっていないためにサードパーティーのライブラリ.

Clangにはさらに便利な警告があります。 `-Weverything` デフォルトのビルドに何かを選ぶ。

プロダクションビルドでは、gccが使用されます（clangよりもやや効率的なコードが生成されます）。 開発のために、clangは通常使用するのがより便利です。 デバッグモードで自分のマシン上に構築することができます（ラップトップのバッテリーを節約するため）が、コンパイラはより多くの警告を生成する `-O3` よりよい制御流れおよび相互プロシージャの分析が原因で。 Clangでビルドするとき, `libc++` の代わりに使用される。 `libstdc++` そして、デバッグモードでビルドするときは、 `libc++` 使用可能にするにはより誤差があります。.

## 消毒剤 {#sanitizers}

**アドレス消毒剤**.
ASanの下でコミットごとに機能テストと統合テストを実行します。

**Valgrind(Memcheck)**.
私たちは一晩valgrindの下で機能テストを実行します。 それは複数の時間がかかります。 現在、既知の偽陽性があります `re2` ライブラリ、参照 [この記事](https://research.swtch.com/sparse).

**未定義の動作消毒剤。**
ASanの下でコミットごとに機能テストと統合テストを実行します。

**スレッド消毒剤**.
TSanの下でコミットごとに機能テストを実行します。 TSanの下では、コミットごとに統合テストは実行されません。

**メモリ消毒剤**.
現在、我々はまだmsanを使用していません。

**デバッグアロケータ。**
デバッグバージョン `jemalloc` デバッグビルドに使用されます。

## Fuzzing {#fuzzing}

単純なfuzzテストを使用して、ランダムなsqlクエリを生成し、サーバーが死んでいないことを確認します。 ファジーテストはアドレスサニタイザーで実行されます。 あなたはそれを見つける `00746_sql_fuzzy.pl`. このテストは継続的に実行する必要があります（夜間および長期）。

December2018の時点では、ライブラリコードの孤立したファズテストはまだ使用していません。

## セキュリティ監査 {#security-audit}

Yandexのクラウド部門の人々は、セキュリティの観点からClickHouse機能のいくつかの基本的な概要を行います。

## 静的分析器 {#static-analyzers}

私たちは走る `PVS-Studio` コミットごとに。 我々は評価した `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`. あなたは、使用中の使用方法を見つけるでしょう `tests/instructions/` ディレクトリ。 また読むことができます [ロシア語の記事](https://habr.com/company/yandex/blog/342018/).

使用する場合 `CLion` IDEとして、次のいくつかを活用できます `clang-tidy` 箱からの点検。

## 硬化 {#hardening}

`FORTIFY_SOURCE` デフォルトで使用されます。 それはほとんど役に立たないですが、まれに意味があり、私たちはそれを無効にしません。

## コードスタイル {#code-style}

コードのスタイルのルールを記述 [ここに](https://clickhouse.tech/docs/en/development/style/).

一般的なスタイル違反を確認するには、次のようにします `utils/check-style` スクリプト

コードの適切なスタイルを強制するには、次のようにします `clang-format`. ファイル `.clang-format` ソースのルートにあります。 主に実際のコードスタイルに対応しています。 しかし、適用することはお勧めしません `clang-format` 既存のファイルには、書式設定が悪化するためです。 を使用することができ `clang-format-diff` clangソースリポジトリにあるツールです。

あるいは、 `uncrustify` コードを再フォーマットするツール。 設定は `uncrustify.cfg` ソースのルートで。 での試験によ `clang-format`.

`CLion` 独自のコードをフォーマッタしていると見ることができる調整のためのコードです。

## メトリカb2bテスト {#metrica-b2b-tests}

各clickhouseのリリースは、yandexのメトリカとappmetricaエンジンでテストされています。 クリックハウスのテストおよび安定版は、vm上に配備され、入力データの固定サンプルを処理しているmetricaエンジンの小さなコピーで実行されます。 次に，メトリカエンジンの二つのインスタンスの結果を共に比較した。

これらの試験により自動化されており、別のチームです。 可動部品の数が多いため、テストはほとんどの場合完全に無関係な理由で失敗します。 がこれらの試験は負の値です。 しかしこれらの試験することが明らかとなったが有用である一又は二倍の数百名

## テスト範囲 {#test-coverage}

July2018の時点で、テストカバレッジは追跡されません。

## テストの自動化 {#test-automation}

また試験のyandex内ciと雇用自動化システムの名前 “Sandbox”.

ビルドジョブとテストは、コミットごとにsandboxで実行されます。 結果のパッケージとテスト結果はgithubに公開され、直接リンクでダウンロードできます。 成果物は永遠に保存されます。 githubでpullリクエストを送信すると、次のようにタグ付けします “can be tested” そして私達のCIシステムはあなたのためのClickHouseのパッケージを造ります（解放、住所のsanitizer、等と、デバッグします）。

私たちは、時間と計算能力の限界のためにtravis ciを使用しません。
ジェンキンスは使わない で使用される前に、現しました嬉しい使用していないjenkins.

[元の記事](https://clickhouse.tech/docs/en/development/tests/) <!--hide-->
ベロップメント/テスト/) <!--hide-->
