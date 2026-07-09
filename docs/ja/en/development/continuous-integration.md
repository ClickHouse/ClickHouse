---
description: 'ClickHouse の継続的インテグレーションシステムの概要'
sidebar_label: '継続的インテグレーション (CI)'
sidebar_position: 55
slug: /development/continuous-integration
title: '継続的インテグレーション (CI)'
doc_type: 'reference'
---

プルリクエストを送信すると、ClickHouse の[継続的インテグレーション (CI) システム](tests.md#test-automation)によって、コードに対していくつかの自動チェックが実行されます。
これは、リポジトリのメンテナー (ClickHouse チームの担当者) がコードを確認し、プルリクエストに `can be tested` ラベルを追加したあとに行われます。
チェックの結果は、[GitHub のチェックに関するドキュメント](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks)で説明されているとおり、GitHub のプルリクエストページに表示されます。
チェックが失敗した場合は、修正が必要になることがあります。
このページでは、遭遇する可能性のあるチェックの概要と、それらを修正するためにできることを説明します。

チェックの失敗が自分の変更と無関係に見える場合は、一時的な障害か、インフラストラクチャの問題である可能性があります。
プルリクエストに空コミットをプッシュして、CI チェックを再実行してください:

```shell
git commit --allow-empty
git push
```

何をすればよいかわからない場合は、メンテナーに相談してください。

<div id="merge-with-master">
  ## master とのマージ
</div>

PR を master にマージできることを確認します。
できない場合は、`Cannot fetch mergecommit` というメッセージが表示されて失敗します。
このチェックを解消するには、[GitHub のドキュメント](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github) の説明に従って競合を解決するか、git を使って `master` ブランチを自分のプルリクエストのブランチにマージしてください。

<div id="docs-check">
  ## Docs チェック
</div>

ClickHouse のドキュメントサイトのビルドを試行します。
ドキュメントに変更を加えた場合、失敗することがあります。
最も可能性が高い原因は、ドキュメント内のクロスリンクのいずれかが誤っていることです。
チェックレポートを開き、`ERROR` と `WARNING` のメッセージを確認してください。

<div id="description-check">
  ## 説明の確認
</div>

プルリクエストの説明がテンプレート [PULL&#95;REQUEST&#95;TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md) に準拠していることを確認してください。
変更に対応する変更履歴のカテゴリ (例: バグ修正) を指定し、[CHANGELOG.md](../whats-new/changelog/index.md) 向けに、その変更内容を説明するユーザー向けのメッセージを記述する必要があります

<div id="docker-image">
  ## Docker イメージ
</div>

ClickHouse server と Keeper の Docker イメージをビルドし、正常にビルドできることを確認します。

<div id="official-docker-library-tests">
  ### 公式 Docker ライブラリのテスト
</div>

[公式 Docker ライブラリ](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files)のテストを実行し、`clickhouse/clickhouse-server` Docker イメージが正しく動作することを検証します。

新しいテストを追加するには、ディレクトリ `ci/jobs/scripts/docker_server/tests/$test_name` を作成し、その中にスクリプト `run.sh` を配置します。

テストの詳細については、[CI jobs scripts documentation](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server)を参照してください。

<div id="marker-check">
  ## Marker チェック
</div>

このチェックは、CI システムがプルリクエストの処理を開始したことを示します。
ステータスが &#39;pending&#39; の場合は、まだすべてのチェックが開始されていないことを示します。
すべてのチェックが開始されると、ステータスは &#39;success&#39; に変わります。

<div id="style-check">
  ## スタイルチェック
</div>

コードベースに対して各種のスタイルチェックを実行します。以下の各サブチェックは、それぞれ [`ci/jobs/check_style.py`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/check_style.py) 内の `testname` に対応しており、`--test <name>` を使用して個別に実行できます (下記参照) 。

<div id="cpp">
  ##### cpp
</div>

[`check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh) による、正規表現ベースの C++ スタイルチェックです。失敗した場合は、[コードスタイルガイド](style.md) に従って問題を修正してください。

<div id="whitespace-check">
  ##### whitespace_check
</div>

カラムの位置揃えの一部ではない、C++ におけるコンマ後の二重スペースを検出します。

<div id="catch-all">
  ##### catch_all
</div>

不明な例外を握りつぶすのは安全ではないため、デストラクタ、`main`、および fuzzer のエントリポイントを除き、`catch (...)` の使用を禁止します。

<div id="yamllint">
  ##### yamllint
</div>

`.yamllint` を使用して、`.github/` 配下の YAML ワークフローファイルに対して lint を実行します。

<div id="xmllint">
  ##### xmllint
</div>

`tests/` と `programs/` 配下の XML ファイルを検証します。

<div id="functional-tests-check">
  ##### functional_tests_check
</div>

stateless tests を確認します。`event_date` で絞り込むクエリでは、`today()` ではなく `>= yesterday()` を使用する必要があります (深夜前後の不安定さを避けるため) 。また、テストファイル名に `fail` を含めてはなりません。

<div id="test-numbers-check">
  ##### test_numbers_check
</div>

stateless テストの番号 (`tests/queries/0_stateless/<NNNNN>_*`) にある大きなギャップを検出します。

<div id="symlinks">
  ##### シンボリックリンク
</div>

リポジトリ内の壊れたシンボリックリンクを検出します。

<div id="various">
  ##### 各種
</div>

[`various_checks.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/various_checks.sh) による各種リポジトリチェック: `system.query_log` / `system.parts` / などに対するクエリは `currentDatabase` でフィルタする必要があり、`Replicated*MergeTree` の ZooKeeper パスにはテストごとのプレフィックスを含める必要があり、インテグレーションテストのディレクトリには `__init__.py` が必要であり、UTF BOM は不可、ソース/データファイルに実行可能ビットを付けるのは不可、サードパーティの docker-compose イメージに `:latest` タグを使うのは不可、などがあります。

<div id="running-style-check-locally">
  ### スタイルチェックジョブをローカルで実行する
</div>

*スタイルチェック*ジョブ全体は、Dockerコンテナ内で次のようにローカルで実行できます。

```sh
python -m ci.praktika run "Style check"
```

特定のチェック (例: *cpp* チェック) を実行するには:

```sh
python -m ci.praktika run "Style check" --test cpp
```

これらのコマンドは `clickhouse/style-test` Docker イメージ を取得し、コンテナー化された環境でジョブを実行します。
必要なのは Python 3 と Docker のみで、その他の依存関係はありません。

<div id="running-stateless-tests">
  ## stateless テストの実行
</div>

デフォルト設定でローカルにインストールした ClickHouse は、特定のテストケースでは動作することがありますが、すべてのテストクエリを正しく実行できるわけではありません。CI では、各ジョブごとに特定の ClickHouse 構成 (例: S3 ストレージ、並列レプリカ) をセットアップするため、これを手作業で再現するのは煩雑になりがちです。これを避けるには、CI と同じオーケストレーションを使って任意の CI ジョブをローカルで再現できます。手動で設定する必要はありません。

<div id="ci-prerequisites">
  #### 前提条件
</div>

* Python 3 (標準ライブラリのみ)
* Docker

必要に応じて Ubuntu に Docker をインストールし、再ログインしてください:

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

<div id="run-ci-job-locally">
  #### ローカルでCIジョブを実行する
</div>

CI レポートから任意のジョブ名を選択し、ローカルで実行します。

```bash
python -m ci.praktika run "<JOB_NAME>"
```

* ジョブ名は、CI レポートに表示されている表記を必ずそのまま引用してください (スペースやカンマが含まれる場合があります) 。例: `"Stateless tests (amd_debug, parallel)"`。これにより、CI と同じ ClickHouse の構成が設定され、同じテストが実行されます。
* ジョブ名に含まれるアーキテクチャとビルド種別 (例: `amd_debug`) は、CI 固有のラベルです。ローカルで実行する場合、これらは影響しません。ジョブでは、指定したバイナリと実行中のアーキテクチャがそのまま使われます。ジョブ名によって決まるのは、ClickHouse の構成とテストセットだけです (`--test` で上書きしない限り) 。
* CI では、リソース使用状況を改善するため、機能テストはバッチに分割されています。たとえば、`"Stateless tests (amd_debug, parallel)"` と `"Stateless tests (amd_debug, sequential)"` を合わせると、対象範囲全体をカバーできます。並列実行しても安全なテストは同時実行され、それ以外は順次実行されます。この分割により、可能な限り並列度を高めて CI 全体の所要時間を短縮できます。ローカルでテスト範囲全体を再現するには、両方のバッチを実行してください。
* `"Fast test"` という CI ジョブもあり、ClickHouse の基本機能を確認するために、限定された範囲の機能テストを実行します。これはすべてのオプションモジュールを含まないビルドを使用し、回帰をすばやく検出する最も手軽な方法です。ローカルでも同じ方法で実行できます。ClickHouse バイナリを既定の検索パスのいずれか (`./ci/tmp/clickhouse`、`./build/programs/clickhouse`、または `./clickhouse`) に配置してください。そうしないと、ジョブは最初に ClickHouse のビルドを試みます。
  ```bash
  python -m ci.praktika run "Fast test"
  ```

<div id="run-specific-tests-within-ci-job">
  #### CIジョブ内で特定のテストを実行する
</div>

`--test` を使用すると、CI で使われているものと同じ ClickHouse のセットアップを準備し、選択したテストのみを実行します。

```bash
python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
  --test 00001_select1
```

* 複数のテスト名を指定できます。
  ```bash
  python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
    --test 00001_select1 00002_log_and_exception_messages_formatting
  ```
* ヒント: ClickHouse のどの設定でも問題なく、特定のテストだけを実行したい場合は、完全な job 名の代わりにエイリアス `functional` を使用してください。
  ```bash
  python -m ci.praktika run functional --test 00001_select1
  ```

<div id="additional-customization-options">
  #### その他のカスタマイズオプション
</div>

* `--path PATH` — ClickHouse 実行ファイルへのカスタムパス。デフォルトでは、ランナーは `./ci/tmp/clickhouse`、`./build/programs/clickhouse`、`./clickhouse` の順に検索します。
* `--count N` — 各テストを N 回繰り返します。
* `--workers N` — マシンの性能に基づいて自動計算される並列ワーカー数を上書きします。

<div id="build-check">
  ## ビルドチェック
</div>

以降の手順で使用するため、さまざまな構成でClickHouseをビルドします。

<div id="running-builds-locally">
  ### ローカルでビルドを実行する
</div>

ビルドは、次のコマンドを使用してCIに近い環境でローカルに実行できます。

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

Python 3 と Docker 以外に必要な依存関係はありません。

<div id="available-build-jobs">
  #### 利用可能なビルドジョブ
</div>

ビルドジョブ名は、CI Report に表示される名前と完全に一致しています。

**AMD64 ビルド:**

* `Build (amd_debug)` - シンボル付きの Debug ビルド
* `Build (amd_release)` - 最適化された release ビルド
* `Build (amd_asan)` - Address Sanitizer ビルド
* `Build (amd_tsan)` - Thread Sanitizer ビルド
* `Build (amd_msan)` - Memory Sanitizer ビルド
* `Build (amd_ubsan)` - Undefined Behavior Sanitizer ビルド
* `Build (amd_binary)` - Thin LTO なしの高速な release ビルド
* `Build (amd_compat)` - 旧式システム向けの互換ビルド
* `Build (amd_musl)` - musl libc を使用したビルド
* `Build (amd_darwin)` - macOS ビルド
* `Build (amd_freebsd)` - FreeBSD ビルド

**ARM64 ビルド:**

* `Build (arm_release)` - ARM64 向けに最適化された release ビルド
* `Build (arm_asan)` - ARM64 Address Sanitizer ビルド
* `Build (arm_coverage)` - カバレッジ用のインストルメンテーションを含む ARM64 ビルド
* `Build (arm_binary)` - Thin LTO なしの高速な ARM64 release ビルド
* `Build (arm_darwin)` - macOS ARM64 ビルド
* `Build (arm_v80compat)` - ARMv8.0 互換ビルド

**その他のアーキテクチャ:**

* `Build (ppc64le)` - PowerPC 64 ビット リトルエンディアン
* `Build (riscv64)` - RISC-V 64 ビット
* `Build (s390x)` - IBM System/390 64 ビット
* `Build (loongarch64)` - LoongArch 64 ビット

ジョブが成功すると、ビルド結果は `<repo_root>/ci/tmp/build` ディレクトリで利用できます。

**注:** 「その他のアーキテクチャ」カテゴリ以外のビルド (クロスコンパイルを使用するもの) では、`BUILD_JOB_NAME` で指定したビルドを生成するために、ローカルマシンのアーキテクチャがビルドタイプと一致している必要があります。

<div id="example-run-local">
  #### 例
</div>

ローカルでデバッグビルドを実行するには:

```bash
python -m ci.praktika run "Build (amd_debug)"
```

上記の方法でうまくいかない場合は、ビルドログにある `cmake` オプションを使って、[一般的なビルド手順](../development/build.md)に従ってください。

<div id="functional-stateless-tests">
  ## ステートレス機能テスト
</div>

release、debug、サニタイザ有効など、さまざまな構成でビルドされた ClickHouse バイナリに対して、[ステートレス機能テスト](tests.md#functional-tests)を実行します。
どのテストが失敗しているかはレポートで確認し、その後、[こちら](/ja/development/tests#functional-tests)で説明されている手順に従ってローカルで再現してください。
再現するには正しいビルド構成を使う必要がある点に注意してください。たとえば、あるテストは AddressSanitizer では失敗しても、Debug では通る場合があります。
バイナリは [CI build checks page](/ja/install/advanced) からダウンロードするか、ローカルでビルドしてください。

<div id="integration-tests">
  ## 結合テスト
</div>

[結合テスト](tests.md#integration-tests)を実行します。

<div id="bugfix-validate-check">
  ## バグ修正検証チェック
</div>

新しいテスト (機能テストまたはインテグレーション) が追加されているか、または変更されたテストの中に master ブランチでビルドしたバイナリでは失敗するものがあるかを確認します。
このチェックは、プルリクエストに &quot;pr-bugfix&quot; ラベルがある場合にトリガーされます。

<div id="stress-test">
  ## ストレステスト
</div>

複数のクライアントからステートレスな機能テストを同時実行し、同時実行に起因するエラーを検出します。失敗した場合は:

* まず、ほかのすべてのテスト失敗を修正します。
  * レポートを確認してサーバーログを見つけ、エラーの原因として考えられる点を確認します。

<div id="compatibility-check">
  ## 互換性チェック
</div>

`clickhouse` バイナリが古い libc バージョンの Linux ディストリビューション上で実行できることを確認します。
失敗した場合は、メンテナに支援を依頼してください。

<div id="ast-fuzzer">
  ## AST fuzzer
</div>

プログラムのエラーを検出するために、ランダムに生成されたクエリを実行します。
失敗した場合は、メンテナーに相談してください。

<div id="performance-tests">
  ## パフォーマンステスト
</div>

クエリパフォーマンスの変化を測定します。
これは、実行に6時間弱かかる最も時間のかかるチェックです。
パフォーマンステストレポートについては、[こちら](https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/scripts/README.md#how-to-read-the-report)で詳しく説明されています。