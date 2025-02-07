---
slug: /ja/development/continuous-integration
sidebar_position: 62
sidebar_label: Continuous Integration Checks
title: Continuous Integration Checks
description: プルリクエストを送信すると、ClickHouseの継続的インテグレーション (CI) システムにより自動化されたチェックがコードに対して実行されます
---

プルリクエストを送信すると、ClickHouseの[継続的インテグレーション (CI) システム](tests.md#test-automation)により、自動化されたチェックがコードに対して実行されます。これは、リポジトリの管理者（ClickHouseチームの誰か）がコードを確認し、プルリクエストに`can be tested`ラベルを追加した後に行われます。チェックの結果は、[GitHubチェックのドキュメント](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks)に記載されているように、GitHubのプルリクエストページに一覧表示されます。チェックが失敗した場合、それを修正する必要があるかもしれません。このページでは、遭遇する可能性のあるチェックの概要と、それを修正するためにできることを説明します。

チェックの失敗が変更内容とは関係ないように見える場合、それは一時的な失敗やインフラの問題かもしれません。プルリクエストに空のコミットをプッシュして、CIチェックを再起動してください。
```
git reset
git commit --allow-empty
git push
```

どうすればよいか分からない場合は、管理者に助けを求めてください。


## Merge With Master

PRがマスターにマージできることを確認します。できない場合、`Cannot fetch mergecommit`というメッセージで失敗します。このチェックを修正するためには、[GitHubのドキュメント](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github)に記載されているようにコンフリクトを解決するか、gitを使用して`master`ブランチをプルリクエストのブランチにマージします。


## Docs check

ClickHouseのドキュメントサイトのビルドを試みます。ドキュメントに何か変更を加えた場合に失敗する可能性があります。最も可能性が高い原因は、ドキュメント内のクロスリンクが間違っていることです。チェックレポートに移動し、`ERROR`および`WARNING`メッセージを探してください。


## Description Check

プルリクエストの説明がテンプレート[PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md)に準拠しているかチェックします。変更に対する変更ログカテゴリ（例: Bug Fix）を指定し、[CHANGELOG.md](../whats-new/changelog/index.md)に変更を説明するユーザー向けメッセージを書かなければなりません。


## Push To DockerHub

ビルドやテストに使用されるDockerイメージをビルドし、DockerHubにプッシュします。


## Marker Check

このチェックはCIシステムがプルリクエストの処理を開始したことを意味します。'pending'ステータスのときは、まだすべてのチェックが開始されていないことを示します。すべてのチェックが開始されると、ステータスは'success'に変更されます。


## Style Check

[`utils/check-style/check-style`](https://github.com/ClickHouse/ClickHouse/blob/master/utils/check-style/check-style)バイナリを使用して、コードスタイルの単純な正規表現ベースのチェックを行います（ローカルで実行可能です）。失敗の場合は、[コーディングスタイルガイド](style.md)に従ってスタイルエラーを修正してください。

#### ローカルでスタイルチェックを実行する:
```sh
mkdir -p /tmp/test_output
# 全てのチェックを実行
python3 tests/ci/style_check.py --no-push

# 指定されたチェックスクリプトを実行（例: ./check-mypy）
docker run --rm --volume=.:/ClickHouse --volume=/tmp/test_output:/test_output -u $(id -u ${USER}):$(id -g ${USER}) --cap-add=SYS_PTRACE --entrypoint= -w/ClickHouse/utils/check-style clickhouse/style-test ./check-mypy

# スタイルチェックスクリプトがあるディレクトリへ移動:
cd ./utils/check-style

# 重複インクルードをチェック
./check-duplicate-includes.sh

# C++フォーマットをチェック
./check-style

# Pythonフォーマットをblackでチェック
./check-black

# Pythonの型ヒントをmypyでチェック
./check-mypy

# flake8でPythonをチェック
./check-flake8

# codespellでコードをチェック
./check-typos

# ドキュメントのスペルをチェック
./check-doc-aspell

# 空白をチェック
./check-whitespaces

# GitHub Actionsのワークフローをチェック
./check-workflows

# サブモジュールをチェック
./check-submodules

# shellcheckでシェルスクリプトをチェック
./shellcheck-run.sh
```

## Fast Test

通常これはPRに対して最初に実行されるチェックです。ClickHouseのビルドと、多くの[ステートレス機能テスト](tests.md#functional-tests)が実行されますが、一部は省略されます。これが失敗すると、それが修正されるまでさらなるチェックは開始されません。どのテストが失敗したかを確認し、それをローカルで再現する方法は[こちら](tests.md#functional-test-locally)にあります。

#### ローカルでFast Testを実行する:
```sh
mkdir -p /tmp/test_output
mkdir -p /tmp/fasttest-workspace
cd ClickHouse
# このdockerコマンドは最小限のClickHouseビルドを行い、FastTestsを実行します
docker run --rm --cap-add=SYS_PTRACE -u $(id -u ${USER}):$(id -g ${USER})  --network=host -e FASTTEST_WORKSPACE=/fasttest-workspace -e FASTTEST_OUTPUT=/test_output -e FASTTEST_SOURCE=/ClickHouse --cap-add=SYS_PTRACE -e stage=clone_submodules --volume=/tmp/fasttest-workspace:/fasttest-workspace --volume=.:/ClickHouse --volume=/tmp/test_output:/test_output clickhouse/fasttest
```


#### ステータスページファイル
- `runlog.out.log`はすべての他のログを含む一般的なログです。
- `test_log.txt`
- `submodule_log.txt`には、必要なサブモジュールのクローン化およびチェックアウトに関するメッセージが含まれます。
- `stderr.log`
- `stdout.log`
- `clickhouse-server.log`
- `clone_log.txt`
- `install_log.txt`
- `clickhouse-server.err.log`
- `build_log.txt`
- `cmake_log.txt`にはC/C++およびLinuxフラグチェックに関するメッセージが含まれます。

#### ステータスページのカラム

- *Test name*：テストの名前（パスを除く、例えばすべてのタイプのテストは名前に統合されます）。
- *Test status* -- _Skipped_, _Success_, _Fail_のいずれか。
- *Test time, sec.* -- このテストでは空白です。


## ビルドチェック {#build-check}

さまざまな構成でClickHouseをビルドし、さらにステップで使用します。失敗したビルドを修正する必要があります。ビルドログにはエラーを修正するための十分な情報が含まれていることが多いですが、ローカルで失敗を再現する必要があるかもしれません。`cmake`オプションはビルドログで`cmake`を探して見つけることができます。これらのオプションを使用して[一般的なビルドプロセス](../development/build.md)に従ってください。

### レポート詳細

- **Compiler**: `clang-18`、オプションでターゲットプラットフォームの名前
- **Build type**: `Debug`または`RelWithDebInfo` (cmake)。
- **Sanitizer**: `none`（サニタイザーなし）、`address` (ASan)、`memory` (MSan)、`undefined` (UBSan)、または`thread` (TSan)。
- **Status**: `success`または`fail`
- **Build log**: ビルドおよびファイルコピーのログへのリンク、ビルド失敗時に役立ちます。
- **Build time**。
- **Artifacts**: ビルド結果ファイル（`XXX`はサーバーバージョン例: `20.8.1.4344`）。
  - `clickhouse-client_XXX_amd64.deb`
  - `clickhouse-common-static-dbg_XXX[+asan, +msan, +ubsan, +tsan]_amd64.deb`
  - `clickhouse-common-staticXXX_amd64.deb`
  - `clickhouse-server_XXX_amd64.deb`
  - `clickhouse`: 主なビルド済みバイナリ。
  - `clickhouse-odbc-bridge`
  - `unit_tests_dbms`: ClickHouseのユニットテストを含むGoogleTestバイナリ。
  - `performance.tar.zst`: パフォーマンステスト用の特別パッケージ。


## 特殊ビルドチェック

`clang-tidy`を使用して静的解析とコードスタイルのチェックを行います。レポートは[ビルドチェック](#build-check)に類似しています。ビルドログの中で見つかったエラーを修正してください。

#### ローカルでclang-tidyを実行する:
Dockerでclang-tidyビルドを実行する便利な`packager`スクリプトがあります
```sh
mkdir build_tidy
./docker/packager/packager --output-dir=./build_tidy --package-type=binary --compiler=clang-18 --debug-build --clang-tidy
```


## Stateless 機能テスト

さまざまな構成でビルドされたClickHouseバイナリに対して、[ステートレス機能テスト](tests.md#functional-tests)を実行します。どのテストが失敗したかを確認し、それをローカルで再現する方法は[こちら](tests.md#functional-test-locally)にあります。正しいビルド構成を使用して再現する必要があることに注意してください。アドレスサニタイザ（AddressSanitizer）で失敗するテストがデバッグで成功する可能性があります。バイナリを[CIビルドチェックページ](../development/build.md#you-dont-have-to-build-clickhouse)からダウンロードするか、ローカルでビルドしてください。

## Stateful 機能テスト

[Stateful 機能テスト](tests.md#functional-tests)を実行します。ステートレス機能テストと同様に扱います。違いは[clickstreamデータセット](../getting-started/example-datasets/metrica.md)から`hits`および`visits`テーブルが必要なことです。

## 統合テスト

[統合テスト](tests.md#integration-tests)を実行します。

## バグ修正検証チェック

新しいテスト（機能または統合）があるか、マスターブランチでビルドされたバイナリで失敗する変更されたテストがあるかをチェックします。プルリクエストに"pr-bugfix"ラベルが付いているとこのチェックがトリガーされます。

## ストレステスト

同時に複数のクライアントからステートレス機能テストを実行し、同時実行性に関連するエラーを検出します。これが失敗した場合:

  * まずすべての他のテストの失敗を修正してください;
  * レポートを見て、サーバーログをチェックし、エラーの可能性のある原因を調べてください。



## 互換性チェック

`clickhouse`バイナリが古いlibcバージョンのディストリビューションで実行されることを確認します。失敗した場合、管理者に助けを求めてください。

## AST ファジング

プログラムエラーを発見するためにランダムに生成されたクエリを実行します。失敗した場合は、管理者に助けを求めてください。

## パフォーマンステスト

クエリのパフォーマンスの変化を測定します。これは最も長いチェックで、実行には約6時間かかります。パフォーマンステストレポートの詳細は[こちら](https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#how-to-read-the-report)に記載されています。
