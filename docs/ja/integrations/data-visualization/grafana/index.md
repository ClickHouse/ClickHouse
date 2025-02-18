---
sidebar_label: クイックスタート
sidebar_position: 1
slug: /ja/integrations/grafana
description: ClickHouse を Grafana で使用するための導入
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_native.md';

# Grafana 用 ClickHouse データソースプラグイン

Grafana を使用すると、ダッシュボードを通じてすべてのデータを探索し、共有できます。Grafana は ClickHouse に接続するためにプラグインを必要とし、これは彼らの UI から簡単にインストールできます。

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/bRce9xWiqQM"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

## 1. 接続情報を集める
<ConnectionDetails />

## 2. 読み取り専用ユーザーの作成

ClickHouse を Grafana のようなデータ可視化ツールに接続する場合、データが不適切に変更されないように読み取り専用ユーザーを作成することをお勧めします。

Grafana はクエリが安全であるかを検証しません。クエリには `DELETE` や `INSERT` を含む任意の SQL ステートメントを含めることができます。

読み取り専用ユーザーを設定するには、次の手順に従ってください：
1. [ClickHouse でのユーザーとロールの作成](/docs/ja/operations/access-rights)ガイドに従って `readonly` ユーザープロファイルを作成します。
2. 基盤となる [clickhouse-go クライアント](https://github.com/ClickHouse/clickhouse-go) に必要な `max_execution_time` 設定を変更するための十分な権限が `readonly` ユーザーにあることを確認します。
3. 公共の ClickHouse インスタンスを使用している場合、`readonly` プロファイルで `readonly=2` を設定することはお勧めしません。代わりに、`readonly=1` のままにし、この設定の変更を許可するために `max_execution_time` の制約タイプを [changeable_in_readonly](/docs/ja/operations/settings/constraints-on-settings) に設定します。

## 3. Grafana 用 ClickHouse プラグインをインストールする

Grafana が ClickHouse へ接続できるようにする前に、適切な Grafana プラグインをインストールする必要があります。Grafana にログインしていることを前提に、次の手順に従います：

1. サイドバーの **Connections** ページから、**Add new connection** タブを選択します。

2. **ClickHouse** を検索し、Grafana Labs の署名されたプラグインをクリックします：

    <img src={require('./images/search.png').default} class="image" alt="Connections ページで ClickHouse プラグインを選択" />

3. 次の画面で、**Install** ボタンをクリックします：

    <img src={require('./images/install.png').default} class="image" alt="ClickHouse プラグインをインストール" />

## 4. ClickHouse データソースを定義する

1. インストールが完了したら、**Add new data source** ボタンをクリックします。（**Data sources** タブからもデータソースを追加できます。）

    <img src={require('./images/add_new_ds.png').default} class="image" alt="ClickHouse データソースを作成" />

2. 下にスクロールして **ClickHouse** データソースのタイプを見つけるか、**Add data source** ページの検索バーで検索します。**ClickHouse** データソースを選択すると、以下のページが表示されます：

  <img src={require('./images/quick_config.png').default} class="image" alt="接続設定ページ" />

3. サーバー設定と認証情報を入力します。主要な設定は次のとおりです：

- **Server host address:** ClickHouse サービスのホスト名。
- **Server port:** ClickHouse サービスのポート。サーバー設定およびプロトコルによって異なります。
- **Protocol:** ClickHouse サービスに接続するためのプロトコル。
- **Secure connection:** サーバーがセキュア接続を必要とする場合に有効にします。
- **Username** と **Password**: ClickHouse のユーザー資格情報を入力します。ユーザーを設定していない場合は、ユーザー名に `default` を試してください。[読み取り専用ユーザーを設定](#2-making-a-read-only-user)することをお勧めします。

詳細な設定については、[プラグイン設定](./config.md)ドキュメントを確認してください。

4. **Save & test** ボタンをクリックして、Grafana が ClickHouse サービスに接続できることを確認します。成功した場合、**Data source is working** メッセージが表示されます：

    <img src={require('./images/valid_ds.png').default} class="image" alt="Select Save & test" />

## 5. 次のステップ

これでデータソースの準備が整いました！[query builder](./query-builder.md) でクエリを構築する方法について詳しく学びましょう。

設定の詳細については、[プラグイン設定](./config.md)ドキュメントを確認してください。

これらのドキュメントに含まれていない情報をお探しの場合は、[GitHub のプラグインリポジトリ](https://github.com/grafana/clickhouse-datasource)を確認してください。

## プラグインバージョンのアップグレード

v4 以降では、新しいバージョンがリリースされると設定やクエリがアップグレードされます。

v3 からの設定とクエリは開いたときに v4 に移行されます。古い設定やダッシュボードは v4 でロードされますが、移行は新しいバージョンで再保存するまで保持されません。古い設定/クエリを開いた際に問題が発生した場合は、変更を破棄し、[GitHub で問題を報告](https://github.com/grafana/clickhouse-datasource/issues)してください。

新しいバージョンで作成された設定/クエリを以前のバージョンにダウングレードすることはできません。

## 関連コンテンツ

- [GitHub のプラグインリポジトリ](https://github.com/grafana/clickhouse-datasource)
- ブログ: [Visualizing Data with ClickHouse - Part 1 - Grafana](https://clickhouse.com/blog/visualizing-data-with-grafana)
- ブログ: [Visualizing ClickHouse Data with Grafana - Video](https://www.youtube.com/watch?v=Ve-VPDxHgZU)
- ブログ: [ClickHouse Grafana plugin 4.0 - Leveling up SQL Observability](https://clickhouse.com/blog/clickhouse-grafana-plugin-4-0)
- ブログ: [データの ClickHouse への投入 - Part 3 - S3 の使用](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)
- ブログ: [ClickHouse を用いた Observability ソリューションの構築 - Part 1 - ログ](https://clickhouse.com/blog/storing-log-data-in-clickhouse-fluent-bit-vector-open-telemetry)
- ブログ: [ClickHouse を用いた Observability ソリューションの構築 - Part 2 - トレース](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
- ブログ & ウェビナー: [ClickHouse + Grafana を用いたオープンソース GitHub アクティビティのストーリー](https://clickhouse.com/blog/introduction-to-clickhouse-and-grafana-webinar)
