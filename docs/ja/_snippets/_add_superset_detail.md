<details><summary>DockerでApache Supersetを起動</summary>

Supersetは、[Docker Composeを使用してローカルにSupersetをインストールする](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/)手順を提供しています。GitHubからApache Supersetリポジトリをチェックアウトした後、最新の開発コードや特定のタグを実行することができます。`pre-release`としてマークされていない最新のリリースである2.0.0をお勧めします。

`docker compose`を実行する前にいくつかのタスクを行う必要があります：

1. 公式のClickHouse Connectドライバーを追加
2. MapBox APIキーを取得し、それを環境変数として追加（任意）
3. 実行するSupersetのバージョンを指定

:::tip
以下のコマンドはGitHubリポジトリのトップレベル、`superset`から実行してください。
:::

## 公式ClickHouse Connectドライバー

SupersetデプロイメントでClickHouse Connectドライバーを利用可能にするために、ローカルのrequirementsファイルに追加します：

```bash
echo "clickhouse-connect" >> ./docker/requirements-local.txt
```

## MapBox

これは任意です。MapBox APIキーなしでSupersetで位置データをプロットできますが、キーを追加するべきというメッセージが表示され、地図の背景画像が欠けます（データポイントのみが表示され、地図の背景は表示されません）。MapBoxは無料のティアを提供していますので、利用したい場合はぜひご利用ください。

ガイドが作成するサンプルの可視化の一部は、例えば経度や緯度データなどの位置情報を使用します。SupersetはMapBoxマップのサポートを含んでいます。MapBoxの可視化を使用するには、MapBox APIキーが必要です。[MapBoxの無料ティア](https://account.mapbox.com/auth/signup/)にサインアップし、APIキーを生成してください。

APIキーをSupersetで利用可能にします：

```bash
echo "MAPBOX_API_KEY=pk.SAMPLE-Use-your-key-instead" >> docker/.env-non-dev
```

## Supersetバージョン2.0.0をデプロイ

リリース2.0.0をデプロイするには、以下を実行します：

```bash
git checkout 2.0.0
TAG=2.0.0 docker-compose -f docker-compose-non-dev.yml pull
TAG=2.0.0 docker-compose -f docker-compose-non-dev.yml up
```

</details>
