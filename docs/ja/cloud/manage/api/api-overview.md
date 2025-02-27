---
sidebar_label: 概要
sidebar_position: 1
---

# ClickHouse Cloud API

## 概要

ClickHouse Cloud APIは、ClickHouse Cloud上の組織やサービスを簡単に管理できるように設計されたREST APIです。このCloud APIを使用すると、サービスの作成や管理、APIキーのプロビジョニング、組織内のメンバーの追加または削除などが可能です。

[最初のAPIキーを作成してClickHouse Cloud APIの使用を開始する方法を学びましょう。](/docs/ja/cloud/manage/openapi.md)

## レート制限

開発者は、1つの組織につき100個のAPIキーに制限されています。各APIキーは、10秒間のウィンドウ内で10回のリクエストに制限されています。組織のためにAPIキーや10秒ごとのリクエスト数を増やしたい場合は、support@clickhouse.comまでご連絡ください。

## Terraformプロバイダー

公式のClickHouse Terraformプロバイダーを利用すると、[コードとしてのインフラストラクチャ](https://www.redhat.com/en/topics/automation/what-is-infrastructure-as-code-iac)を使用して、予測可能でバージョン管理された設定を作成し、デプロイメントのエラー発生を大幅に減らすことができます。

Terraformプロバイダーのドキュメントは、[Terraformレジストリ](https://registry.terraform.io/providers/ClickHouse/clickhouse/latest/docs)で閲覧できます。

ClickHouse Terraformプロバイダーに貢献したい場合は、[GitHubリポジトリ](https://github.com/ClickHouse/terraform-provider-clickhouse)でソースを確認できます。

## Swagger（OpenAPI）エンドポイントとUI

ClickHouse Cloud APIはオープンソースの[OpenAPI仕様](https://www.openapis.org/)に基づいて構築されており、クライアント側での予測可能な利用を可能にします。ClickHouse Cloud APIドキュメントをプログラムで利用する必要がある場合は、https://api.clickhouse.cloud/v1を通してJSONベースのSwaggerエンドポイントを提供しています。当社のAPIリファレンスドキュメントは、同じエンドポイントから自動生成されています。Swagger UIを通してAPIドキュメントを閲覧したい場合は、[こちら](https://clickhouse.com/docs/ja/cloud/manage/api/swagger)をクリックしてください。

## サポート

迅速なサポートを得るには、まず[当社のSlackチャネル](https://clickhouse.com/slack)を訪れることをお勧めします。APIやその機能に関して追加のヘルプや詳細情報が必要な場合は、https://clickhouse.cloud/supportでClickHouseサポートにお問い合わせください。
