---
sidebar_label: 共有責任モデル
slug: /ja/cloud/security/shared-responsibility-model
title: セキュリティ共有責任モデル
---

## サービスタイプ

ClickHouse Cloud は3つのサービスタイプを提供しています。詳細は、[サービスタイプ](/docs/ja/cloud/manage/service-types)ページをご覧ください。

- 開発: 小規模なワークロードや開発環境に最適
- 本番: 中規模のワークロードや顧客向けアプリケーション向け
- 専用: 厳しいレイテンシや分離要件があるアプリケーション向け

## クラウドアーキテクチャ

クラウドアーキテクチャは、コントロールプレーンとデータプレーンで構成されています。コントロールプレーンは、組織の作成、コントロールプレーン内のユーザー管理、サービス管理、APIキー管理、および請求を担当しています。データプレーンは、オーケストレーションと管理のためのツールを実行し、顧客サービスを収容します。詳細は、[ClickHouse Cloudアーキテクチャ](/docs/ja/cloud/reference/architecture)図をご覧ください。

## BYOCアーキテクチャ

自身のクラウド（BYOC：Bring Your Own Cloud）を使用すると、顧客は自分のクラウドアカウントでデータプレーンを実行できます。詳細は、[BYOC（Bring Your Own Cloud）](/docs/ja/cloud/reference/byoc)ページをご覧ください。

## ClickHouse Cloudの共有責任モデル

| コントロール                                                          | ClickHouse Cloud  | 顧客 - クラウド | 顧客 - BYOC |
|-----------------------------------------------------------------------|-------------------|------------------|-----------------|
| 環境の分離を維持する                                                  | ✔️                 |                  | ✔️               |
| ネットワーク設定を管理する                                            | ✔️                 | ✔️                | ✔️               |
| ClickHouseシステムへのアクセスを安全に管理する                      | ✔️                 |                  |                 |
| コントロールプレーンおよびデータベース内の組織ユーザーを安全に管理    |                   | ✔️                | ✔️               |
| ユーザー管理と監査                                                    | ✔️                 | ✔️                | ✔️               |
| データを転送中および保管中に暗号化する                                | ✔️                 |                  |                 |
| 顧客管理の暗号化キーを安全に扱う                                      |                   | ✔️                | ✔️               |
| 冗長インフラストラクチャを提供する                                    | ✔️                 |                  | ✔️               |
| データのバックアップ                                                  | ✔️                 |                  |                 |
| バックアップリカバリ機能を確認する                                    | ✔️                 |                  |                 |
| データ保持設定を実装する                                              |                   | ✔️                | ✔️               |
| セキュリティ構成管理                                                    | ✔️                 |                  | ✔️               |
| ソフトウェアおよびインフラの脆弱性修正                                  | ✔️                 |                  |                 |
| ペネトレーションテストを実施する                                      | ✔️                 |                  |                 |
| 脅威の検出と対応                                                      | ✔️                 |                  | ✔️               |
| セキュリティインシデント対応                                          | ✔️                 |                  | ✔️               |

## ClickHouse Cloudの設定可能なセキュリティ機能

<details>
  <summary>ネットワーク接続</summary>

  | 設定                                                                                              | ステータス    | クラウド             | サービスレベル           |  
  |------------------------------------------------------------------------------------------------------|-----------|-------------------|-------------------------|
  | サービスへの接続を制限する[IPフィルタ](/docs/ja/cloud/security/setting-ip-filters)         | Available | AWS, GCP, Azure   | All                     |
  | サービスへの安全な接続のための[プライベートリンク](/docs/ja/cloud/security/private-link-overview)| Available | AWS, GCP, Azure   | Production or Dedicated |
  
</details>
<details>
  <summary>アクセス管理</summary>

  
  | 設定                                                                                              | ステータス    | クラウド             | サービスレベル           |  
  |------------------------------------------------------------------------------------------------------|-----------|-------------------|-------------------------|
  | コントロールプレーンでの[標準のロールベースアクセス](/docs/ja/cloud/security/cloud-access-management) | Available | AWS, GCP, Azure | All               | 
  | [多要素認証（MFA）](/docs/ja/cloud/security/cloud-authentication#multi-factor-authhentication)利用可能 | Available | AWS, GCP, Azure | All   |
  | コントロールプレーンへの[SAMLシングルサインオン](/docs/ja/cloud/security/saml-setup)利用可能      | Preview   | AWS, GCP, Azure   | Qualified Customers     |
  | データベースにおける[詳細なロールベースアクセス制御](/docs/ja/cloud/security/cloud-access-management#database-roles) | Available | AWS, GCP, Azure | All          |
  
</details>
<details>
  <summary>データセキュリティ</summary>

  | 設定                                                                                              | ステータス    | クラウド             | サービスレベル           |  
  |------------------------------------------------------------------------------------------------------|-----------|-------------------|-------------------------|
  | [クラウドプロバイダーとリージョン](/docs/ja/cloud/reference/supported-regions)の選択                   | Available | AWS, GCP, Azure   | All                     |
  | 制限付きの[無料のデイリーバックアップ](/docs/ja/cloud/manage/backups#default-backup-policy)        | Available | AWS, GCP, Azure   | All                     |
  | [カスタムバックアップ設定](/docs/ja/cloud/manage/backups#configurable-backups)利用可能            | Available | GCP, AWS, Azure   | Production or Dedicated |
  | 顧客管理の暗号化キー（CMEK）による透過的なデータ暗号化利用可能 | Available | AWS | Production or Dedicated |
  | 手動キー管理による[フィールドレベル暗号化](/docs/ja/sql-reference/functions/encryption-functions)   | Availablle | GCP, AWS, Azure | All  |

  
</details>
<details>
  <summary>データ保持</summary>

  | 設定                                                                                              | ステータス    | クラウド             | サービスレベル           |  
  |------------------------------------------------------------------------------------------------------|-----------|-------------------|-------------------------|
  | [有効期限(TTL)](/docs/ja/sql-reference/statements/alter/ttl)設定による保持管理                  | Available | AWS, GCP, Azure   | All                     |
  | 大量削除アクションのための[ALTER TABLE DELETE](/docs/ja/sql-reference/statements/alter/delete)   | Available | AWS, GCP, Azure   | All                     |
  | 測定された削除活動のための[軽量DELETE](/docs/ja/sql-reference/statements/delete)                  | Available | AWS, GCP, Azure   | All                     |
  
</details>
<details>
  <summary>監査とロギング</summary>

  | 設定                                                                                              | ステータス    | クラウド             | サービスレベル           |  
  |------------------------------------------------------------------------------------------------------|-----------|-------------------|-------------------------|
  | コントロールプレーン活動のための[監査ログ](/docs/ja/cloud/security/audit-logging)               | Available | AWS, GCP, Azure   | All                     |
  | データベース活動のための[セッションログ](/docs/ja/operations/system-tables/session_log)       | Available | AWS, GCP, Azure   | All                     |
  | データベース活動のための[クエリログ](/docs/ja/operations/system-tables/query_log)              | Available | AWS, GCP, Azure   | All                     |
  
</details>

## ClickHouse Cloudコンプライアンス

  | フレームワーク                                                                                          | ステータス    | クラウド             | サービスレベル           |  
  |------------------------------------------------------------------------------------------------------|-----------|-------------------|-------------------------|
  | ISO 27001コンプライアンス                                                                                | Available | AWS, GCP, Azure   | All                     |
  | SOC 2 Type IIコンプライアンス                                                                         | Available | AWS, GCP, Azure   | All                     |
  | GDPRおよびCCPAコンプライアンス                                                                         | Available | AWS, GCP, Azure   | All                     |
  | HIPAAコンプライアンス                                                                                   | Beta | GCP, `AWSは間もなく対応予定` | Dedicated        |

  対応しているコンプライアンスフレームワークの詳細については、[セキュリティとコンプライアンス](/docs/ja/cloud/security/security-and-compliance)ページをご覧ください。
