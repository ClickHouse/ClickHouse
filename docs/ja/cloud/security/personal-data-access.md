---
sidebar_label: 個人データアクセス
slug: /ja/cloud/security/personal-data-access
title: 個人データアクセス
---

## はじめに
登録ユーザーであれば、ClickHouseを使用してアカウントの個人データを表示および修正することができます。これには、連絡先情報や、役割に応じて組織内の他のユーザーの連絡先情報、APIキーの詳細情報、その他の情報が含まれることがあります。これらはClickHouseのユーザーインターフェース（UI）から自己管理で行うことができます。

**データ主体アクセス要求（DSAR）とは**
あなたの所在地によっては、適用法によりClickHouseが保持するあなたの個人データに関する追加の権利（データ主体の権利）が提供される場合があります。この権利については、ClickHouseのプライバシーポリシーで説明されています。データ主体の権利を行使するためのプロセスは、データ主体アクセス要求（DSAR）として知られています。

**個人データの範囲**
ClickHouseが収集する個人データの詳細やその使用方法については、ClickHouseのプライバシーポリシーをご確認ください。

## セルフサービス
デフォルトでは、ClickHouseはユーザーが自分の個人データをClickHouseのユーザーインターフェース（UI）から直接表示できるようにします。

以下に、アカウントを設定しサービスを使用する際にClickHouseが収集するデータの概要と、特定の個人データをClickHouse UI内で表示できる場所を示します。

| 場所/URL | 説明 | 個人データ |
|-------------|----------------|-----------------------------------------|
| https://auth.clickhouse.cloud/u/signup/ | アカウント登録 | メール、パスワード |
| https://clickhouse.cloud/profile | 一般的なユーザープロファイル詳細 |  名前、メール |
| https://clickhouse.cloud/organizations/OrgID/members | 組織内のユーザーリスト | 名前、メール |
| https://clickhouse.cloud/organizations/OrgID/keys | APIキーのリストとそれを作成した人 | メール |
| https://clickhouse.cloud/organizations/OrgID/activity | ユーザーごとの行動ログ | メール |
| https://clickhouse.cloud/organizations/OrgID/admin/billing | 請求情報と請求書 | 請求先住所、メール |
| https://control-plane.clickhouse-dev.com/support | ClickHouseサポートとのやり取り | 名前、メール |

注意: `OrgID`が含まれるURLは、あなたの特定のアカウントに対応するOrgIDに更新する必要があります。

### 既存のお客様
お客様が私たちのアカウントをお持ちで、上記のセルフサービスオプションで個人データの問題が解決せず、プライバシーポリシーに基づいてデータ主体アクセス要求を行いたい場合は、ClickHouseアカウントにログインし、[サポートケースを開いて](https://clickhouse.cloud/support)ください。これにより、あなたの身元を確認し、リクエストを完了するための手順を減らすことができます。

サポートケースには以下の詳細を含めてください：

| 項目 | リクエストに含めるテキスト |
|-------------|---------------------------------------------------|
| 件名 | データ主体アクセス要求（DSAR） |
| 説明 | ClickHouseに探して、収集して、または提供してほしい情報の詳細な説明。 |

<img src={require('./images/support-case-form.png').default}
  class="image"
  alt="サポートケースフォーム"
  style={{width: '30%'}} />

### アカウントをお持ちでない方
アカウントをお持ちでなく、上記のセルフサービスオプションで個人データの問題が解決しない場合は、プライバシーポリシーに基づいてデータ主体アクセス要求を行うことができます。これらのリクエストは、[privacy@clickhouse.com](mailto:privacy@clickhouse.com)にメールで提出してください。

## 身元確認

メール経由でデータ主体アクセス要求を提出した場合、リクエストを処理しあなたの身元を確認するために特定の情報をお願いすることがあります。適用法により、リクエストを拒否する義務があったり、許可されることがあります。リクエストを拒否する場合は、法的な制限に従いその理由をお伝えします。

詳細については、[ClickHouseプライバシーポリシー](https://clickhouse.com/legal/privacy-policy)をご確認ください。
