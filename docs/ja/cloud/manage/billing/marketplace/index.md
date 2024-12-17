---
slug: /ja/cloud/marketplace
title: Marketplaceの支払い
description: AWS、GCP、Azure Marketplaceを通じてClickHouse Cloudにサブスクライブ。
keywords: [aws, azure, gcp, google cloud, marketplace, billing]
---

ClickHouse Cloudにサブスクライブするには、AWS、GCP、Azureの各マーケットプレイスを通じて行うことができます。これにより、既存のクラウドプロバイダーの請求を通じてClickHouse Cloudの支払いが可能になります。

**従量課金制**（PAYG）を利用するか、市場を通じてClickHouse Cloudとの契約を締結することができます。請求はクラウドプロバイダーによって処理され、すべてのクラウドサービスに対して単一の請求書を受取ります。

- [AWS Marketplace PAYG](/ja/cloud/billing/marketplace/aws-marketplace-payg)
- [AWS Marketplace Committed Contract](/ja/cloud/billing/marketplace/aws-marketplace-committed-contract)
- [GCP Marketplace PAYG](/ja/cloud/billing/marketplace/gcp-marketplace-payg)
- [GCP Marketplace Committed Contract](/ja/cloud/billing/marketplace/gcp-marketplace-committed-contract)
- [Azure Marketplace PAYG](/ja/cloud/billing/marketplace/azure-marketplace-payg)
- [Azure Marketplace Committed Contract](/ja/cloud/billing/marketplace/azure-marketplace-committed-contract)

## よくある質問（FAQ）

**自分の組織がマーケットプレイス請求に接続されていることをどのように確認できますか？**

ClickHouse Cloudコンソールで、**Billing** に移動します。**Payment details** セクションにマーケットプレイスの名前とリンクが表示されるはずです。

**既存のClickHouse Cloudユーザーです。AWS / GCP / Azureマーケットプレイスを介してClickHouse Cloudにサブスクライブした場合、どうなりますか？**

クラウドプロバイダーマーケットプレイスからClickHouse Cloudに登録するには、以下の2ステップが必要です：
1. 最初に、クラウドプロバイダーのマーケットプレースポータルでClickHouse Cloudに「サブスクライブ」します。サブスクライブを完了したら、「Pay Now」または「Manage on Provider」（マーケットプレイスによって異なる）をクリックします。これで、ClickHouse Cloudにリダイレクトされます。
2. ClickHouse Cloudで新しいアカウントに登録するか、既存のアカウントでサインインします。どちらの場合も、マーケットプレイス請求に紐付けられた新しいClickHouse Cloud組織が作成されます。

**注**：以前のClickHouse Cloudサインアップで作成された既存のサービスと組織は残り、マーケットプレイス請求には接続されません。ClickHouse Cloudでは、同一アカウントで異なる請求方法を持つ複数の組織を管理できます。

ClickHouse Cloudコンソールの左下のメニューから組織を切り替えることができます。

**既存のClickHouse Cloudユーザーです。既存のサービスをマーケットプレイス経由で請求したい場合はどうすれば良いですか？**

その場合は、[ClickHouse Cloudサポート](https://clickhouse.com/support/program)にお問い合わせください。マーケットプレイスを通じてClickHouse Cloudにサブスクライブする必要があり、リソースへの組織のリンクを切り替えて、請求がマーケットプレイスを通じて行われるようにします。

**マーケットプレイスユーザーとしてClickHouse Cloudにサブスクライブしました。どのようにして購読を解除できますか？**

ClickHouse Cloudの使用を停止して、既存のすべてのClickHouse Cloudサービスを削除するだけでも問題ありません。サブスクリプションは引き続き有効ですが、再発料金はないため、料金は発生しません。

もしサブスクリプションの解除を希望する場合は、クラウドプロバイダーコンソールに移動し、そこでサブスクリプションの更新をキャンセルしてください。サブスクリプションが終了すると、すべての既存サービスは停止され、クレジットカードの登録が求められます。カードが追加されていない場合、2週間後にすべての既存サービスが削除されます。

**マーケットプレイスユーザーとしてClickHouse Cloudにサブスクライブしてから、サブスクリプションを解除しました。再度サブスクライブするにはどうすれば良いですか？**

その場合は、通常通りClickHouse Cloudにサブスクライブしてください（マーケットプレイス経由でのサブスクライブに関するセクションを参照）。

- AWSマーケットプレイスの場合、新しいClickHouse Cloud組織が作成され、マーケットプレイスに接続されます。
- GCPマーケットプレイスの場合は、古い組織が再有効化されます。

マーケットプレイスを利用した組織の再活性化に問題がある場合は、[ClickHouse Cloudサポート](https://clickhouse.com/support/program)にお問い合わせください。

**ClickHouse Cloudサービスのマーケットプレイスサブスクリプションの請求書にはどこでアクセスできますか？**

- [AWS請求コンソール](https://us-east-1.console.aws.amazon.com/billing/home)
- [GCPマーケットプレースオーダー](https://console.cloud.google.com/marketplace/orders)（サブスクリプションに使用した請求アカウントを選択）

**なぜ使用状況のステートメントの日付がマーケットプレイス請求書と一致しないのですか？**

マーケットプレイスの請求はカレンダー月のサイクルに従います。たとえば、12月1日から1月1日までの使用について、1月3日から5日の間に請求書が発行されます。

ClickHouse Cloudの使用状況ステートメントは、サインアップ日から始まる30日間のサイクルに従ってメーターされ、報告されます。

これらの日付が同じでない場合は、使用状況と請求書の日付が異なることがあります。使用状況ステートメントは、特定のサービスの一日の使用を追跡するため、ユーザーはその費用の内訳を確認する際にステートメントに頼ることができます。

**一般的な請求情報はどこで確認できますか？**

[Billing overview page](http://localhost:3000/docs/en/manage/billing)をご覧ください。
