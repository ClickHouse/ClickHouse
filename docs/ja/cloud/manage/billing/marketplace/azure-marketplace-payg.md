---
slug: /ja/cloud/billing/marketplace/azure-marketplace-payg
title: Azure Marketplace PAYG
description: Azure Marketplace (PAYG) を通じて ClickHouse Cloud に登録する方法。
keywords: [azure, marketplace, 課金, payg]
---

[Azure Marketplace](https://azuremarketplace.microsoft.com/en-us/marketplace/apps) で「PAYG (Pay-as-you-go)」のパブリックオファーを利用して ClickHouse Cloud を始めましょう。

## 前提条件

- あなたの課金管理者によって購入権限が有効になっている Azure プロジェクト。
- Azure Marketplace で ClickHouse Cloud を購読するには、購入権限のあるアカウントでログインし、適切なプロジェクトを選択する必要があります。

1. [Azure Marketplace](https://azuremarketplace.microsoft.com/en-us/marketplace/apps) にアクセスし、ClickHouse Cloud を検索してください。マーケットプレースでオファーを購入できるようにログインしていることを確認してください。

<br />

<img src={require('./images/azure-marketplace-payg-1.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '300px'}}
/>

<br />

2. 製品リストページで、**Get It Now** をクリックします。

<br />

<img src={require('./images/azure-marketplace-payg-2.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '500px'}}
/>

<br />

3. 次の画面で、名前、メール、所在地の情報を提供する必要があります。

<br />

<img src={require('./images/azure-marketplace-payg-3.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

4. 次の画面で、**Subscribe** をクリックします。

<br />

<img src={require('./images/azure-marketplace-payg-4.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

5. 次の画面で、サブスクリプション、リソースグループ、およびリソースグループの場所を選択します。リソースグループの場所は、ClickHouse Cloud でサービスを起動する予定の場所と同じである必要はありません。

<br />

<img src={require('./images/azure-marketplace-payg-5.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '500px'}}
/>

<br />

6. サブスクリプションの名前を提供する必要があり、利用可能なオプションから課金条件を選択します。**Recurring billing** をオンまたはオフに設定できます。オフに設定すると、課金条件が終了した時点で契約が終了し、リソースが廃止されます。

<br />

<img src={require('./images/azure-marketplace-payg-6.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '500px'}}
/>

<br />

7. **"Review + subscribe"** をクリックします。

8. 次の画面で、すべてが正しいことを確認し、**Subscribe** をクリックします。

<br />

<img src={require('./images/azure-marketplace-payg-7.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

9. この時点で、ClickHouse Cloud の Azure サブスクリプションに登録されていますが、ClickHouse Cloud アカウントのセットアップはまだ行われていません。この次のステップは、ClickHouse Cloud が Azure サブスクリプションにバインドするために必要であり、Azure マーケットプレイスを通じて正しく課金されるために重要です。

<br />

<img src={require('./images/azure-marketplace-payg-8.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '500px'}}
/>

<br />

10. Azure のセットアップが完了すると、**Configure account now** ボタンがアクティブになります。

<br />

<img src={require('./images/azure-marketplace-payg-9.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

11. **Configure account now** をクリックします。

<br />

アカウントの設定に関する詳細を記載した以下のようなメールを受け取ります。

<br />

<img src={require('./images/azure-marketplace-payg-10.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

12. ClickHouse Cloud のサインアップまたはサインインページにリダイレクトされます。新しいアカウントを使用してサインアップするか、既存のアカウントを使用してサインインすることができます。サインインが完了すると、Azure Marketplace を介して請求される準備が整った新しい組織が作成されます。

13. 続行する前に、住所や会社の詳細に関するいくつかの質問に答える必要があります。

<br />

<img src={require('./images/aws-marketplace-payg-8.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

<img src={require('./images/aws-marketplace-payg-9.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム 2'
    class='image'
    style={{width: '400px'}}
/>

<br />

14. **Complete sign up** をクリックすると、ClickHouse Cloud 内の組織に移動し、Azure Marketplace 経由で請求されることを確認し、サービスを作成できる請求画面を表示できます。

<br />

<br />

<img src={require('./images/azure-marketplace-payg-11.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '300px'}}
/>

<br />

<br />

<img src={require('./images/azure-marketplace-payg-12.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '500px'}}
/>

<br />

15. もし問題が発生した場合は、遠慮なく[サポートチーム](https://clickhouse.com/support/program)までご連絡ください。
