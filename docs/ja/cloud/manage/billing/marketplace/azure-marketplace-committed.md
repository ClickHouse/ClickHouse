---
slug: /ja/cloud/billing/marketplace/azure-marketplace-committed-contract
title: Azure Marketplace コミットメント契約
description: コミットメント契約を通じて Azure Marketplace で ClickHouse Cloud に登録
keywords: [Microsoft, Azure, Marketplace, 請求, コミットメント, コミットメント契約]
---

[Azure Marketplace](https://azuremarketplace.microsoft.com/en-us/marketplace/apps)でコミットメント契約を通じてClickHouse Cloudを始めましょう。コミットメント契約、またはプライベートオファーとも呼ばれるこの契約では、一定期間中にClickHouse Cloud に一定額を支払うことをお客様が約束します。

## 前提条件

- 特定の契約条件に基づくClickHouseからのプライベートオファー。

## 登録手順

1. プライベートオファーを確認して受け入れるためのリンクが記載されたメールを受け取っているはずです。

<br />

<img src={require('./images/azure-marketplace-committed-1.png').default}
    alt='Azure Marketplace プライベートオファーのメール'
    class='image'
    style={{width: '400px'}}
/>

<br />

2. メール内の**プライベートオファーを確認**リンクをクリックします。これにより、プライベートオファーの詳細が表示されたGCP Marketplaceのページに移動します。

<br />

<img src={require('./images/azure-marketplace-committed-2.png').default}
    alt='Azure Marketplace プライベートオファーの詳細'
    class='image'
    style={{width: '600px'}}
/>

<br />

3. オファーを受け入れると、**プライベートオファー管理**画面に移動します。Azureは購入準備に少し時間がかかる場合があります。

<br />

<img src={require('./images/azure-marketplace-committed-3.png').default}
    alt='Azure Marketplace プライベートオファー管理ページ'
    class='image'
    style={{width: '600px'}}
/>

<br />

<img src={require('./images/azure-marketplace-committed-4.png').default}
    alt='Azure Marketplace プライベートオファー管理ページの読み込み中'
    class='image'
    style={{width: '600px'}}
/>

<br />

4. 数分後にページをリフレッシュします。オファーが**購入**可能になっているはずです。

<br />

<img src={require('./images/azure-marketplace-committed-5.png').default}
    alt='Azure Marketplace プライベートオファー管理ページ 購入可'
    class='image'
    style={{width: '500px'}}
/>

<br />

5. **購入**をクリックすると、フライアウトが開きます。次のことを完了してください：

<br />

- サブスクリプションとリソースグループ
- SaaSサブスクリプションの名前を提供
- プライベートオファーが設定された課金プランを選択します。プライベートオファーが作成された期間のみ（金額が表示されます（例: 1年）。他の課金期間オプションには$0の金額が表示されます）。
- 定期課金の有無を選択します。定期課金が選択されていない場合、契約は請求期間の終わりに終了し、リソースは廃止予定となります。
- **レビューしてサブスクライブ**をクリックします。

<br />

<img src={require('./images/azure-marketplace-committed-6.png').default}
    alt='Azure Marketplace サブスクリプションフォーム'
    class='image'
    style={{width: '500px'}}
/>

<br />

6. 次の画面で、すべての詳細を確認し、**サブスクライブ**を押します。

<br />

<img src={require('./images/azure-marketplace-committed-7.png').default}
    alt='Azure Marketplace サブスクリプション確認'
    class='image'
    style={{width: '500px'}}
/>

<br />

7. 次の画面では、**SaaSサブスクリプションは進行中です**というメッセージが表示されます。

<br />

<img src={require('./images/azure-marketplace-committed-8.png').default}
    alt='Azure Marketplace サブスクリプション送信ページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

8. 準備が整ったら、**今すぐアカウントを構成**をクリックします。このステップは重要です。Azureのサブスクリプションをお客様のClickHouse Cloud組織に結びつける役割を果たします。このステップがないと、Marketplaceのサブスクリプションは完了しません。

<br />

<img src={require('./images/azure-marketplace-committed-9.png').default}
    alt='Azure Marketplace アカウント構成今すぐボタン'
    class='image'
    style={{width: '400px'}}
/>

<br />

9. ClickHouse Cloudのサインアップまたはサインインページにリダイレクトされます。新しいアカウントを使用してサインアップするか、既存のアカウントを使用してサインインできます。サインインが完了すると、Azure Marketplaceを通じて請求され、使用可能な新しい組織が作成されます。

10. いくつかの質問に答える必要があります。次のステップに進む前に、住所や会社の詳細を入力してください。

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

11. **サインアップを完了**をクリックすると、ClickHouse Cloud内の組織に移動し、Azure Marketplaceを通じて課金されることを確認でき、サービスを作成できるようになります。

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

問題が発生した場合は、[サポートチーム](https://clickhouse.com/support/program) にお気軽にお問い合わせください。
