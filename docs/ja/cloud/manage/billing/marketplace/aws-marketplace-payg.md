---
slug: /ja/cloud/billing/marketplace/aws-marketplace-payg
title: AWS Marketplace PAYG
description: AWS Marketplace（PAYG）を介してClickHouse Cloudにサブスクライブします。
keywords: [aws, marketplace, 請求, payg]
---

[AWS Marketplace](https://aws.amazon.com/marketplace)でのPAYG（従量課金制）パブリックオファーを通じてClickHouse Cloudを始めましょう。

## 前提条件

- 購入権限が請求管理者によって有効化されているAWSアカウント。
- 購入するには、このアカウントでAWS Marketplaceにログインする必要があります。

## サインアップの手順

1. [AWS Marketplace](https://aws.amazon.com/marketplace)にアクセスし、ClickHouse Cloudを検索します。

<br />

<img src={require('./images/aws-marketplace-payg-1.png').default}
    alt='AWS Marketplace ホームページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

2. [リスティング](https://aws.amazon.com/marketplace/pp/prodview-jettukeanwrfc)をクリックし、**購入オプションの表示**を選択します。

<br />

<img src={require('./images/aws-marketplace-payg-2.png').default}
    alt='AWS Marketplace ClickHouse 検索'
    class='image'
    style={{width: '500px'}}
/>

<br />

3. 次の画面で契約を設定します：
- **契約の期間** - PAYG契約は月単位で実行されます。
- **更新設定** - 契約の自動更新を設定することができます。 自動更新を有効にしない場合、組織は請求サイクルの終了時に自動的に猶予期間に入り、その後停止されます。

- **契約オプション** - このテキストボックスに任意の数（または1）を入力できます。これらの単位の価格はパブリックオファーの場合$0であるため、価格には影響しません。これらの単位は通常、ClickHouse Cloudからのプライベートオファーを受け入れる際に使用されます。

- **発注書** - これは任意なので、無視しても問題ありません。

<br />

<img src={require('./images/aws-marketplace-payg-3.png').default}
    alt='AWS Marketplace 契約設定'
    class='image'
    style={{width: '500px'}}
/>

<br />

上記の情報を入力したら、**契約を作成**をクリックします。 契約価格が0ドルで表示されていることを確認できますが、これは支払いがないことを意味し、使用に基づいて請求が発生することになります。

<br />

<img src={require('./images/aws-marketplace-payg-4.png').default}
    alt='AWS Marketplace 契約確認'
    class='image'
    style={{width: '500px'}}
/>

<br />

4. **契約を作成**をクリックすると、確認して支払うためのモーダルが表示されます（$0の支払い予定）。

5. **今すぐ支払う**をクリックすると、ClickHouse CloudのAWS Marketplaceオファリングを購読していることの確認が表示されます。

<br />

<img src={require('./images/aws-marketplace-payg-5.png').default}
    alt='AWS Marketplace 支払い確認'
    class='image'
    style={{width: '500px'}}
/>

<br />

6. この時点で、設定はまだ完了していません。**アカウントを設定する**をクリックしてClickHouse Cloudにリダイレクトし、そこでサインアップする必要があります。

7. ClickHouse Cloudにリダイレクトしたら、既存のアカウントでログインするか、新しいアカウントを登録できます。このステップは、AWS Marketplaceの請求にClickHouse Cloudの組織を結びつけるために非常に重要です。

<br />

<img src={require('./images/aws-marketplace-payg-6.png').default}
    alt='ClickHouse Cloud サインインページ'
    class='image'
    style={{width: '300px'}}
/>

<br />

新しいClickHouse Cloudユーザーである場合は、ページの下部にある**登録**をクリックします。新しいユーザーを作成してメールを確認するように求められます。メールを確認した後、ClickHouse Cloudのログインページから離れ、新しいユーザー名を使用して[https://clickhouse.cloud](https://clickhouse.cloud)でログインできます。

<br />

<img src={require('./images/aws-marketplace-payg-7.png').default}
    alt='ClickHouse Cloud サインアップページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

新しいユーザーである場合は、ビジネスに関する基本的な情報も提供する必要があります。以下のスクリーンショットを参照してください。

<br />

<img src={require('./images/aws-marketplace-payg-8.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

<br />

<img src={require('./images/aws-marketplace-payg-9.png').default}
    alt='ClickHouse Cloud サインアップ情報フォーム2'
    class='image'
    style={{width: '400px'}}
/>

<br />

既存のClickHouse Cloudユーザーである場合は、資格情報を使用してログインするだけです。

8. ログインに成功すると、新しいClickHouse Cloud組織が作成されます。この組織はAWSの請求アカウントに接続され、すべての使用がAWSアカウントを通じて請求されます。

9. ログインすると、請求がAWS Marketplaceに結びついていることを確認し、ClickHouse Cloudリソースのセットアップを開始できます。

<br />

<img src={require('./images/aws-marketplace-payg-10.png').default}
    alt='ClickHouse Cloud AWS Marketplace 請求確認'
    class='image'
    style={{width: '300px'}}
/>

<br />

<img src={require('./images/aws-marketplace-payg-11.png').default}
    alt='ClickHouse Cloud 新しいサービスページ'
    class='image'
    style={{width: '400px'}}
/>

<br />

10. サインアップ確認のメールを受け取るはずです：

<br />

<img src={require('./images/aws-marketplace-payg-12.png').default}
    alt='AWS Marketplace 確認メール'
    class='image'
    style={{width: '500px'}}
/>

<br />

問題が発生した場合は、[サポートチーム](https://clickhouse.com/support/program)にお気軽にご連絡ください。
