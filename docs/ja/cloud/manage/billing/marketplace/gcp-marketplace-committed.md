---
slug: /ja/cloud/billing/marketplace/gcp-marketplace-committed-contract
title: GCP Marketplace コミット契約
description: GCP Marketplaceを通じてClickHouse Cloudにサブスクライブする（コミット契約）
keywords: [gcp, google, marketplace, billing, committed, committed contract]
---

[GCP Marketplace](https://console.cloud.google.com/marketplace)を通じてClickHouse Cloudを利用するには、コミット契約を行います。コミット契約はプライベートオファーとも呼ばれ、一定期間、ClickHouse Cloudに対して一定額を支出することをコミットする契約です。

## 前提条件

- 特定の契約条件に基づくClickHouseからのプライベートオファー。

## 登録手順

1. プライベートオファーのレビューと承認を求められるメールを受信しているはずです。

<br />

<img src={require('./images/gcp-marketplace-committed-1.png').default}
    alt='GCP Marketplace プライベートオファーのメール'
    class='image'
    style={{width: '300px'}}
/>

<br />

2. メール内の**オファーをレビュー**リンクをクリックします。これにより、プライベートオファーの詳細を含むGCP Marketplaceページに遷移します。

<br />

<img src={require('./images/gcp-marketplace-committed-2.png').default}
    alt='GCP Marketplace オファーの概要'
    class='image'
    style={{width: '300px'}}
/>

<br />

<img src={require('./images/gcp-marketplace-committed-3.png').default}
    alt='GCP Marketplace 価格の概要'
    class='image'
    style={{width: '300px'}}
/>

<br />

3. プライベートオファーの詳細をレビューし、問題がなければ**承認**をクリックします。

<br />

<img src={require('./images/gcp-marketplace-committed-4.png').default}
    alt='GCP Marketplace 承認ページ'
    class='image'
    style={{width: '300px'}}
/>

<br />

4. **製品ページに移動**をクリックします。

<br />

<img src={require('./images/gcp-marketplace-committed-5.png').default}
    alt='GCP Marketplace 承認確認'
    class='image'
    style={{width: '400px'}}
/>

<br />

5. **プロバイダで管理**をクリックします。

<br />

<img src={require('./images/gcp-marketplace-committed-6.png').default}
    alt='GCP Marketplace ClickHouse Cloudページ'
    class='image'
    style={{width: '400px'}}
/>

<br />

ここでClickHouse Cloudにリダイレクトし、サインアップまたはサインインを行うことが不可欠です。このステップを完了しないと、GCP MarketplaceのサブスクリプションをClickHouse Cloudにリンクすることができません。

<br />

<img src={require('./images/gcp-marketplace-committed-7.png').default}
    alt='GCP Marketplace サイト離脱確認モーダル'
    class='image'
    style={{width: '400px'}}
/>

<br />

6. ClickHouse Cloudにリダイレクトされると、既存のアカウントでログインするか、新しいアカウントを登録できます。

<br />

<img src={require('./images/aws-marketplace-payg-6.png').default}
    alt='ClickHouse Cloud サインインページ'
    class='image'
    style={{width: '300px'}}
/>

<br />

新しいClickHouse Cloudユーザーの場合は、ページ下部の**登録**をクリックしてください。新しいユーザーを作成し、メールを確認するように促されます。メールを確認した後、ClickHouse Cloudのログインページを離れ、新しいユーザー名を使用して[https://clickhouse.cloud](https://clickhouse.cloud)でログインできます。

<br />

<img src={require('./images/aws-marketplace-payg-7.png').default}
    alt='ClickHouse Cloud サインアップページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

新しいユーザーの場合、ビジネスに関する基本情報も提供する必要があります。以下のスクリーンショットを参照してください。

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

既存のClickHouse Cloudユーザーの場合は、資格情報を使用してログインするだけです。

7. ログインに成功すると、新しいClickHouse Cloud組織が作成されます。この組織はGCP課金アカウントと接続され、すべての使用量がGCPアカウントを通じて課金されます。

8. ログイン後、課金がGCP Marketplaceに確実に結び付けられていることを確認し、ClickHouse Cloudリソースの設定を開始できます。

<br />

<img src={require('./images/gcp-marketplace-payg-5.png').default}
    alt='ClickHouse Cloud サインインページ'
    class='image'
    style={{width: '300px'}}
/>

<br />

<img src={require('./images/aws-marketplace-payg-11.png').default}
    alt='ClickHouse Cloud 新サービスページ'
    class='image'
    style={{width: '400px'}}
/>

<br />

9. サインアップの確認メールが送信されるはずです：

<br />
<br />

<img src={require('./images/gcp-marketplace-payg-6.png').default}
    alt='GCP Marketplace 確認メール'
    class='image'
    style={{width: '300px'}}
/>

<br />

<br />

問題がある場合は、[サポートチーム](https://clickhouse.com/support/program)にお気軽にお問い合わせください。
