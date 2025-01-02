---
slug: /ja/cloud/billing/marketplace/gcp-marketplace-payg
title: GCP Marketplace PAYG
description: GCP Marketplace で ClickHouse Cloud に PAYG 形式でサブスクライブする。
keywords: [gcp, marketplace, billing, payg]
---

[GCP Marketplace](https://console.cloud.google.com/marketplace) を通じて、PAYG（従量課金制）公開オファーで ClickHouse Cloud を始めましょう。

## 前提条件

- 請求管理者によって購入権限が有効化された GCP プロジェクト。
- GCP Marketplace で ClickHouse Cloud にサブスクライブするには、購入権限を持ったアカウントでログインし、適切なプロジェクトを選択する必要があります。

## 申し込み手順

1. [GCP Marketplace](https://cloud.google.com/marketplace) にアクセスし、ClickHouse Cloud を検索します。適切なプロジェクトが選択されていることを確認してください。

<br />

<img src={require('./images/gcp-marketplace-payg-1.png').default}
    alt='GCP Marketplace ホームページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

2. [リスト](https://console.cloud.google.com/marketplace/product/clickhouse-public/clickhouse-cloud)をクリックし、**購読** ボタンを押します。

<br />

<img src={require('./images/gcp-marketplace-payg-2.png').default}
    alt='GCP Marketplace の ClickHouse Cloud'
    class='image'
    style={{width: '500px'}}
/>

<br />

3. 次の画面でサブスクリプションを設定します：

- プランはデフォルトで "ClickHouse Cloud" になります
- サブスクリプション期間は "月次"
- 適切な課金アカウントを選択
- 利用条件を確認し、**購読** ボタンをクリック

<br />

<img src={require('./images/gcp-marketplace-payg-3.png').default}
    alt='GCP Marketplace でのサブスクリプション設定'
    class='image'
    style={{width: '400px'}}
/>

<br />

4. **購読** をクリックすると、**ClickHouse へのサインアップ** というモーダルが表示されます。

<br />

<img src={require('./images/gcp-marketplace-payg-4.png').default}
    alt='GCP Marketplace サインアップモーダル'
    class='image'
    style={{width: '400px'}}
/>

<br />

5. ここで設定はまだ完了していません。**アカウントを設定** をクリックして ClickHouse Cloud にリダイレクトし、サインアップを完了してください。

6. ClickHouse Cloud にリダイレクトしたら、既存のアカウントでログインするか、新しいアカウントで登録します。このステップは、ClickHouse Cloud の組織を GCP Marketplace の請求に紐付けるために非常に重要です。

<br />

<img src={require('./images/aws-marketplace-payg-6.png').default}
    alt='ClickHouse Cloud サインインページ'
    class='image'
    style={{width: '300px'}}
/>

<br />

新しい ClickHouse Cloud ユーザーの場合は、ページ下部の **登録** をクリックします。新しいユーザーを作成し、メールを確認するように促されます。メールを確認した後は、ClickHouse Cloud のログインページを閉じ、[https://clickhouse.cloud](https://clickhouse.cloud) で新しいユーザー名を使用してログインできます。

<br />

<img src={require('./images/aws-marketplace-payg-7.png').default}
    alt='ClickHouse Cloud サインアップページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

新規ユーザーの場合、事業に関する基本的な情報を提供する必要があることに注意してください。以下のスクリーンショットを参照してください。

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

既存の ClickHouse Cloud ユーザーであれば、単に認証情報を使用してログインします。

7. ログインが成功すると、新しい ClickHouse Cloud の組織が作成されます。この組織は GCP の請求アカウントに接続され、すべての使用量が GCP アカウントを通じて請求されます。

8. ログイン後に、請求が GCP Marketplace に紐付けられていることを確認し、ClickHouse Cloud のリソースを設定し始めることができます。

<br />

<img src={require('./images/gcp-marketplace-payg-5.png').default}
    alt='ClickHouse Cloud サインインページ'
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

9. サインアップを確認するメールを受け取るはずです:

<br />
<br />

<img src={require('./images/gcp-marketplace-payg-6.png').default}
    alt='GCP Marketplace 確認メール'
    class='image'
    style={{width: '300px'}}
/>

<br />

<br />

問題が発生した場合は、どうぞ [サポートチーム](https://clickhouse.com/support/program) までご連絡ください。
