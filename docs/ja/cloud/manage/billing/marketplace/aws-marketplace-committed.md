---
slug: /ja/cloud/billing/marketplace/aws-marketplace-committed-contract
title: AWS Marketplace コミットメント契約
description: AWS Marketplace (コミットメント契約) 経由で ClickHouse Cloud に登録する方法
keywords: [aws, amazon, marketplace, billing, committed, committed contract]
---

[AWS Marketplace](https://aws.amazon.com/marketplace) を通じてコミットメント契約で ClickHouse Cloud を始めましょう。コミットメント契約、またはプライベートオファーとも呼ばれるものは、顧客が特定の期間にわたり ClickHouse Cloud に一定の金額を支払うことを約束できる契約です。

## 前提条件

- 特定の契約条件に基づく ClickHouse からのプライベートオファー。

## サインアップの手順

1. プライベートオファーを確認して受理するリンクが記載されたメールを受け取るはずです。

<br />

<img src={require('./images/aws-marketplace-committed-1.png').default}
    alt='AWS Marketplace プライベートオファーメール'
    class='image'
    style={{width: '400px'}}
/>

<br />

2. メールの **Review Offer** リンクをクリックしてください。これにより、プライベートオファーの詳細が記載された AWS Marketplace ページに移動します。プライベートオファーを受け入れる際には、契約オプションのプルダウンでユニット数を1に設定してください。

3. AWS ポータルでの購読手続きを完了し、**Set up your account** をクリックします。この時点で ClickHouse Cloud にリダイレクトし、新規アカウントを登録するか既存アカウントでサインインしてください。このステップを完了しないと、AWS Marketplace サブスクリプションを ClickHouse Cloud とリンクすることができません。

4. ClickHouse Cloud にリダイレクトしたら、既存のアカウントでログインするか、新しいアカウントを登録してください。このステップは、ClickHouse Cloud オーガニゼーションを AWS Marketplace の請求設定に接続するため非常に重要です。

<br />

<img src={require('./images/aws-marketplace-payg-6.png').default}
    alt='ClickHouse Cloud サインインページ'
    class='image'
    style={{width: '300px'}}
/>

<br />

新しい ClickHouse Cloud ユーザーの場合は、ページ下部の **Register** をクリックします。新しいユーザーを作成し、メールを確認するように求められます。メールを確認した後は、ClickHouse Cloud ログインページを離れ、新しいユーザー名を使用して [https://clickhouse.cloud](https://clickhouse.cloud) にログインできます。

<br />

<img src={require('./images/aws-marketplace-payg-7.png').default}
    alt='ClickHouse Cloud 新規登録ページ'
    class='image'
    style={{width: '500px'}}
/>

<br />

新規ユーザーの場合は、ビジネスの基本情報も提供する必要があることに注意してください。以下のスクリーンショットをご覧ください。

<br />

<img src={require('./images/aws-marketplace-payg-8.png').default}
    alt='ClickHouse Cloud 新規登録情報フォーム'
    class='image'
    style={{width: '400px'}}
/>

<br />

<br />

<img src={require('./images/aws-marketplace-payg-9.png').default}
    alt='ClickHouse Cloud 新規登録情報フォーム 2'
    class='image'
    style={{width: '400px'}}
/>

<br />

既存の ClickHouse Cloud ユーザーの場合は、単に資格情報を使用してログインしてください。

5. ログインが成功すると、新しい ClickHouse Cloud オーガニゼーションが作成されます。このオーガニゼーションは AWS の請求アカウントに接続され、すべての利用が AWS アカウントを通じて請求されます。

6. ログイン後、請求が AWS Marketplace に結びついていることを確認し、ClickHouse Cloud リソースのセットアップを開始できます。

<br />

<img src={require('./images/aws-marketplace-payg-10.png').default}
    alt='AWS Marketplace の請求を表示する ClickHouse Cloud'
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

6. サインアップ確認メールを受け取るはずです：

<br />

<img src={require('./images/aws-marketplace-payg-12.png').default}
    alt='AWS Marketplace 確認メール'
    class='image'
    style={{width: '500px'}}
/>

<br />

何か問題が発生した場合は、お気軽に[サポートチーム](https://clickhouse.com/support/program) にお問い合わせください。
