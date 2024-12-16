---
slug: /ja/cloud/security/secure-s3
sidebar_label: S3データに安全にアクセスする
title: S3データに安全にアクセスする
---

この記事では、ClickHouse Cloudの顧客が役割ベースのアクセスを活用してAmazon Simple Storage Service (S3) に認証し、安全にデータにアクセスする方法を説明します。

## はじめに

安全なS3アクセスのセットアップに進む前に、その仕組みについて理解することが重要です。以下は、ClickHouseサービスが顧客のAWSアカウント内の役割を引き受けてプライベートS3バケットにアクセスする方法の概要です。

![secures3](@site/docs/ja/cloud/security/images/secures3.jpg)

このアプローチにより、顧客は全てのS3バケットへのアクセスを一か所（引き受けた役割のIAMポリシー）で管理することができ、バケットポリシーをすべて見直してアクセスを追加または削除する必要がなくなります。

## セットアップ

### ClickHouseサービスのIAMロールArnを取得

1 - ClickHouseクラウドアカウントにログインします。

2 - 統合を作成したいClickHouseサービスを選択します。

3 - **設定** タブを選択します。

4 - ページの下部にある **このサービスについて** セクションまでスクロールします。

5 - 下に示されているように、サービスに属する **IAMロール** 値をコピーします。

![s3info](@site/docs/ja/cloud/security/images/secures3_arn.jpg)

### IAM引き受けロールの設定

#### オプション1: CloudFormationスタックを使用してデプロイ

1 - IAMロールの作成と管理の権限を持つIAMユーザーで、ウェブブラウザからAWSアカウントにログインします。

2 - [このURL](https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/quickcreate?templateURL=https://s3.us-east-2.amazonaws.com/clickhouse-public-resources.clickhouse.cloud/cf-templates/secure-s3.yaml&stackName=ClickHouseSecureS3) にアクセスしてCloudFormationスタックを構成します。

3 - ClickHouseサービスに属する **IAMロール** を入力（または貼り付け）します。

4 - CloudFormationスタックを設定します。以下は各パラメータについての追加情報です。

| パラメータ              | デフォルト値              | 説明                                                                                                |
| :---                      |    :----:            | :----                                                                                              |
| RoleName                  | ClickHouseAccess-001 | ClickHouse CloudがあなたのS3バケットにアクセスするのに使用する新しいロールの名前                       |
| Role Session Name         |      *               | Role Session Nameは、バケットをさらに保護するための共有シークレットとして使用できます。                  |
| ClickHouse Instance Roles |                      | このSecure S3統合を使用できるClickHouseサービス IAMロールのカンマ区切りリスト。                |
| Bucket Access             |    Read              | 提供されたバケットに対するアクセスレベルを設定します。                                                |
| Bucket Names              |                      | このロールがアクセス権を持つ**バケット名**のカンマ区切りリスト。                                       |

*注意*: フルバケットArnではなく、バケット名のみを入力してください。

5 - **AWS CloudFormationがカスタム名付きのIAMリソースを作成する可能性があります。** チェックボックスを選択します。

6 - 右下の **スタックを作成** ボタンをクリックします。

7 - CloudFormationスタックがエラーなく完了することを確認します。

8 - CloudFormationスタックの **Outputs** を選択します。

9 - この統合に必要な **RoleArn** 値をコピーします。これにより、S3バケットにアクセスできます。

![s3info](@site/docs/ja/cloud/security/images/secures3_output.jpg)

#### オプション2: IAMロールを手動で作成する

1 - IAMロールの作成と管理の権限を持つIAMユーザーで、ウェブブラウザからAWSアカウントにログインします。

2 - IAMサービスコンソールに移動します。

3 - 以下のIAMポリシーと信頼ポリシーを持つ新しいIAMロールを作成します。

信頼ポリシー（{ClickHouse_IAM_ARN}をClickHouseインスタンスに属するIAMロールのarnに置き換えてください）:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "{ClickHouse_IAM_ARN}"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

IAMポリシー（{BUCKET_NAME}をあなたのバケット名に置き換えてください）:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::{BUCKET_NAME}"
            ],
            "Effect": "Allow"
        },
        {
            "Action": [
                "s3:Get*",
                "s3:List*"
            ],
            "Resource": [
                "arn:aws:s3:::{BUCKET_NAME}/*"
            ],
            "Effect": "Allow"
        }
    ]
}
```

4 - 作成後の新しい **IAMロールArn** をコピーします。これにより、S3バケットにアクセスできます。

## ClickHouseAccessロールでS3バケットにアクセス

ClickHouse Cloudには、新しく作成したロールを使用して`extra_credentials`をS3テーブル関数の一部として指定できる新機能があります。以下は、新しく作成したロールを使用してクエリを実行する例です。

```
describe table s3('https://s3.amazonaws.com/BUCKETNAME/BUCKETOBJECT.csv','CSVWithNames',extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001'))
```

以下は、`role_session_name`を共有シークレットとして使用してバケットからデータをクエリする例です。`role_session_name`が正しくない場合、この操作は失敗します。

```
describe table s3('https://s3.amazonaws.com/BUCKETNAME/BUCKETOBJECT.csv','CSVWithNames',extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001', role_session_name = 'secret-role-name'))
```

:::note
データ転送料を削減するために、ソースS3がClickHouse Cloudサービスと同じ地域にあることをお勧めします。詳細については、[S3料金]( https://aws.amazon.com/s3/pricing/) を参照してください。
:::
