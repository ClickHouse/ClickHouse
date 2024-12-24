---
slug: /ja/integrations/clickpipes/secure-kinesis
sidebar_label: Kinesis ロールベース アクセス
title: Kinesis ロールベース アクセス
---

この記事では、ClickPipes 顧客がロールベース・アクセスを活用して Amazon Kinesis に認証し、安全にデータストリームへアクセスする方法を説明します。

## はじめに

Kinesis への安全なアクセスを設定する前に、そのメカニズムを理解することが重要です。以下は、ClickPipes が顧客の AWS アカウント内でロールを引き受けることで Amazon Kinesis ストリームにアクセスする方法の概要です。

![securekinesis](@site/docs/ja/integrations/data-ingestion/clickpipes/images/securekinesis.jpg)

この方法を使用することで、顧客は IAM ポリシー（引き受けられるロールの IAM ポリシー）内で Kinesis データストリームへのすべてのアクセスを一元管理でき、それぞれのストリームのアクセスポリシーを個別に変更する必要がありません。

## 設定

### ClickHouse サービス IAM ロール Arn の取得

1 - ClickHouse クラウドアカウントにログインします。

2 - 統合したい ClickHouse サービスを選択します。

3 - **設定** タブを選択します。

4 - ページ下部の **このサービスについて** セクションまでスクロールします。

5 - 以下に示すように、サービスに属する **IAM ロール** の値をコピーします。

![s3info](@site/docs/ja/cloud/security/images/secures3_arn.jpg)

### IAM ロールの設定

#### IAM ロールを手動で作成

1 - IAM ユーザーとしての許可がある AWS アカウントにウェブブラウザでログインして、IAM ロールを作成および管理します。

2 - IAM サービスコンソールに移動します。

3 - 以下の IAM および信頼ポリシーを持つ新しい IAM ロールを作成します。IAM ロールの名前は `ClickHouseAccessRole-` で始める必要があることに注意してください。

信頼ポリシー（{ClickHouse_IAM_ARN} を ClickHouse インスタンスに属する IAM ロールの arn で置き換えてください）:

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

IAM ポリシー（{STREAM_NAME} を Kinesis ストリーム名で置き換えてください）:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListShards",
                "kinesis:SubscribeToShard",
                "kinesis:DescribeStreamConsumer",
                "kinesis:RegisterStreamConsumer",
                "kinesis:DeregisterStreamConsumer",
                "kinesis:ListStreamConsumers"
            ],
            "Resource": [
                "arn:aws:kinesis:region:account-id:stream/{STREAM_NAME}"
            ],
            "Effect": "Allow"
        },
        {
            "Action": [
                "kinesis:ListStreams"
            ],
            "Resource": "*",
            "Effect": "Allow"
        }
    ]

}
```

4 - 作成後、新しい **IAM ロール Arn** をコピーします。これが Kinesis ストリームにアクセスするために必要です。
