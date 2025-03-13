---
sidebar_label: ユーザー管理の暗号化キー
slug: /ja/cloud/security/cmek
title: ユーザー管理の暗号化キー (CMEK)
---

# ユーザー管理の暗号化キー (CMEK)

ClickHouse Cloudは、ユーザー自身のキー管理サービス (KMS) キーを活用してサービスを保護することを可能にします。私たちは、ClickHouseの組み込み機能である[データ暗号化用の仮想ファイルシステム機能](/docs/ja/operations/storing-data#encrypted-virtual-file-system)を利用し、あなたのデータを暗号化・保護します。ClickHouse Cloudサービスで使用されるデータ暗号化キーは、[エンベロープ暗号化](https://docs.aws.amazon.com/wellarchitected/latest/financial-services-industry-lens/use-envelope-encryption-with-customer-master-keys.html)として知られるプロセスを通じて、ユーザーが提供したKMSキーを使用して暗号化・保護されます。この機能を動作させるために必要なのは、ランタイムでデータ暗号化キーを復号・暗号化するためのKMSキーへのアクセスです。

この機能は ClickHouse Cloud のプロダクションサービスに限定されており、この機能を有効にするには[support](/docs/ja/cloud/support)にご連絡ください。ユーザー管理の暗号化キーは、サービス作成時に指定する必要があり、既存のサービスではこのオプションを使用できません。[バックアップと復元](#backup-and-restore)を確認して代替オプションをご覧ください。

現在サポートされている KMS プロバイダー:

- AWSにホストされているサービス向けの[AWS Key Management Service](https://aws.amazon.com/kms)

近日提供予定:

- Azureにホストされているサービス向けの[Azure Key Vault](https://azure.microsoft.com/en-us/products/key-vault)
- GCPにホストされているサービス向けの[GCP Cloud Key Management](https://cloud.google.com/security-key-management)
- AWS、Azure、GCPにホストされているサービス向けの[Hashicorp Vault](https://www.hashicorp.com/products/vault)

:::warning
ClickHouse Cloud サービスの暗号化に使用されたKMSキーを削除すると、ClickHouseサービスは停止され、データおよび既存のバックアップは取得不可能になります。
:::

## 手順1. KMSキーの作成

### AWS KMSを使用

AWSコンソール、CloudFormation スタック、またはTerraform プロバイダを使用してAWS KMSキーを作成できます。以下にそれぞれの手順を説明します。

#### オプション1. AWSコンソールを使用してKMSキーを手動で作成

*注意: 既に使用したいKMSキーがある場合は、次のステップに進んでください。*

1. AWSアカウントにログインし、Key Management Service に移動します。
2. 左側のメニューから__ユーザー管理キー__を選択します。
3. 右上の__キーを作成__をクリックします。
4. __キータイプ__「対称キー」と__キー使用__「暗号化および復号化」を選択し、次へをクリックします。
5. キーのエイリアス（表示名）を入力し、次へをクリックします。
6. キー管理者を選択し、次へをクリックします。
7. （オプション）キーのユーザーを選択し、次へをクリックします。
8. __キー ポリシー__の下に、以下のコードスニペットを追加します:

    ```json
    {
        "Sid": "Allow ClickHouse Access",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::576599896960:role/prod-kms-request-role"
            },
            "Action": ["kms:GetPublicKey",
            "kms:Decrypt",
            "kms:GenerateDataKeyPair",
            "kms:Encrypt",
            "kms:GetKeyRotationStatus",
            "kms:GenerateDataKey",
            "kms:DescribeKey"],
            "Resource": "*"
    }
    ```

    ![暗号化キー ポリシー](@site/docs/ja/_snippets/images/cmek1.png)

9. 完了をクリックします。
10. 作成したキーのエイリアスをクリックします。
11. コピー ボタンを使用してARNをコピーします。

#### オプション2. CloudFormationスタックを使用してKMSキーを設定または作成

ClickHouseは、キーのためのAWSポリシーをデプロイするための簡単なCloudFormationスタックを提供します。この方法は、既存のKMSキーおよびClickHouse Cloud統合のための新しいKMSキーの作成の両方をサポートします。

##### 既存のKMSキーを使用

1. AWS アカウントにログインします。
2. [このリンク](https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/quickcreate?templateURL=https://s3.us-east-2.amazonaws.com/clickhouse-public-resources.clickhouse.cloud/cf-templates/cmek.yaml&stackName=ClickHouseBYOK&param_KMSCreate=false&param_ClickHouseRole=arn:aws:iam::576599896960:role/prod-kms-request-role)を訪問してCloudFormation テンプレートを準備します。
3. 使用したいKMSキーのARNを入力します（カンマで区切り、スペースを空けないこと）。
4. 「AWS CloudFormationがカスタム名でIAMリソースを作成する可能性があることを認識しています。」に同意し、__スタックを作成__をクリックします。
5. 次のステップで必要なスタック出力の`RoleArn`と`KeyArn`を書き留めます。

##### 新しいKMSキーを作成

1. AWS アカウントにログインします。
2. [このリンク](https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/quickcreate?templateURL=https://s3.us-east-2.amazonaws.com/clickhouse-public-resources.clickhouse.cloud/cf-templates/cmek.yaml&stackName=ClickHouseBYOK&param_KMSCreate=true&param_ClickHouseRole=arn:aws:iam::576599896960:role/prod-kms-request-role)を訪問してCloudFormation テンプレートを準備します。
3. 「AWS CloudFormationがカスタム名でIAMリソースを作成する可能性があることを認識しています。」に同意し、__スタックを作成__をクリックします。
4. 次のステップで必要なスタック出力の`KeyArn`を書き留めます。

#### オプション3. Terraformを通じてKMSキーを作成

キーをTerraformを使用してデプロイしたいユーザー向けに、AWSプロバイダのドキュメントは[こちら](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kms_key)をご覧ください。

## 手順2. ユーザー管理の暗号化キーを使用してClickHouseサービスを開始

1. ClickHouse Cloudアカウントにログインします。
2. サービス画面に移動します。
3. __新しいサービス__ をクリックします。
4. クラウドプロバイダー、リージョンを選択し、サービスに名前を付けます。
5. __暗号化キーの設定 (CMEK)__をクリックします。例はAWS KMSプロバイダーを使用して示されています。
6. ウィンドウの右側のフィールドにAWS ARNを貼り付けます。

    ![暗号化設定](@site/docs/ja/_snippets/images/cmek2.png)

7. システムは暗号化キーがアクセス可能であることを確認します。
8. AWS ARNボックスの上に__有効__メッセージが表示されたら__サービスを作成__をクリックします。
9. サービス画面のサービスタイルの右上隅にキーアイコンが表示され、暗号化されていることを知らせます。

    ![サービス暗号化済み](@site/docs/ja/_snippets/images/cmek3.png)

## バックアップと復元

バックアップは、関連するサービスと同じキーを使用して暗号化されます。暗号化されたバックアップを復元することで、元のインスタンスと同じKMSキーを使用する暗号化済みインスタンスが作成されます。KMSキーを回転させることも可能です。詳細については[キーの回転](#key-rotation)を参照してください。

暗号化済みインスタンスは、非暗号化バックアップを復元し、新しいサービスのために希望するKMSキーを指定することで作成できますので、[support](/docs/ja/cloud/support)にご連絡ください。

## KMS キーポーラー

エンベロープ暗号化を使用する場合、提供されたKMSキーが依然として有効であることを定期的に確認する必要があります。KMSキーのアクセスを10分ごとに確認します。アクセスが無効になった時点でClickHouseサービスを停止します。サービスを再開するには、このガイドの手順に従ってアクセスを復活させ、それからサービスを開始してください。

この機能の性質上、KMSキーが削除された後にClickHouse Cloudサービスを復旧することはできません。これを防ぐために、ほとんどのプロバイダーはキーをすぐに削除せず、削除をスケジュールします。プロバイダーのドキュメントを確認するか、このプロセスについて[support](/docs/ja/cloud/support)にお問い合わせください。

## キーの回転

キー回転は同じKMSプロバイダー内でサポートされています。これは新しいKMSキーを使用してデータ暗号化キーを再暗号化し、このリクエストはClickHouseサービスのダウンタイムなしで即時に処理されます。この操作を実行するには、設定されたKMSキーと新しいKMSキーの両方にアクセスできることを確認し、KMSキー情報を添えて[support](/docs/ja/cloud/support)にご連絡ください。

## パフォーマンス

本ページで指定されているように、私たちはClickHouseの組み込み機能である[データ暗号化用の仮想ファイルシステム機能](/docs/ja/operations/storing-data#encrypted-virtual-file-system)を利用し、データを暗号化・保護しています。

この機能で使用されるアルゴリズムは`AES_256_CTR`であり、ワークロードに応じて5-15%のパフォーマンス低下が予想されます：

![CMEKパフォーマンス低下](@site/docs/ja/_snippets/images/cmek-performance.png)
