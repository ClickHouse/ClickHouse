<details><summary>GCS バケットと HMAC キーを作成する</summary>

### ch_bucket_us_east1

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-bucket-1.png)

### ch_bucket_us_east4

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-bucket-2.png)

### アクセスキーを生成する

### サービスアカウントの HMAC キーとシークレットを作成する

**Cloud Storage > Settings > Interoperability** を開き、既存の **Access key** を選択するか、**CREATE A KEY FOR A SERVICE ACCOUNT** を選択します。このガイドでは、新しいサービスアカウントの新しいキーを作成する手順を説明します。

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-create-a-service-account-key.png)

### 新しいサービスアカウントを追加する

すでにサービスアカウントが存在しないプロジェクトの場合は、**CREATE NEW ACCOUNT** をクリックします。

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-create-service-account-0.png)

サービスアカウントを作成するには3つのステップがあります。最初のステップでは、アカウントに意味のある名前、ID、説明を付けます。

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-create-service-account-a.png)

Interoperability 設定ダイアログでは、IAM ロールとして **Storage Object Admin** ロールが推奨されます。ステップ2でそのロールを選択します。

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-create-service-account-2.png)

ステップ3はオプションであり、このガイドでは使用しません。ポリシーに基づいて、ユーザーにこれらの特権を与えることができます。

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-create-service-account-3.png)

サービスアカウントの HMAC キーが表示されます。この情報を保存してください。ClickHouse の設定で使用します。

![バケットを追加](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-guide-key.png)

</details>
