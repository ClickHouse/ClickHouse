<details><summary>S3バケットとIAMユーザーの作成</summary>

この記事では、AWS IAMユーザーを設定し、S3バケットを作成し、ClickHouseをそのバケットをS3ディスクとして使用するように設定する基本を説明しています。使用する権限を決定するためにセキュリティチームと協力し、これらを出発点として考えてください。

### AWS IAMユーザーの作成
この手順では、ログインユーザーではなくサービスアカウントユーザーを作成します。
1.  AWS IAM 管理コンソールにログインします。

2. 「ユーザー」で、**ユーザーを追加** を選択します。

  ![create_iam_user_0](@site/docs/ja/_snippets/images/s3/s3-1.png)

3. ユーザー名を入力し、資格情報の種類を **アクセスキー - プログラムによるアクセス** に設定し、**次: 権限** を選択します。

  ![create_iam_user_1](@site/docs/ja/_snippets/images/s3/s3-2.png)

4. ユーザーをグループに追加せず、**次: タグ** を選択します。

  ![create_iam_user_2](@site/docs/ja/_snippets/images/s3/s3-3.png)

5. タグを追加する必要がなければ、**次: 確認** を選択します。

  ![create_iam_user_3](@site/docs/ja/_snippets/images/s3/s3-4.png)

6. **ユーザーを作成** を選択します。

    :::note
    ユーザーに権限がないという警告メッセージは無視できます。次のセクションでバケットに対してユーザーに権限が付与されます。
    :::

  ![create_iam_user_4](@site/docs/ja/_snippets/images/s3/s3-5.png)

7. ユーザーが作成されました。**表示** をクリックし、アクセスキーとシークレットキーをコピーします。
:::note
これがシークレットアクセスキーが利用可能な唯一のタイミングですので、キーを別の場所に保存してください。
:::

  ![create_iam_user_5](@site/docs/ja/_snippets/images/s3/s3-6.png)

8. 閉じるをクリックし、ユーザー画面でそのユーザーを見つけます。

  ![create_iam_user_6](@site/docs/ja/_snippets/images/s3/s3-7.png)

9. ARN（Amazon Resource Name）をコピーし、バケットのアクセスポリシーを設定する際に使用するために保存します。

  ![create_iam_user_7](@site/docs/ja/_snippets/images/s3/s3-8.png)

### S3バケットの作成
1. S3バケットセクションで、**バケットの作成** を選択します。

  ![create_s3_bucket_0](@site/docs/ja/_snippets/images/s3/s3-9.png)

2. バケット名を入力し、他のオプションはデフォルトのままにします。
:::note
バケット名はAWS全体で一意である必要があります。組織内だけでなく、一意でない場合はエラーが発生します。
:::
3. `すべてのパブリックアクセスをブロック` を有効のままにします。パブリックアクセスは必要ありません。

  ![create_s3_bucket_2](@site/docs/ja/_snippets/images/s3/s3-a.png)

4. ページの下部にある **バケットの作成** を選択します。

  ![create_s3_bucket_3](@site/docs/ja/_snippets/images/s3/s3-b.png)

5. リンクを選択し、ARNをコピーして、バケットのアクセスポリシーを設定するときに使用するために保存します。

6. バケットが作成されたら、S3バケットリストで新しいS3バケットを見つけ、リンクを選択します。

  ![create_s3_bucket_4](@site/docs/ja/_snippets/images/s3/s3-c.png)

7. **フォルダを作成** を選択します。

  ![create_s3_bucket_5](@site/docs/ja/_snippets/images/s3/s3-d.png)

8. ClickHouse S3ディスクのターゲットとなるフォルダ名を入力し、**フォルダを作成** を選択します。

  ![create_s3_bucket_6](@site/docs/ja/_snippets/images/s3/s3-e.png)

9. フォルダがバケットリストに表示されるはずです。

  ![create_s3_bucket_7](@site/docs/ja/_snippets/images/s3/s3-f.png)

10. 新しいフォルダのチェックボックスを選択し、**URLをコピー** をクリックします。コピーしたURLは、次のセクションでのClickHouseストレージ設定で使用します。

  ![create_s3_bucket_8](@site/docs/ja/_snippets/images/s3/s3-g.png)

11. **権限** タブを選択し、**バケットポリシー** セクションの **編集** ボタンをクリックします。

  ![create_s3_bucket_9](@site/docs/ja/_snippets/images/s3/s3-h.png)

12. 以下の例のようにバケットポリシーを追加します:
```json
{
	"Version": "2012-10-17",
	"Id": "Policy123456",
	"Statement": [
		{
			"Sid": "abc123",
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::921234567898:user/mars-s3-user"
			},
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::mars-doc-test",
				"arn:aws:s3:::mars-doc-test/*"
			]
		}
	]
}
```

```response
|パラメータ | 説明 | 例 |
|----------|-------------|----------------|
|Version | ポリシーインタープリタのバージョン、そのままにしておく | 2012-10-17 |
|Sid | ユーザー定義のポリシーID | abc123 |
|Effect | ユーザー要求が許可されるか拒否されるか | Allow |
|Principal | 許可されるアカウントまたはユーザー | arn:aws:iam::921234567898:user/mars-s3-user |
|Action | バケット上で許可される操作| s3:*|
|Resource | バケット内で操作が許可されるリソース | "arn:aws:s3:::mars-doc-test", "arn:aws:s3:::mars-doc-test/*" |
```

:::note
使用する権限を決定するためにセキュリティチームと協力し、これらを出発点として考えてください。
ポリシーと設定の詳細については、AWSドキュメントをご参照ください：
https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-policy-language-overview.html
:::

13. ポリシー設定を保存します。

</details>
