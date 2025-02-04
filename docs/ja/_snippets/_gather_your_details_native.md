ClickHouseにネイティブTCPで接続するには、次の情報が必要です。

- **HOSTとPORT**: 通常、TLSを使用している場合はポートは9440、TLSを使用していない場合は9000です。

- **データベース名**: デフォルトでは、`default`という名前のデータベースがあります。接続したいデータベースの名前を使用してください。

- **ユーザー名とパスワード**: デフォルトのユーザー名は`default`です。使用するケースに適したユーザー名を利用してください。

ClickHouse Cloudサービスの詳細は、ClickHouse Cloudコンソールで確認できます。接続するサービスを選択し、**Connect**をクリックします。

![ClickHouse Cloud service connect button](@site/docs/ja/_snippets/images/cloud-connect-button.png)

**Native** を選択すると、例として `clickhouse-client` コマンドで使用可能な詳細が表示されます。

![ClickHouse Cloud Native TCP connection details](@site/docs/ja/_snippets/images/connection-details-native.png)

セルフマネージドのClickHouseを使用している場合、接続の詳細はClickHouse管理者によって設定されます。
