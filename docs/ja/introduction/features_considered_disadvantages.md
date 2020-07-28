# 欠点と考えられるClickHouseの機能

1. 本格的なトランザクションはありません。
2. 既に挿入されたデータの変更または削除を、高頻度かつ低遅延に行う機能はありません。 [GDPR](https://gdpr-info.eu)に準拠するなど、データをクリーンアップまたは変更するために、バッチ削除およびバッチ更新が利用可能です。
3. インデックスが疎であるため、ClickHouseは、キーで単一行を取得するようなクエリにはあまり適していません。

[Original article](https://clickhouse.yandex/docs/en/introduction/features_considered_disadvantages/) <!--hide-->

