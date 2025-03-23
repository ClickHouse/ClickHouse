---
slug: /ja/faq/operations/delete-old-data
title: ClickHouseテーブルから古いレコードを削除できますか？
toc_hidden: true
toc_priority: 20
---

# ClickHouseテーブルから古いレコードを削除できますか？ {#is-it-possible-to-delete-old-records-from-a-clickhouse-table}

短い答えは「はい」です。ClickHouseは、古いデータを削除してディスクスペースを解放するための複数の仕組みを提供しています。各仕組みは異なるシナリオに対応しています。

## 有効期限 (TTL) {#ttl}

ClickHouseは、条件が発生したときに自動的に値を削除することを可能にします。この条件は、通常はタイムスタンプカラムに対する固定オフセットとして、任意のカラムに基づく式として設定されます。

このアプローチの主要な利点は、TTLが設定されると、データの削除がバックグラウンドで自動的に行われ、外部システムがトリガーを必要としないことです。

:::note
TTLは、データを[/dev/null](https://en.wikipedia.org/wiki/Null_device)に移動するだけでなく、SSDからHDDへのように、異なるストレージシステム間でのデータの移動にも使用できます。
:::

[有効期限 (TTL) の設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)に関する詳細情報。

## DELETE FROM

[DELETE FROM](/docs/ja/sql-reference/statements/delete.md)は、ClickHouseでの標準的なDELETEクエリの実行を可能にします。フィルタ句で対象とされた行は削除としてマークされ、将来の結果セットから除外されます。行のクリーンアップは非同期で行われます。

:::note
DELETE FROMは、バージョン23.3以降で一般に使用可能です。古いバージョンではエクスペリメンタルであり、以下の設定で有効化する必要があります:
```
SET allow_experimental_lightweight_delete = true;
```
:::

## ALTER DELETE {#alter-delete}

ALTER DELETEは非同期バッチ操作を使用して行を削除します。DELETE FROMとは異なり、ALTER DELETE後に実行され、バッチ操作が完了する前のクエリは、削除対象の行を含めます。詳細は[ALTER DELETE](/docs/ja/sql-reference/statements/alter/delete.md)ドキュメントを参照してください。

`ALTER DELETE`は柔軟に古いデータを削除するために発行できます。定期的に行う必要がある場合の主な欠点は、クエリを送信する外部システムが必要になることです。また、ミューテーションは削除する行が1つしかない場合でも完全なパーツをリライトするため、一部のパフォーマンス上の考慮事項があります。

これは、ClickHouseを基にしたシステムを[GDPR](https://gdpr-info.eu)準拠にするための最も一般的なアプローチです。

[ミューテーション](../../sql-reference/statements/alter/index.md#alter-mutations)に関する詳細情報。

## パーティションの削除 {#drop-partition}

`ALTER TABLE ... DROP PARTITION`は、パーティション全体を削除するためのコスト効率の良い方法です。柔軟性には欠け、テーブル作成時に適切なパーティションスキームの設定が必要ですが、それでも一般的なケースの多くをカバーします。ミューテーションと同様に、定期的に使用するには外部システムから実行する必要があります。

[パーティション操作](../../sql-reference/statements/alter/partition.md#alter_drop-partition)に関する詳細情報。

## TRUNCATE {#truncate}

テーブルからすべてのデータを削除することは極端ですが、場合によってはそれが必要なこともあります。

[テーブルトランケーション](/docs/ja/sql-reference/statements/truncate.md)に関する詳細情報。
