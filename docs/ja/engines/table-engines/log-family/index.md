---
slug: /ja/engines/table-engines/log-family/
sidebar_position: 20
sidebar_label: Logファミリー
---

# Log エンジンファミリー

これらのエンジンは、多くの小さなテーブル（約100万行まで）をすばやく書き込み、後で全体として読む必要があるシナリオのために開発されました。

ファミリーのエンジン:

- [StripeLog](/docs/ja/engines/table-engines/log-family/stripelog.md)
- [Log](/docs/ja/engines/table-engines/log-family/log.md)
- [TinyLog](/docs/ja/engines/table-engines/log-family/tinylog.md)

`Log`ファミリーのテーブルエンジンは、[HDFS](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-hdfs)や[S3](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)の分散ファイルシステムにデータを保存できます。

## 共通のプロパティ {#common-properties}

エンジンの特徴:

- データをディスクに保存します。

- データをファイルの末尾に追加して書き込みます。

- 同時データアクセス用のロックをサポートします。

    `INSERT`クエリの間、テーブルはロックされ、他の読み書きクエリはテーブルがアンロックされるのを待ちます。データ書き込みのクエリがない場合、任意の数のデータ読み取りクエリを並行して実行できます。

- [ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#alter-mutations)をサポートしません。

- インデックスをサポートしません。

    これにより、データ範囲の`SELECT`クエリは効率的ではありません。

- データをアトミックに書き込みません。

    たとえば、サーバーの異常終了などで書き込み操作が中断されると、データが破損したテーブルが得られる可能性があります。

## 差異 {#differences}

`TinyLog`エンジンは、ファミリー内で最もシンプルで、機能と効率が最も低いです。`TinyLog`エンジンは、単一クエリで複数のスレッドによる並行データ読み取りをサポートしません。単一のクエリから並行読み取りをサポートする他のエンジンよりもデータを読むのが遅く、各カラムを別々のファイルに保存するため、`Log`エンジンとほぼ同じ数のファイルディスクリプタを使用します。簡単なシナリオでのみ使用してください。

`Log`および`StripeLog`エンジンは並行データ読み取りをサポートしています。データを読み取る際、ClickHouseは複数のスレッドを使用し、各スレッドは別々のデータブロックを処理します。`Log`エンジンはテーブルの各カラムに対して別々のファイルを使用します。一方、`StripeLog`はすべてのデータを1つのファイルに格納します。その結果、`StripeLog`エンジンは使用するファイルディスクリプタが少なくなりますが、`Log`エンジンの方がデータ読み取りの効率が高くなります。
