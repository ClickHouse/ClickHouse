---
slug: /ja/engines/table-engines/special/view
sidebar_position: 90
sidebar_label: View
---

# View テーブルエンジン

ビューを実装するために使用されます（詳細については、`CREATE VIEW クエリ`を参照してください）。データを保存することはなく、指定された`SELECT`クエリのみを保存します。テーブルから読み込む際には、このクエリを実行し、クエリから不要なカラムをすべて削除します。
