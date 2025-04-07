---
slug: /ja/sql-reference/data-types/domains/
sidebar_position: 56
sidebar_label: ドメイン
---

# ドメイン

ドメインは、既存の基本型にいくつかの追加機能を持たせた特別な型であり、基盤となるデータ型のオンワイヤおよびオンディスク形式を保持します。現時点で、ClickHouseはユーザー定義ドメインをサポートしていません。

ドメインは、対応する基本型が使用できるすべての場所で使用できます。たとえば：

- ドメイン型のカラムを作成
- ドメインカラムから/へ値を読み書き
- 基本型がインデックスとして使用できる場合にはそれをインデックスとして使用
- ドメインカラムの値を使って関数を呼び出し

### ドメインの追加機能

- `SHOW CREATE TABLE` または `DESCRIBE TABLE` での明示的なカラム型名
- `INSERT INTO domain_table(domain_column) VALUES(...)` を用いた人間に優しいフォーマットからの入力
- `SELECT domain_column FROM domain_table` のための人間に優しいフォーマットへの出力
- 外部ソースから人間に優しいフォーマットでデータをロード: `INSERT INTO domain_table FORMAT CSV ...`

### 制限事項

- 基本型のインデックスカラムを `ALTER TABLE` でドメイン型に変換できません。
- 別のカラムやテーブルからデータを挿入する際に、文字列値をドメイン値に暗黙的に変換できません。
- ドメインは、保存された値に対して制約を追加しません。
