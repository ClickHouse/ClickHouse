---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 58
toc_title: "\u6982\u8981"
---

# 藩 {#domains}

ドメインは、既存の基本タイプの上にいくつかの追加機能を追加する特別な目的のタイプですが、基になるデータタイプのオンワイヤとオンディス 現時点では、clickhouseはユーザー定義ドメインをサポートしていません。

ドメインは、対応する基本タイプを使用できる任意の場所で使用できます。:

-   ドメイン型の列を作成する
-   ドメイン列からの値の読み取り/書き込み
-   基本型をインデックスとして使用できる場合は、インデックスとして使用します
-   ドメイン列の値を持つ関数の呼び出し

### ドメインの追加機能 {#extra-features-of-domains}

-   明示的な列タイプ名in `SHOW CREATE TABLE` または `DESCRIBE TABLE`
-   人間に優しいフォーマットからの入力 `INSERT INTO domain_table(domain_column) VALUES(...)`
-   人間に優しいフォーマットへの出力 `SELECT domain_column FROM domain_table`
-   人間に優しい形式で外部ソースからデータをロードする: `INSERT INTO domain_table FORMAT CSV ...`

### 制限 {#limitations}

-   ベース型のインデックス列をドメイン型に変換できません `ALTER TABLE`.
-   できない暗黙的に変換し文字列値がコマンドライン値を挿入する際、データから別のテーブルや列.
-   ドメインは、格納された値に制約を追加しません。

[元の記事](https://clickhouse.tech/docs/en/data_types/domains/overview) <!--hide-->
