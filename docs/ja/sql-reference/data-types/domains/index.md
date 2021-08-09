---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u30C9\u30E1\u30A4\u30F3"
toc_priority: 56
toc_title: "\u6982\u8981"
---

# ドメイン {#domains}

ドメインは、既存の基本型の上にいくつかの余分な機能を追加する特殊な目的の型ですが、基になるデータ型のオンワイヤおよびオンディスク形式は 現時点では、ClickHouseはユーザー定義ドメインをサポートしていません。

たとえば、対応する基本タイプを使用できる任意の場所でドメインを使用できます:

-   ドメイン型の列を作成する
-   ドメイン列から/への読み取り/書き込み値
-   基本型をインデックスとして使用できる場合は、インデックスとして使用します
-   ドメイン列の値を持つ関数の呼び出し

### ドメインの追加機能 {#extra-features-of-domains}

-   明示的な列タイプ名 `SHOW CREATE TABLE` または `DESCRIBE TABLE`
-   人間に優しいフォーマットからの入力 `INSERT INTO domain_table(domain_column) VALUES(...)`
-   人間に優しいフォーマットへの出力 `SELECT domain_column FROM domain_table`
-   人間に優しい形式で外部ソースからデータを読み込む: `INSERT INTO domain_table FORMAT CSV ...`

### 制限 {#limitations}

-   基本型のインデックス列をドメイン型に変換できません `ALTER TABLE`.
-   別の列または表からデータを挿入するときに、文字列値を暗黙的にドメイン値に変換できません。
-   ドメインは、格納された値に制約を追加しません。

[元の記事](https://clickhouse.tech/docs/en/data_types/domains/overview) <!--hide-->
