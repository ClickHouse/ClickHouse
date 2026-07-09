---
description: '既存の基本型を拡張して追加機能を提供する、ClickHouse のドメイン型の概要'
sidebar_label: 'ドメイン'
sidebar_position: 56
slug: /sql-reference/data-types/domains/
title: 'ドメイン'
doc_type: 'reference'
---

ドメインは、既存の基本型に追加機能を加える特別用途の型です。基になるデータ型の on-wire および on-disk フォーマットはそのまま維持されます。現在、ClickHouse はユーザー定義ドメインをサポートしていません。

対応する基本型を使用できる場所であれば、どこでもドメインを使用できます。たとえば、次のとおりです。

* ドメイン型のカラムを作成する
* ドメイン型のカラムの値を読み書きする
* 基本型を索引として使用できる場合は、ドメインも索引として使用する
* ドメイン型のカラムの値を使って関数を呼び出す

<div id="extra-features-of-domains">
  ### Domains の追加機能
</div>

* `SHOW CREATE TABLE` または `DESCRIBE TABLE` における明示的なカラムの型名
* `INSERT INTO domain_table(domain_column) VALUES(...)` による、人が読みやすい形式での入力
* `SELECT domain_column FROM domain_table` の結果を、人が読みやすい形式で出力
* 人が読みやすい形式で外部ソースからデータを読み込む: `INSERT INTO domain_table FORMAT CSV ...`

<div id="limitations">
  ### 制限事項
</div>

* `ALTER TABLE` で、基本型の索引カラムをドメイン型に変換することはできません。
* 別のカラムまたはテーブルからデータを挿入する際、文字列の値をドメイン値に暗黙的に変換することはできません。
* ドメインは、格納される値に制約を課しません。