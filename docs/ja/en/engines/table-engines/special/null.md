---
description: '`Null` テーブルに書き込まれたデータは無視され、`Null` テーブルから読み取った場合のレスポンスは空になります。'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Null table engine'
doc_type: 'reference'
---

`Null` テーブルにデータを書き込むと、そのデータは無視されます。
`Null` テーブルから読み取ると、レスポンスは空になります。

`Null` テーブルエンジンは、変換後に元のデータが不要になるようなデータ変換に便利です。
この用途では、`Null` テーブル上に materialized view を作成できます。
テーブルに書き込まれたデータはそのビューで処理されますが、元の生データは破棄されます。