---
description: 'ビューの実装に使用します（詳細は `CREATE VIEW
  query` を参照してください）。データ自体は保存せず、指定された `SELECT` クエリだけを保持します。テーブルの読み取り時には、このクエリが実行されます（クエリから不要なカラムはすべて削除されます）。'
sidebar_label: 'ビュー'
sidebar_position: 90
slug: /engines/table-engines/special/view
title: 'View テーブルエンジン'
doc_type: 'reference'
---

ビューの実装に使用します (詳細は `CREATE VIEW query` を参照してください) 。データ自体は保存せず、指定された `SELECT` クエリだけを保持します。テーブルの読み取り時には、このクエリが実行されます (クエリから不要なカラムはすべて削除されます) 。