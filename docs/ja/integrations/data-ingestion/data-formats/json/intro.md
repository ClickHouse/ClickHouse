---
sidebar_label: 概要
sidebar_position: 10
title: JSONの操作
slug: /ja/integrations/data-formats/json/overview
description: ClickHouseでのJSONの操作
keywords: [json, clickhouse]
---

# 概要

<div style={{width:'640px', height: '360px'}}>
  <iframe src="//www.youtube.com/embed/gCg5ISOujtc"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

<br />

ClickHouseは、JSONを扱うためのいくつかのアプローチを提供しており、それぞれに利点と欠点、および利用用途があります。このガイドでは、JSONをどのようにロードし、スキーマを最適に設計するかについて説明します。以下のセクションで構成されています：

- [JSONのロード](/docs/ja/integrations/data-formats/json/loading) - 簡単なスキーマを使用してClickHouseでJSON（特に[NDJSON](https://github.com/ndjson/ndjson-spec)）をロードし、クエリを実行します。
- [JSONスキーマの推測](/docs/ja/integrations/data-formats/json/inference) - JSONスキーマの推測を使用してJSONをクエリし、テーブルスキーマを作成します。
- [JSONスキーマの設計](/docs/ja/integrations/data-formats/json/schema) - JSONスキーマを設計し最適化するためのステップ。
- [JSONのエクスポート](/docs/ja/integrations/data-formats/json/exporting) - JSONをどのようにエクスポートするかについて。
- [その他のJSON形式の処理](/docs/ja/integrations/data-formats/json/other-formats) - NDJSON以外のJSON形式を処理するためのいくつかのヒント。
- [JSONをモデリングするためのその他のアプローチ](/docs/ja/integrations/data-formats/json/other-approaches) - JSONをモデリングするための高度なアプローチ。**推奨されません。**

:::note 重要: 新しいJSONタイプが間もなくリリースされます
このガイドでは、既存のJSON処理技術について考慮しています。現在、新しいJSONタイプが活発に開発されており、間もなく利用可能になります。この機能の進捗状況については、[このGitHub issue](https://github.com/ClickHouse/ClickHouse/issues/54864)を追ってください。この新しいデータタイプは、既存の廃止予定の[オブジェクトデータタイプ](/docs/ja/sql-reference/data-types/object-data-type)（エイリアス`JSON`）に取って代わります。
:::
