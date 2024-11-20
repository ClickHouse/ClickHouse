---
slug: /ja/engines/table-engines/special/null
sidebar_position: 50
sidebar_label: 'Null'
---

# Null テーブルエンジン

`Null` テーブルに書き込むとデータは無視されます。`Null` テーブルから読み取ると、応答は空になります。

:::note
これがなぜ有用なのか疑問に思う場合は、`Null` テーブルに対して Materialized View を作成できることを確認してください。従って、テーブルに書き込まれたデータはビューに影響を与えますが、元の生データは無視されます。
:::
