---
slug: /ja/sql-reference/statements/watch
sidebar_position: 53
sidebar_label: WATCH
---

# WATCH ステートメント (試験的)

:::note    
これは、将来のリリースで後方互換性のない変更が行われる可能性のある試験的な機能です。ライブビューと `WATCH` クエリを有効にするには、`set allow_experimental_live_view = 1` を使用してください。
:::

``` sql
WATCH [db.]live_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

`WATCH` クエリは、[ライブビュー](./create/view.md#live-view)テーブルから継続的なデータ取得を行います。`LIMIT` 句が指定されていない場合、[ライブビュー](./create/view.md#live-view)からクエリ結果の無限ストリームを提供します。

```sql
WATCH [db.]live_view [EVENTS] [LIMIT n] [FORMAT format]
```

## 仮想カラム

クエリ結果内の仮想カラム `_version` は、現在の結果バージョンを示します。

**例:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv;
```

```bash
┌───────────────now()─┬─_version─┐
│ 2021-02-21 09:17:21 │        1 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 09:17:26 │        2 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 09:17:31 │        3 │
└─────────────────────┴──────────┘
...
```

デフォルトでは、要求されたデータはクライアントに返されますが、[INSERT INTO](../../sql-reference/statements/insert-into.md) と組み合わせて使用することで、異なるテーブルに転送できます。

**例:**

```sql
INSERT INTO [db.]table WATCH [db.]live_view ...
```

## EVENTS 句

`EVENTS` 句を使用すると、`WATCH` クエリの簡略版を取得できます。この場合、クエリ結果の代わりに最新のクエリ結果バージョンのみが返されます。

```sql
WATCH [db.]live_view EVENTS;
```

**例:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv EVENTS;
```

```bash
┌─version─┐
│       1 │
└─────────┘
┌─version─┐
│       2 │
└─────────┘
...
```

## LIMIT 句

`LIMIT n` 句は、`WATCH` クエリが終了する前に待機する更新の数を指定します。デフォルトでは更新の数に制限はなく、したがってクエリは終了しません。値が `0` の場合、`WATCH` クエリは新しいクエリ結果を待機しないことを示し、クエリ結果が評価され次第すぐに返されます。

```sql
WATCH [db.]live_view LIMIT 1;
```

**例:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv EVENTS LIMIT 1;
```

```bash
┌─version─┐
│       1 │
└─────────┘
```

## FORMAT 句

`FORMAT` 句は、[SELECT](../../sql-reference/statements/select/format.md#format-clause) と同じ方法で機能します。

:::note    
[HTTP インターフェース](../../interfaces/formats.md#jsoneachrowwithprogress)を介して [ライブビュー](./create/view.md#live-view) テーブルを監視する際は、[JSONEachRowWithProgress](../../interfaces/formats.md#jsoneachrowwithprogress) フォーマットを使用するべきです。進行状況メッセージは、クエリ結果が変化するまで長時間維持される HTTP 接続を保つために出力に追加されます。進行状況メッセージの間隔は、[live_view_heartbeat_interval](./create/view.md#live-view-settings) 設定を使用して制御されます。
:::
