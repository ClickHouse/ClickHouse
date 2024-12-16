---
slug: /ja/materialized-view/refreshable-materialized-view
title: リフレッシュ可能なMaterialized View（エクスペリメンタル機能）
description: クエリを高速化するためのMaterialized Viewの使い方
keywords: [refreshable materialized view, refresh, materialized views, speed up queries, query optimization]
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

リフレッシュ可能なMaterialized Viewは、伝統的なOLTPデータベースにおけるMaterialized Viewと概念的に類似しており、指定されたクエリの結果を保存して迅速に取得することで、リソース集約的なクエリを繰り返し実行する必要を減らします。ClickHouseの[インクリメンタルMaterialized View](/ja/materialized-view)とは異なり、完全なデータセットに対して定期的にクエリを実行する必要があり、その結果がクエリ用のターゲットテーブルに保存されます。この結果セットは、理論的には元のデータセットより小さくなり、後続のクエリの実行を高速化します。

<img src={require('./images/refreshable-materialized-view-diagram.png').default}
  class='image'
  alt='リフレッシュ可能なMaterialized Viewの図'
  style={{width: '100%', background: 'none' }} />

## リフレッシュ可能なMaterialized Viewを使用するべき場合

ClickHouseのインクリメンタルMaterialized Viewは非常に強力で、特に単一テーブルに対する集計を行う場合、リフレッシュ可能なMaterialized Viewの手法よりも通常、大規模にスケールします。データが挿入される各ブロックごとに集計を計算し、最終テーブルでインクリメンタルな状態をマージすることにより、クエリは常にデータの一部に対してのみ実行されます。この方法はペタバイト単位のデータにまでスケールする可能性があり、通常は好まれる方法です。

しかし、このインクリメンタルなプロセスが必要でない場合や適用できない場合もあります。いくつかの問題はインクリメンタルアプローチと互換性がなかったり、リアルタイムでの更新を必要としなかったりし、定期的な再構築の方が適切である場合があります。たとえば、複雑なジョインを使用するためインクリメンタルアプローチと互換性がないケースでは、データセット全体に対してビューの完全な再計算を定期的に実行したい場合があります。

> リフレッシュ可能なMaterialized Viewは、非正規化といったタスクを行うバッチプロセスを実行するために使用されます。リフレッシュ可能なMaterialized Viewの間に依存関係を作成することで、あるビューが別のビューの結果に依存し、そのビューが完了すると実行されるようにすることができます。これを使用して、スケジュールされたワークフローや[dbt](https://www.getdbt.com/)ジョブのような単純なDAGを置き換えることができます。

## 例

例として、StackOverflowデータセットの`postlinks`データセットを`posts`テーブルに非正規化する以下のクエリを考えます。なぜユーザーがこれを行いたがるかについては、[非正規化データガイド](/ja/data-modeling/denormalization)で探っています。

```sql
SELECT
    posts.*,
    arrayMap(p -> (p.1, p.2), arrayFilter(p -> p.3 = 'Linked' AND p.2 != 0, Related)) AS LinkedPosts,
    arrayMap(p -> (p.1, p.2), arrayFilter(p -> p.3 = 'Duplicate' AND p.2 != 0, Related)) AS DuplicatePosts
FROM posts
LEFT JOIN (
    SELECT
   	 PostId,
   	 groupArray((CreationDate, RelatedPostId, LinkTypeId)) AS Related
    FROM postlinks
    GROUP BY PostId
) AS postlinks ON posts_types_codecs_ordered.Id = postlinks.PostId
```

`posts`と`postlinks`の両方のテーブルは更新される可能性があります。このジョインをインクリメンタルMaterialized Viewで実装する代わりに、このクエリを1時間ごとに実行し、その結果を`post_with_links`テーブルに保存するスケジュールを設定することで十分かもしれません。

ここでの構文はインクリメンタルMaterialized Viewと同一ですが、[`REFRESH`](/ja/sql-reference/statements/create/view#refreshable-materialized-view)句を含めています：

```sql
-- エクスペリメンタル機能を有効化
SET allow_experimental_refreshable_materialized_view = 1

CREATE MATERIALIZED VIEW posts_with_links_mv
REFRESH EVERY 1 HOUR TO posts_with_links AS
SELECT
    posts.*,
    arrayMap(p -> (p.1, p.2), arrayFilter(p -> p.3 = 'Linked' AND p.2 != 0, Related)) AS LinkedPosts,
    arrayMap(p -> (p.1, p.2), arrayFilter(p -> p.3 = 'Duplicate' AND p.2 != 0, Related)) AS DuplicatePosts
FROM posts
LEFT JOIN (
    SELECT
   	 PostId,
   	 groupArray((CreationDate, RelatedPostId, LinkTypeId)) AS Related
    FROM postlinks
    GROUP BY PostId
) AS postlinks ON posts_types_codecs_ordered.Id = postlinks.PostId
```

このビューは、ソーステーブルへの更新が反映されるように設定されており、即時に実行され、その後も毎時間実行されます。特に、クエリが再実行されるとき、結果セットは原子かつ透過的に更新されます。
