---
slug: /ja/operations/system-tables/scheduler
---
# scheduler

ローカルサーバーに存在する[スケジューリングノード](/docs/ja/operations/workload-scheduling.md/#hierarchy)の情報とステータスを含んでいます。このテーブルはモニタリングに利用できます。テーブルには各スケジューリングノードごとに行が含まれています。

例:

``` sql
SELECT *
FROM system.scheduler
WHERE resource = 'network_read' AND path = '/prio/fair/prod'
FORMAT Vertical
```

``` text
Row 1:
──────
resource:          network_read
path:              /prio/fair/prod
type:              fifo
weight:            5
priority:          0
is_active:         0
active_children:   0
dequeued_requests: 67
canceled_requests: 0
dequeued_cost:     4692272
canceled_cost:     0
busy_periods:      63
vruntime:          938454.1999999989
system_vruntime:   ᴺᵁᴸᴸ
queue_length:      0
queue_cost:        0
budget:            -60524
is_satisfied:      ᴺᵁᴸᴸ
inflight_requests: ᴺᵁᴸᴸ
inflight_cost:     ᴺᵁᴸᴸ
max_requests:      ᴺᵁᴸᴸ
max_cost:          ᴺᵁᴸᴸ
max_speed:         ᴺᵁᴸᴸ
max_burst:         ᴺᵁᴸᴸ
throttling_us:     ᴺᵁᴸᴸ
tokens:            ᴺᵁᴸᴸ
```

カラム:

- `resource` (`String`) - リソース名
- `path` (`String`) - このリソーススケジューリング階層内のスケジューリングノードへのパス
- `type` (`String`) - スケジューリングノードのタイプ。
- `weight` (`Float64`) - `fair`タイプの親ノードで使用されるノードの重み。
- `priority` (`Int64`) - 'priority'タイプの親ノードで使用されるノードの優先度（値が低いほど優先度が高い）。
- `is_active` (`UInt8`) - このノードが現在アクティブであるかどうか（リソース要求をデキューして制約を満たすことが可能か）。
- `active_children` (`UInt64`) - アクティブ状態の子ノードの数。
- `dequeued_requests` (`UInt64`) - このノードからデキューされたリソース要求の総数。
- `canceled_requests` (`UInt64`) - このノードからキャンセルされたリソース要求の総数。
- `dequeued_cost` (`UInt64`) - このノードからデキューされたすべての要求のコスト（例: バイト単位サイズ）の合計。
- `canceled_cost` (`UInt64`) - このノードからキャンセルされたすべての要求のコスト（例: バイト単位サイズ）の合計。
- `busy_periods` (`UInt64`) - このノードの非アクティブ化の総数。
- `vruntime` (`Nullable(Float64)`) - `fair`ノードの子ノードのみ。最大最小公平方式で次の子ノードを処理するためにSFQアルゴリズムによって使用されるノードの仮想ランタイム。
- `system_vruntime` (`Nullable(Float64)`) - `fair`ノードのみ。最後に処理されたリソース要求の`vruntime`を表示する仮想ランタイム。子ノードのアクティブ化時に`vruntime`の新しい値として使用。
- `queue_length` (`Nullable(UInt64)`) - `fifo`ノードのみ。キュー内にあるリソース要求の現在の数。
- `queue_cost` (`Nullable(UInt64)`) - `fifo`ノードのみ。キュー内にあるすべての要求のコスト（例: バイト単位サイズ）の合計。
- `budget` (`Nullable(Int64)`) - `fifo`ノードのみ。新しいリソース要求のための利用可能な「コスト単位」の数。リソース要求の予想と実際のコストの不一致の場合に現れることがあります（例: 読み取り/書き込みエラーの後）。
- `is_satisfied` (`Nullable(UInt8)`) - 制約ノードのみ（例: `inflight_limit`）。このノードのすべての制約が満たされている場合に`1`。
- `inflight_requests` (`Nullable(Int64)`) - `inflight_limit`ノードのみ。このノードからデキューされ、現在消費状態にあるリソース要求の数。
- `inflight_cost` (`Nullable(Int64)`) - `inflight_limit`ノードのみ。このノードからデキューされ、現在消費状態にあるすべてのリソース要求のコスト（例: バイト単位）の合計。
- `max_requests` (`Nullable(Int64)`) - `inflight_limit`ノードのみ。制約違反につながる`inflight_requests`の上限。
- `max_cost` (`Nullable(Int64)`) - `inflight_limit`ノードのみ。制約違反につながる`inflight_cost`の上限。
- `max_speed` (`Nullable(Float64)`) - `bandwidth_limit`ノードのみ。1秒あたりのトークンでの帯域幅の上限。
- `max_burst` (`Nullable(Float64)`) - `bandwidth_limit`ノードのみ。トークンバケットスロットリングで利用可能な`tokens`の上限。
- `throttling_us` (`Nullable(Int64)`) - `bandwidth_limit`ノードのみ。このノードがスロットリング状態にあった合計マイクロ秒数。
- `tokens` (`Nullable(Float64)`) - `bandwidth_limit`ノードのみ。トークンバケットスロットリングで現在利用可能なトークンの数。
