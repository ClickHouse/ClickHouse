---
slug: /en/operations/system-tables/scheduler
---
# scheduler

Contains information and status for [scheduling nodes](/docs/en/operations/workload-scheduling.md/#hierarchy) residing on the local server.
This table can be used for monitoring. The table contains a row for every scheduling node.

Example:

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

Columns:

- `resource` (`String`) - Resource name
- `path` (`String`) - Path to a scheduling node within this resource scheduling hierarchy
- `type` (`String`) - Type of a scheduling node.
- `weight` (`Float64`) - Weight of a node, used by a parent node of `fair`` type.
- `priority` (`Int64`) - Priority of a node, used by a parent node of 'priority' type (Lower value means higher priority).
- `is_active` (`UInt8`) - Whether this node is currently active - has resource requests to be dequeued and constraints satisfied.
- `active_children` (`UInt64`) - The number of children in active state.
- `dequeued_requests` (`UInt64`) - The total number of resource requests dequeued from this node.
- `canceled_requests` (`UInt64`) - The total number of resource requests canceled from this node.
- `dequeued_cost` (`UInt64`) - The sum of costs (e.g. size in bytes) of all requests dequeued from this node.
- `canceled_cost` (`UInt64`) - The sum of costs (e.g. size in bytes) of all requests canceled from this node.
- `busy_periods` (`UInt64`) - The total number of deactivations of this node.
- `vruntime` (`Nullable(Float64)`) - For children of `fair` nodes only. Virtual runtime of a node used by SFQ algorithm to select the next child to process in a max-min fair manner.
- `system_vruntime` (`Nullable(Float64)`) - For `fair` nodes only. Virtual runtime showing `vruntime` of the last processed resource request. Used during child activation as the new value of `vruntime`.
- `queue_length` (`Nullable(UInt64)`) - For `fifo` nodes only. Current number of resource requests residing in the queue.
- `queue_cost` (`Nullable(UInt64)`) - For `fifo` nodes only. Sum of costs (e.g. size in bytes) of all requests residing in the queue.
- `budget` (`Nullable(Int64)`) - For `fifo` nodes only. The number of available "cost units" for new resource requests. Can appear in case of discrepancy of estimated and real costs of resource requests (e.g. after read/write failure)
- `is_satisfied` (`Nullable(UInt8)`) - For constraint nodes only (e.g. `inflight_limit`). Equals `1` if all the constraint of this node are satisfied.
- `inflight_requests` (`Nullable(Int64)`) - For `inflight_limit` nodes only. The number of resource requests dequeued from this node, that are currently in consumption state.
- `inflight_cost` (`Nullable(Int64)`) - For `inflight_limit` nodes only. The sum of costs (e.g. bytes) of all resource requests dequeued from this node, that are currently in consumption state.
- `max_requests` (`Nullable(Int64)`) - For `inflight_limit` nodes only. Upper limit for `inflight_requests` leading to constraint violation.
- `max_cost` (`Nullable(Int64)`) - For `inflight_limit` nodes only. Upper limit for `inflight_cost` leading to constraint violation.
- `max_speed` (`Nullable(Float64)`) - For `bandwidth_limit` nodes only. Upper limit for bandwidth in tokens per second.
- `max_burst` (`Nullable(Float64)`) - For `bandwidth_limit` nodes only. Upper limit for `tokens` available in token-bucket throttler.
- `throttling_us` (`Nullable(Int64)`) - For `bandwidth_limit` nodes only. Total number of microseconds this node was in throttling state.
- `tokens` (`Nullable(Float64)`) - For `bandwidth_limit` nodes only. Number of tokens currently available in token-bucket throttler.
