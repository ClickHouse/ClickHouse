# Learnings

- For parallel partial-merge aggregation pipelines, having each partial emit ALL groups to the merge is a major bottleneck. If the query has ORDER BY + LIMIT K, each partial should sort and emit only its top K groups. For monotonic aggregates like `max`/`min`, this is provably correct: if a group is in the global top K, at least one partial must rank it in its local top K (the one containing the globally-best value).
- Double-checked locking on `std::shared_ptr` (reading outside mutex while another thread writes) is a data race — causes "Pure virtual function called!" crashes. Use `std::atomic<std::shared_ptr>` (C++20) or always lock.
- For `TopNAggregation` Mode 2, the `generateMode2Partial` finalize pass (O(num_groups) `insertResultInto` + partial sort) is NOT the bottleneck even at 10M groups — it's fast because `max`/`min` states are trivial copies and data fits in cache. The real cost is I/O + per-row `SipHash` + hash-table probing during `consumeMode2`.
