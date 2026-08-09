# A dedicated huge-page jemalloc arena for hash tables

Date: 2026-08-09
Branch: `hashtable-huge-pages`
Status: prototype, measured

## Problem

Aggregation hash tables are probed randomly over hundreds of megabytes. On 4 KB pages nearly every
probe misses the dTLB, and `Allocator` never calls `madvise(MADV_HUGEPAGE)`, so they always run on
4 KB pages. Enabling transparent hugepages is worth up to 4.5x on a large `GROUP BY`.

The obvious fix - madvise the hash-table buffer - does not work, for two independent reasons.

**Marking outlives the allocation.** `MADV_HUGEPAGE` sets `VM_HUGEPAGE` on the VMA, not on the
allocation. jemalloc frees with `MADV_DONTNEED` and retains the extent, so the flag survives and
whatever is placed there next silently inherits huge pages. A targeted madvise therefore has the
same risk profile as `thp:always`, arrived at unpredictably - which is worse than setting it
deliberately.

**Marking fragments the address space.** Each `madvise` over a sub-range splits the VMA around it.
Measured directly on a 4 GB region:

| | VMAs added | huge pages |
| --- | --- | --- |
| madvise 1024 sub-ranges individually | **+2047** | 2048 MB |
| madvise the whole region once | **+0** | 4096 MB |
| `MADV_DONTNEED` on 512 sub-ranges afterwards | **+0** | frees 1024 MB, re-faults huge |

## Design

Mark address space, not allocations.

A dedicated jemalloc arena is created with custom `extent_hooks_t`. Memory is reserved in 1 GiB
chunks, each marked once with a single `madvise(MADV_HUGEPAGE)`, and the alloc hook carves
allocations out of those chunks. jemalloc only calls the alloc hook when the arena needs *new*
address space - it recycles extents it already owns internally - so the marking cost is one madvise
per gigabyte rather than one per allocation.

- `alloc` carves from the chunk pool, 2 MB-aligned so the kernel can actually back the range.
- `dalloc` returns true, meaning "not deallocated": jemalloc keeps the extent and reuses it *inside
  this arena*. That is what confines marked memory to hash tables.
- `purge_forced` passes `MADV_DONTNEED` through. This is safe precisely because `MADV_DONTNEED`
  does not modify `vm_flags`: it releases the physical pages, keeps the marking, and the range
  faults back in as huge pages. Memory can still be returned under pressure.
- `split` and `merge` are bookkeeping inside jemalloc and do not touch the mapping, so both succeed.
- `decommit` is unsupported, since decommitting would mean giving the address space back.

Only allocations of at least 2 MB are routed to the arena; below that, 2 MB granularity would waste
far more than the TLB misses it saves.

`realloc` has to go through the arena explicitly (`rallocx` with `MALLOCX_ARENA`). Plain `realloc`
would satisfy the request from the calling thread's arena, so a hash table would leave the huge
pages behind the first time it grew - which is every table of interesting size.

The chunk pool allocates nothing on the heap. The hooks run with jemalloc's arena locks held, so a
`std::vector` growing or a log message formatting inside them risks recursing into the allocator or
deadlocking; hence a fixed array and a counter instead of logging.

## Results

`big_clustered`: 300M rows, 30M distinct keys, single-level, `SELECT k, count() ... GROUP BY k`.

| host | mode | 1 thread | 16 threads |
| --- | --- | --- | --- |
| AMD EPYC 9R45 | 4 KB pages | 8.218 s | 6.222 s |
| AMD EPYC 9R45 | **arena** | **1.819 s** | **1.845 s** |
| AMD EPYC 9R45 | `thp:always` | 1.798 s | 1.937 s |
| Xeon 6975P-C | 4 KB pages | 3.510 s | 7.027 s |
| Xeon 6975P-C | **arena** | **2.560 s** | **3.878 s** |
| Xeon 6975P-C | `thp:always` | 2.460 s | 3.728 s |

The arena matches `thp:always` on speed - 4.5x over baseline on AMD - while costing less of what
`thp:always` costs. Peak, sampled while the query runs:

| host | mode | peak RSS | AnonHugePages | peak VMAs |
| --- | --- | --- | --- | --- |
| AMD | 4 KB pages | 1.99 GB | 0.00 GB | 530 |
| AMD | **arena** | 2.25 GB (+13%) | 1.83 GB (81%) | **531 (+1)** |
| AMD | `thp:always` | 2.42 GB (+22%) | 2.17 GB (90%) | 568 (+38) |
| Intel | 4 KB pages | 2.17 GB | 0.00 GB | 530 |
| Intel | **arena** | 2.41 GB (+11%) | 1.83 GB (76%) | **522 (-8)** |
| Intel | `thp:always` | 2.35 GB (+8%) | 2.07 GB (88%) | 588 (+58) |

The pages really are huge - 1.83 GB of `AnonHugePages` confirmed from smaps, not inferred from a
dTLB counter. VMA growth is within noise of the baseline, against +38 and +58 for `thp:always`, and
against the +2047 that a per-allocation madvise would cost.

## Measurement traps hit along the way

- **The watchdog.** ClickHouse forks a watchdog parent that also matches the server's port, and it
  is the *first* match, so `pgrep -f tcp_port=... | head -1` returns a near-idle process. A first
  pass reported 0.18 GB peak RSS for every mode. Set `CLICKHOUSE_WATCHDOG_ENABLE=0`.
- **Sampling after the query.** Huge pages cost 2 MB rounding on *live* allocations, so the cost
  only exists while they are live. Sampling once the query finished reported zero for every mode.
- **This dev box cannot do THP at all.** Kernel 7.0.0-1010-aws reports `THPeligible: 0` for a 2 MB
  aligned anonymous mapping with `VM_HUGEPAGE` set, with sysfs settings identical to the metal
  hosts on 6.17. Correctness and VMA counts can be checked locally; anything about huge pages
  cannot.

## Open

- Gated on `CH_HASHTABLE_HUGE_PAGES` for the experiment. A server setting is the productionisation
  step, along with deciding a default - the win is strongly architecture-dependent (4.5x on AMD,
  1.4x on Intel) and shrinks with parallelism.
- Chunk size (1 GiB) and the 2 MB routing threshold are unswept.
- Only `HashTableAllocator` is routed. `Arena` (aggregate states) and `PODArray` are untouched, and
  aggregate states are the other large random-access structure in aggregation.
- Behaviour under memory pressure is untested: the arena never returns address space, only physical
  pages via purge.
