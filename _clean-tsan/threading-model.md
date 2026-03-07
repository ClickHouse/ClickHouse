# Threading Model

## AllocationQueue

### Mutexes
- `mutex` (std::recursive_mutex) — guards all mutable state: `pending_allocations`, `running_allocations`, `increasing_allocations`, `decreasing_allocations`, `removing_allocations`, `last_unique_id`, `pending_allocations_size`, `skip_activation`, `rejects`
  - Recursive property is not exercised by any current code path (all re-entries happen after the lock is released); could be plain `std::mutex` but kept as-is
  - Acquired by: user threads (TCPHandler, PipelineExecutor) and scheduler thread (WorkloadResMgr)

### Lock Ordering
- `AllocationQueue::mutex` must NOT be held when acquiring `EventQueue::mutex`
  - `scheduleActivation` and `propagate` are called outside lock scope
- `AllocationQueue::mutex` must NOT be held when acquiring `MemoryReservation::mutex` from the scheduler-thread callback path
  - `approveIncrease` and `approveDecrease` release `AllocationQueue::mutex` before calling `increaseApproved`/`decreaseApproved`
- **Exception**: `updateMinMaxAllocated` and `updateQueueLimit` hold `AllocationQueue::mutex` and call `allocationFailed()` which acquires `MemoryReservation::mutex` (`AllocationQueue::mutex` → `MemoryReservation::mutex` edge)
  - Safe in practice because these are called by the same single scheduler thread as `approveIncrease`/`approveDecrease` (no concurrency between them for a given queue)
  - However, this creates a reversed edge that TSan may flag against the `MemoryReservation::mutex` → `AllocationQueue::mutex` edge from `syncWithScheduler`

### Cross-Class Calls Under Lock
- `insertAllocation`, `increaseAllocation`, `decreaseAllocation` defer `scheduleActivation` until after `mutex` is released using `bool need_activation`
- `processActivation` releases `mutex` before calling `propagate`
- `approveIncrease` captures `alloc_ptr`/`req_ptr` under lock, releases lock, then calls `increaseApproved`
- `approveDecrease` captures `alloc_ptr`/`req_ptr` under lock, releases lock, then calls `decreaseApproved`
- `updateMinMaxAllocated`, `updateQueueLimit`: hold `mutex` and call `allocationFailed()` (acquires `MemoryReservation::mutex`) and `propagate()` (acquires `EventQueue::mutex`)

### Thread Roles
- User threads (TCPHandler, PipelineExecutor): `insertAllocation`, `increaseAllocation`, `decreaseAllocation`, `getQueueLengthAndSize`, `getRejects`, `getPending`
- WorkloadResMgr threads (scheduler threads created by WorkloadResourceManager): `processActivation`, `approveIncrease`, `approveDecrease`, `updateMinMaxAllocated`, `updateQueueLimit`, `selectAllocationToKill`, `purgeQueue`, `propagateUpdate`

### Known Safe Patterns
- `skip_activation` flag: set during `approveIncrease`/`approveDecrease` callback window to suppress `scheduleActivation()` calls from `increaseAllocation()`/`decreaseAllocation()` re-entered via `syncWithScheduler`. Without it, those calls would acquire `EventQueue::mutex` while `MemoryReservation::mutex` is held, violating lock ordering.
- After the `skip_activation` window in `approveIncrease`, orphaned decrease requests (added by other user threads) are detected and trigger `scheduleActivation` outside the lock.
- After the `skip_activation` window in `approveDecrease`, orphaned increase requests (added by other user threads) are detected and trigger `scheduleActivation` outside the lock.
- Without these checks, requests arriving during the `skip_activation` window could be permanently orphaned: `skip_activation` suppresses their activation, the only `setXxx()` call in the second lock section picks up requests of the same type (not cross-type), and the parent chain never learns about them.

## MemoryReservation

### Mutexes
- `mutex` (std::mutex) — guards per-allocation state: `allocated_size`, `actual_size`, `increase_enqueued`, `decrease_enqueued`, `removed`, `kill_reason`, `fail_reason`, `metrics`
  - Plain (non-recursive) std::mutex
  - Acquired by: user threads (TCPHandler, PipelineExecutor) in constructor, destructor, `syncWithMemoryTracker`
  - Acquired by: scheduler thread (WorkloadResMgr) in `increaseApproved`, `decreaseApproved`, `allocationFailed`, `killAllocation`

### Lock Ordering
- Consistent ordering: `MemoryReservation::mutex` before `AllocationQueue::mutex`
- Scheduler callbacks (`increaseApproved`, `decreaseApproved`): called with `AllocationQueue::mutex` released; they acquire `MemoryReservation::mutex`, then (via `syncWithScheduler`) acquire `AllocationQueue::mutex`
- User-thread paths (`~MemoryReservation`, `syncWithMemoryTracker`): release `MemoryReservation::mutex` BEFORE calling `queue.increaseAllocation`/`queue.decreaseAllocation` to avoid creating a reverse edge with `EventQueue::mutex`
- `skip_activation = true` prevents `scheduleActivation()` from acquiring `EventQueue::mutex` inside `syncWithScheduler` callbacks

### Cross-Class Calls Under Lock
- `~MemoryReservation`: two-phase pattern — captures state under `mutex`, releases `mutex`, then calls `queue.decreaseAllocation` outside lock
- `syncWithMemoryTracker`: two-phase pattern — captures sizes under `mutex`, releases `mutex`, then calls `queue.increaseAllocation`/`queue.decreaseAllocation` outside lock
- `increaseApproved`, `decreaseApproved`: acquire `MemoryReservation::mutex`, then via `syncWithScheduler` acquire `AllocationQueue::mutex` — consistent ordering
- `syncWithScheduler` (called under `MemoryReservation::mutex`, scheduler thread only): calls `queue.increaseAllocation`/`queue.decreaseAllocation` which acquire `AllocationQueue::mutex` — consistent ordering

### Condition Variables
- `cv` (std::condition_variable) — waits on admission (`actual_size <= allocated_size`), signaled by `increaseApproved`, `decreaseApproved`, `allocationFailed`, `killAllocation`, `syncWithScheduler`

### Thread Roles
- User threads: constructor (blocks on `cv`), destructor (blocks on `cv`), `syncWithMemoryTracker` (may block on `cv`)
- Scheduler thread: `increaseApproved`, `decreaseApproved`, `allocationFailed`, `killAllocation`

### Known Safe Patterns
- `syncWithScheduler` is called exclusively from scheduler-thread callbacks (`increaseApproved`, `decreaseApproved`) where `AllocationQueue::mutex` has already been released before the callback and `skip_activation` is active. It holds `MemoryReservation::mutex` while acquiring `AllocationQueue::mutex` — consistent ordering.
- User threads never hold both `MemoryReservation::mutex` and `AllocationQueue::mutex` simultaneously (two-phase pattern ensures `MemoryReservation::mutex` is released before `AllocationQueue::mutex` is acquired), so no deadlock even though `updateMinMaxAllocated` holds `AllocationQueue::mutex` while acquiring `MemoryReservation::mutex`.

## EventQueue

### Mutexes
- `mutex` (std::mutex) — guards `events`, `activations`, `postponed`, `last_event_id`
- `activation_mutex` (std::mutex) — serializes activation processing with `cancelActivation`

### Lock Ordering
- `EventQueue::mutex` → `EventQueue::activation_mutex` in `processActivation`
- No external mutex should be held when calling `enqueueActivation` (acquires `EventQueue::mutex`)

### Cross-Class Calls Under Lock
- `processActivation` holds `activation_mutex` while calling `node.processActivation()` (may acquire node's internal mutex)

### Condition Variables
- `pending` (std::condition_variable) — waits on `!events.empty() || !activations.empty() || !postponed.empty()`, signaled by `enqueue`, `enqueueActivation`, `postpone`

### Thread Roles
- Any thread: `enqueue`, `enqueueActivation`, `cancelActivation`, `postpone`, `cancelPostponed`
- Scheduler thread only: `process`, `tryProcess`, `forceProcess`, `processQueue`, `processActivation`

### Atomics
- `manual_time` (std::atomic<TimePoint>) — test-only manual time control

## ISchedulerNode

### Thread Roles
- `scheduleActivation`: any thread, enqueues into EventQueue. Must NOT hold `AllocationQueue::mutex`
- `cancelActivation`: any thread (typically destruction), acquires `EventQueue::mutex`
- `processActivation`: scheduler thread only, called while EventQueue holds `activation_mutex`

## TestAllocation (test-only, gtest_workload_resource_manager.cpp)

### Mutexes
- `mutex` (std::mutex) — guards per-allocation state: `real_size`, `allocated_size`, `increase_enqueued`, `decrease_enqueued`, `removed`, `kill_reason`, `fail_reason`, `approved_callback`
  - Acquired by: test main thread in constructor, destructor, `setSize`, `waitSync`, `waitKilled`, `throwReason`, `assertIncreaseEnqueued`
  - Acquired by: scheduler thread (WorkloadResMgr) in `killAllocation`, `increaseApproved`, `decreaseApproved`, `allocationFailed`

### Lock Ordering
- `TestAllocation::mutex` must NOT be held when acquiring `AllocationQueue::mutex` or `EventQueue::mutex`
- All queue operations (`insertAllocation`, `increaseAllocation`, `decreaseAllocation`) are called outside `mutex` scope using the two-phase pattern
- Constructor: sets state under lock, calls `insertAllocation` outside lock (safe: no other thread has a reference during construction)
- Destructor: captures state under lock, releases lock, calls `decreaseAllocation` outside lock, re-acquires for `cv.wait`
- `setSize`: captures state under lock via `computeSyncAction`, releases lock, calls `performSyncAction` outside lock
- `killAllocation`: follows production `MemoryReservation::killAllocation` pattern — acquires `mutex`, sets `kill_reason` and `real_size = 0`, calls `cv.notify_all()`. No queue operations. Safe to call within `EventQueue::activation_mutex` scope.
- `increaseApproved`/`decreaseApproved`: same problem as `killAllocation` when called from `AllocationQueue::approveIncrease`/`approveDecrease` during `processActivation` — `performSyncAction` acquires `EventQueue::mutex` while `activation_mutex` is still held
- Scheduler path: `EventQueue::activation_mutex` → `AllocationQueue::processActivation` → `propagate` → `AllocationLimit::setIncrease` → `killAllocation` → `performSyncAction` → `enqueueActivation` (acquires `EventQueue::mutex`) — THIS IS THE CYCLE
- The fix for `TestAllocation::mutex` ordering (iteration 005) eliminated the `TestAllocation::mutex` → `EventQueue::mutex` edge, but exposed the deeper `activation_mutex` → `EventQueue::mutex` edge that was always present
- Production `MemoryReservation::killAllocation` avoids this by ONLY setting flags + `cv.notify_all()` — no queue operations at all. The user thread handles the decrease after waking from CV

### Cross-Class Calls Under Lock
- `computeSyncAction` (called under `mutex`): pure computation, does NOT call queue operations; sets flags and captures sizes
- `performSyncAction` (called outside `mutex`): calls `queue.increaseAllocation`/`queue.decreaseAllocation` which acquire `AllocationQueue::mutex` and may call `scheduleActivation` -> `enqueueActivation` (acquires `EventQueue::mutex`)
- `killAllocation` no longer calls `performSyncAction`; it only sets flags and notifies `cv`, matching the production `MemoryReservation::killAllocation` pattern. The `activation_mutex` -> `EventQueue::mutex` edge from `killAllocation` is eliminated.
- `increaseApproved`/`decreaseApproved` still call `performSyncAction` but are safe because `AllocationQueue` sets `skip_activation = true` before calling them, suppressing `scheduleActivation` and thus preventing `EventQueue::mutex` acquisition.
- `allocationFailed`: acquires `mutex`, sets state, notifies cv — no queue operations (safe)

### Condition Variables
- `cv` (std::condition_variable) — waits in destructor (`allocated_size == 0` or `removed`), `waitSync` (`real_size == allocated_size`), `waitKilled` (`kill_reason`)

### Kill Flow (critical path)
- Production `MemoryReservation`: `killAllocation` sets `kill_reason` + CV notify. The user thread detects it in `syncWithMemoryTracker` (called on every memory allocation) which sees `actual_size` vs `allocated_size` discrepancy and enqueues decrease. The user thread always eventually calls `syncWithMemoryTracker`, so the decrease is guaranteed. Crucially, production has a SEPARATE user thread per allocation that is always running.
- `TestAllocation` (iteration 007): `killAllocation` sets `kill_reason`, sets `real_size = 0`, + CV notify. `waitKilled` detects `kill_reason`, then calls `computeSyncAction` under lock followed by `performSyncAction` outside lock to enqueue the decrease. This is the test-side equivalent of production's `syncWithMemoryTracker` detecting the kill and handling the decrease.
- **Problem (iteration 008)**: The test has only ONE thread. When allocation A is killed while the test thread is blocked in `waitSync` on a DIFFERENT allocation B, nobody calls `waitKilled` for A. The killed allocation's resources remain in the scheduler's accounting, blocking B's increase from being approved. This creates a livelock: the test thread waits for B's sync, B's sync needs A's resources freed, A's resources need `waitKilled` to be called, `waitKilled` is after `waitSync` in the test code.
- **Required fix**: `killAllocation` must enqueue the decrease immediately (like production's always-running user thread would). It must call `computeSyncAction` + `performSyncAction` from within the `killAllocation` callback. However, `killAllocation` is called from `AllocationLimit::setIncrease` during `propagate`, which runs under `EventQueue::activation_mutex`. Calling `performSyncAction` -> `queue.decreaseAllocation` -> `scheduleActivation` -> `enqueueActivation` would acquire `EventQueue::mutex` under `activation_mutex`, creating a lock-order-inversion with the normal `EventQueue::mutex -> activation_mutex` ordering in `processActivation`.
- **Safe resolution**: Since `skip_activation` is NOT set during the `killAllocation` callback (it is only set during `approveIncrease`/`approveDecrease`), we need an alternative approach. Options: (1) Make `AllocationQueue::processActivation` loop after `propagate` to pick up new decrease requests, allowing `killAllocation` to call `decreaseAllocation` with activation suppressed; (2) Accept the `activation_mutex -> EventQueue::mutex` edge as safe (single scheduler thread means no real deadlock); (3) Post the decrease work to a separate mechanism that runs outside `activation_mutex`.

### Thread Roles
- Test main thread: constructor, destructor, `setSize`, `waitSync`, `waitKilled`
- Scheduler thread: `killAllocation`, `increaseApproved`, `decreaseApproved`, `allocationFailed`

## Global Lock Ordering Summary

Consistent total order (no cycle in practice for a single scheduler thread):

```
EventQueue::mutex → EventQueue::activation_mutex → MemoryReservation::mutex → AllocationQueue::mutex
```

The `AllocationQueue::mutex` → `MemoryReservation::mutex` edge in `updateMinMaxAllocated`/`updateQueueLimit` is not a practical deadlock risk
because these are processed serially by the same scheduler thread as `approveIncrease`/`approveDecrease`.
However, TSan may report it as a lock-order-inversion if it observes both orderings.
