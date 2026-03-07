## Iteration 001: NEW_ALERT
- **Alert:** lock-order-inversion (M0=AllocationQueue::mutex => M1=EventQueue::mutex => M2=EventQueue::activation_mutex => M0)
- **Root cause:** insertAllocation/increaseAllocation/decreaseAllocation called scheduleActivation() under AllocationQueue::mutex
- **Fix:** Deferred scheduleActivation() outside lock scope via bool need_activation pattern in all 3 methods
- **Files:** src/Common/Scheduler/Nodes/SpaceShared/AllocationQueue.cpp

## Iteration 002: FIXED
- **Alert:** lock-order-inversion (M0=AllocationQueue::mutex [recursive] => M1=MemoryReservation::mutex [plain])
- **Root cause:** Two threads acquire the two mutexes in opposite orders:
  - WorkloadResMgr thread: approveIncrease() locks M0 at AllocationQueue.cpp:203, then calls increaseApproved() which locks M1 at MemoryReservation.cpp:175
  - User thread (TCPHandler): ~MemoryReservation() locks M1 at MemoryReservation.cpp:65, then calls queue.decreaseAllocation() which locks M0 at AllocationQueue.cpp:120
- **Fix:** Deferred `increaseApproved()`/`decreaseApproved()` callbacks outside M0 scope in `approveIncrease()`/`approveDecrease()`. Capture `alloc_ptr`/`req_ptr` under lock, release lock, call callback, re-acquire lock to set `skip_activation = false` and call `setIncrease()`/`setDecrease()`.
- **Files:** src/Common/Scheduler/Nodes/SpaceShared/AllocationQueue.cpp

## Iteration 003: FIXED (verified clean — 6/6 tests pass, no TSan errors)
- **Alert:** lock-order-inversion (3-mutex cycle: M_res=MemoryReservation::mutex => M_eq=EventQueue::mutex => M_act=EventQueue::activation_mutex => M_res)
- **Root cause:** User threads (`syncWithMemoryTracker`, `~MemoryReservation`) held `MemoryReservation::mutex` (M_res) while calling `syncWithScheduler()` → `increaseAllocation()`/`decreaseAllocation()` → `scheduleActivation()` → `enqueueActivation()` (acquires M_eq). Combined with scheduler path M_eq → M_act → M_res (via `processActivation` → `killAllocation`), forms 3-mutex cycle.
- **Fix:** Two-phase lock pattern in `~MemoryReservation` and `syncWithMemoryTracker`: (1) under M_res: set flags, capture sizes; (2) release M_res; (3) call queue operations outside M_res; (4) re-acquire M_res for cv.wait. Eliminates the M_res → M_eq edge from user threads.
- **Files:** src/Common/Scheduler/MemoryReservation.cpp

## Iteration 004 (Scheduler): NEW_ALERT
- **Alert:** Hang (livelock with 9.3% CPU) in `SchedulerSpaceShared.ConcurrentReservations` test
- **Root cause:** Lost activation during `skip_activation` window in `AllocationQueue::approveIncrease`/`approveDecrease`. Cross-type requests orphaned because only same-type `setIncrease`/`setDecrease` was called after callback.
- **Fix:** After the `skip_activation` window in both methods, detect orphaned cross-type requests and trigger `scheduleActivation` outside the lock.
- **Files:** src/Common/Scheduler/Nodes/SpaceShared/AllocationQueue.cpp

## Iteration 005 (Scheduler): FIXED
- **Alert:** lock-order-inversion (3-mutex cycle: M0=TestAllocation::mutex => M1=EventQueue::mutex => M2=EventQueue::activation_mutex => M0)
- **Root cause:** `TestAllocation` (test-only class in gtest_workload_resource_manager.cpp) holds its `mutex` while calling queue operations (`insertAllocation`, `increaseAllocation`, `decreaseAllocation`) that eventually acquire `EventQueue::mutex` via `scheduleActivation`. Meanwhile the scheduler thread, under `EventQueue::activation_mutex`, calls `killAllocation`/`allocationFailed` which acquire `TestAllocation::mutex`. This forms the cycle: `TestAllocation::mutex` → `EventQueue::mutex` → `EventQueue::activation_mutex` → `TestAllocation::mutex`.
- **Affected methods:**
  - Constructor (line 1751): holds `mutex` during `insertAllocation`
  - Destructor (line 1763): holds `mutex` during `decreaseAllocation`
  - `setSize` (line 1796): holds `mutex` during `syncSize` → queue operations
  - `killAllocation` (line 1838): holds `mutex` during `syncSize` → queue operations (scheduler callback, called under `EventQueue::activation_mutex`)
  - `increaseApproved` (line 1870): holds `mutex` during `syncSize` → queue operations (scheduler callback)
  - `decreaseApproved` (line 1881): holds `mutex` during `syncSize` → queue operations (scheduler callback)
- **Fix:** Applied same two-phase lock pattern used in `MemoryReservation`. Replaced `syncSize` with `computeSyncAction` (pure computation under lock) + `performSyncAction` (queue operations outside lock). All methods restructured: constructor calls `insertAllocation` outside lock; destructor captures state then calls `decreaseAllocation` outside lock; `setSize`, `killAllocation`, `increaseApproved`, `decreaseApproved` all use `computeSyncAction` under lock then `performSyncAction` outside lock.
- **Files:** src/Common/Scheduler/Nodes/tests/gtest_workload_resource_manager.cpp

## Iteration 007 (Scheduler): NEW_ALERT
- **Alert:** Livelock (hang with 40.9% CPU) in `SchedulerWorkloadResourceManager.MemoryReservationKillsOther` test
- **Root cause:** After iteration 006 changed `killAllocation` to just set flags (matching `MemoryReservation`), the killed allocation's decrease is never enqueued. In production, `syncWithMemoryTracker` handles this automatically. `TestAllocation::waitKilled` detects `kill_reason` but does not enqueue the decrease, so the scheduler cannot free resources and other allocations hang.
- **Fix:** Modified `waitKilled` to call `computeSyncAction` under the lock after detecting `kill_reason`, then `performSyncAction` outside the lock. This enqueues the decrease for the killed allocation's resources, matching what production's `syncWithMemoryTracker` does.
- **Files:** src/Common/Scheduler/Nodes/tests/gtest_workload_resource_manager.cpp

## Iteration 008 (Scheduler): NEW_ALERT
- **Alert:** Livelock (hang with 42.8% CPU) in `SchedulerWorkloadResourceManager.MemoryReservationIncreaseOfRunningHasPriorityOverPending` test
- **Root cause:** Single-threaded test cannot handle killed allocation's resource cleanup when test thread is blocked in `waitSync` on a different allocation. Test flow: (1) `a1` (80 bytes) and `a2` (10 bytes) are running, `a3` (40 bytes) is pending; (2) `a2.setSize(70)` enqueues increase of 60; (3) scheduler's `AllocationLimit::setIncrease` sees limit exceeded, kills `a1`; (4) `killAllocation` sets flags + cv.notify but does NOT enqueue decrease (iteration 006/007 pattern); (5) nobody calls `waitKilled` for `a1` because test thread is blocked in `a2.waitSync()` at line 2446; (6) `a1`'s 80 bytes remain in scheduler accounting; (7) `a2`'s increase cannot proceed; (8) scheduler goes idle — livelock.
- **Difference from iteration 007:** In iteration 007, the test called `waitKilled` before needing other allocations to proceed. Here, `waitSync` (line 2446) precedes `waitKilled` (line 2449), so the killed allocation's decrease must happen without explicit test-thread involvement.
- **Proposed fix:** `TestAllocation::killAllocation` must enqueue the decrease immediately rather than deferring to `waitKilled`. Call `computeSyncAction` under lock + `performSyncAction` outside lock. The challenge is that `killAllocation` runs under `EventQueue::activation_mutex` (from `processActivation` -> `propagate` -> `AllocationLimit::setIncrease`), and `performSyncAction` -> `queue.decreaseAllocation` -> `scheduleActivation` -> `enqueueActivation` acquires `EventQueue::mutex`, creating `activation_mutex -> EventQueue::mutex` (reverse of normal ordering). Since `skip_activation` is NOT set during this path (only set during `approveIncrease`/`approveDecrease`), `scheduleActivation` fires.
- **Safe resolution options:** (A) Accept the `activation_mutex -> EventQueue::mutex` edge — no real deadlock risk since `EventQueue::mutex` was released before `activation_mutex` was acquired, and only one scheduler thread exists; (B) Modify `AllocationQueue::processActivation` to loop after `propagate` to pick up decrease requests added by `killAllocation`, so `killAllocation` can call `decreaseAllocation` without needing `scheduleActivation`; (C) Have `killAllocation` call `queue.decreaseAllocation` and rely on `skip_activation` not being set — the activation fires, creating a benign TSan alert.
- **Recommended approach:** Option (A) is simplest: have `killAllocation` call `computeSyncAction` + `performSyncAction`. The `activation_mutex -> EventQueue::mutex` ordering is safe (single scheduler thread, `mutex` not held during activation processing). If TSan flags it, Option (B) avoids the alert but requires production code changes.
- **Files:** src/Common/Scheduler/Nodes/tests/gtest_workload_resource_manager.cpp (and possibly src/Common/Scheduler/Nodes/SpaceShared/AllocationQueue.cpp for Option B)
