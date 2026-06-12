#pragma once

#include <IO/PrefetchThreadPool.h>

#include <atomic>
#include <exception>
#include <functional>
#include <memory>

namespace DB
{

/// Lifecycle of a fetch machine (driven by `FetchMachineRunner`). Stored in one
/// atomic word that doubles as the ownership token: the executor owns the
/// machine in every parked/terminal state; a pool worker owns it while
/// Scheduled/Running. `Running` is never reclaimed - only flagged
/// (`interrupt_requested`) and waited for via the step handle.
enum class MachineState : uint8_t
{
    Constructed,    /// executor-owned, no step scheduled yet
    Scheduled,      /// a step is queued on the pool
    Running,        /// a worker is executing a step
    AwaitCollect,   /// barrier: products ready, waiting for the executor
    Interrupted,    /// wrapped up at an interrupt point on request; partial products held
    ParkedPoolFull, /// a schedule was rejected; executor reschedules or abandons
    Done,
    Cancelled,      /// revoked while still queued; payload untouched
    Failed,         /// the step threw; `failure` holds the exception
};

/// Which pool a step runs on. One shared pool today; the kind keys the routing
/// once a dedicated cache-write pool exists.
enum class StepKind : uint8_t
{
    Fetch,
    Put,
};

/// A step's verdict: park at the barrier or finish. (Multi-step machines
/// return AwaitCollect and the executor schedules the continuation - every
/// transition is executor-mediated; a worker never schedules the next step.)
enum class StepResult : uint8_t
{
    AwaitCollect,
    Done,
};

/// The protocol half of a fetch machine - everything the runner needs to
/// schedule, revoke, interrupt and join one, with no knowledge of what the
/// steps do. `ReaderExecutor::FetchMachine` inherits this and adds the payload
/// (connection cluster, fetched rope, job-local stats, ...).
struct MachineBase
{
    std::atomic<MachineState> state{MachineState::Constructed};

    /// Cooperative stop request: a running step polls it at tile boundaries
    /// and wraps up (-> Interrupted), bounding the executor's wait by one tile
    /// instead of one step.
    std::atomic<bool> interrupt_requested{false};

    /// The queued/running step's pool handle: its CAS arbitrates the
    /// queued-pickup race (revoke vs run), its `get` is the release wait.
    std::shared_ptr<JobHandle> current_step;

    /// The machine's current step body; run by a pool worker (or inline).
    /// Must touch ONLY machine-owned state, never shared executor members.
    std::function<StepResult()> run_step;

    /// Set when `run_step` threw; the executor rethrows it at collect (fetch
    /// steps are mandatory work) or logs and abandons (put steps).
    std::exception_ptr failure;

    virtual ~MachineBase() = default;
};

}
