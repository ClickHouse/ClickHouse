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

/// A step's verdict. Every transition is executor-mediated; a worker never
/// schedules the next step.
enum class StepResult : uint8_t
{
    AwaitCollect,
    Interrupted, /// wrapped up at an interrupt point, partial products kept
    Done,
};

/// The protocol half of a fetch machine - everything the runner needs to
/// schedule, revoke, interrupt and join one, with no knowledge of what the
/// steps do. `ReaderExecutor::FetchMachine` inherits this and adds the payload
/// (connection cluster, fetched rope, job-local stats, ...).
struct MachineBase
{
    virtual ~MachineBase() = default;

    std::atomic<MachineState> state{MachineState::Constructed};

    /// The ONE cooperative stop request, polled at safe points. Stop policy:
    /// a LIVE connection stops at the next block (saved with the machine,
    /// continues from its frontier later); a one-shot GET is NEVER cut
    /// mid-response - the stop lands between connections. Its production
    /// setter is the cancel path, which does not wait: the machine goes to
    /// the soft list and is reaped after release.
    std::atomic<bool> interrupt_requested{false};

    /// The queued/running step's pool handle: its CAS arbitrates the
    /// queued-pickup race (revoke vs run), its `get` is the release wait.
    std::shared_ptr<JobHandle> current_step;

    /// The current step body, run by a pool worker (or inline). Must touch
    /// ONLY machine-owned state, never shared executor members.
    std::function<StepResult()> run_step;

    /// Set when `run_step` threw; the executor rethrows at collect (fetch
    /// steps are mandatory work) or logs and abandons (put steps).
    std::exception_ptr failure;
};

}
