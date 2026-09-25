#include <algorithm>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <Common/MemoryTrackerUtils.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Processors/IProcessor.h>


namespace DB
{
void MemorySpillScheduler::checkAndSpill(IProcessor * processor)
{
    chassert(processor->isSpillable());

    MemoryRecoveryEpisodePtr episode;
    {
        std::lock_guard lock(mutex);
        episode = active_recovery.lock();
    }

    const bool force_spill = episode
        && !episode->closed.load(std::memory_order_acquire)
        && !episode->completed.load(std::memory_order_acquire);
    const auto stats = processor->getMemoryStats();

    if (force_spill)
    {
        bool should_spill = false;
        bool should_run_dedicated_spill = false;
        {
            std::lock_guard lock(mutex);
            auto active = active_recovery.lock();
            if (!active || active.get() != episode.get()
                || episode->closed.load(std::memory_order_relaxed)
                || episode->completed.load(std::memory_order_relaxed))
                return;

            auto [state_it, inserted] = processor_states.try_emplace(processor);
            if (inserted)
                ++forced_spill_remaining;
            auto & state = state_it->second;
            state.stats = stats;
            if (state.claimed_forced_epoch >= episode->id)
                return;

            state.claimed_forced_epoch = episode->id;
            state.memory_before_spill = getCurrentQueryMemoryUsage();
            state.spill_requested = false;
            should_spill = stats.spillable_memory_bytes > 0;
            should_run_dedicated_spill = !should_spill;
            state.dedicated_spill_in_progress = should_run_dedicated_spill;
        }

        if (should_run_dedicated_spill)
        {
            const auto pressure_result = processor->spillForMemoryPressure();
            const bool spilled = pressure_result == IProcessor::MemoryPressureSpillResult::Progress;
            const bool spill_pending = pressure_result == IProcessor::MemoryPressureSpillResult::Pending;
            const Int64 memory_after = getCurrentQueryMemoryUsage();

            std::lock_guard lock(mutex);
            auto state = processor_states.find(processor);
            if (state == processor_states.end()
                || state->second.claimed_forced_epoch != episode->id
                || state->second.completed_forced_epoch >= episode->id)
                return;

            state->second.dedicated_spill_in_progress = false;
            const Int64 reclaimed_bytes = std::max<Int64>(state->second.memory_before_spill - memory_after, 0);
            if (spilled || reclaimed_bytes > 0)
            {
                episode->outcome.store(ForcedSpillOutcome::Progress, std::memory_order_relaxed);
                episode->reclaimed_bytes.fetch_add(reclaimed_bytes, std::memory_order_relaxed);
            }
            if (spill_pending)
            {
                state->second.spill_requested = true;
                return;
            }
            completeForcedSpillProcessor(episode, state->second);
            return;
        }

        const bool spill_succeeded = processor->spillOnSize(stats.spillable_memory_bytes);

        std::lock_guard lock(mutex);
        auto state = processor_states.find(processor);
        if (state == processor_states.end()
            || state->second.claimed_forced_epoch != episode->id)
            return;

        state->second.spill_requested = spill_succeeded;
        return;
    }

    if (!enable || !getHardLimit())
        return;

    if (processor == selectSpilledProcessor(processor, stats, false))
        processor->spillOnSize(stats.spillable_memory_bytes);
}

void MemorySpillScheduler::finishSpill(IProcessor * processor)
{
    MemoryRecoveryEpisodePtr episode;
    {
        std::lock_guard lock(mutex);
        episode = active_recovery.lock();
    }
    if (!episode || episode->completed.load(std::memory_order_acquire))
        return;

    const bool spill_pending = processor->hasPendingSpill();
    const Int64 memory_after = getCurrentQueryMemoryUsage();
    std::lock_guard lock(mutex);

    auto state = processor_states.find(processor);
    if (state == processor_states.end()
        || state->second.claimed_forced_epoch != episode->id
        || state->second.dedicated_spill_in_progress
        || state->second.completed_forced_epoch >= episode->id)
        return;

    if (state->second.spill_requested && spill_pending)
        return;

    const Int64 reclaimed_bytes = std::max<Int64>(state->second.memory_before_spill - memory_after, 0);
    if (state->second.spill_requested || reclaimed_bytes > 0)
    {
        episode->outcome.store(ForcedSpillOutcome::Progress, std::memory_order_relaxed);
        episode->reclaimed_bytes.fetch_add(reclaimed_bytes, std::memory_order_relaxed);
    }
    completeForcedSpillProcessor(episode, state->second);
}

void MemorySpillScheduler::registerProcessor(IProcessor * processor)
{
    registerProcessorImpl(processor, {}, false);
}

void MemorySpillScheduler::registerProcessor(const std::shared_ptr<IProcessor> & processor)
{
    registerProcessorImpl(processor.get(), processor, true);
}

void MemorySpillScheduler::registerProcessorImpl(
    IProcessor * processor, std::weak_ptr<IProcessor> lifetime, bool lifetime_tracked)
{
    if (!processor->isSpillable())
        return;

    std::lock_guard lock(mutex);
    const auto [state, inserted] = processor_states.try_emplace(processor);
    if (lifetime_tracked)
    {
        state->second.lifetime = std::move(lifetime);
        state->second.lifetime_tracked = true;
    }

    auto episode = active_recovery.lock();
    if (inserted && episode
        && !episode->closed.load(std::memory_order_relaxed)
        && !episode->completed.load(std::memory_order_relaxed))
        ++forced_spill_remaining;
    updateTopProcessor();
}

MemoryRecoveryEpisodePtr MemorySpillScheduler::requestForcedSpill()
{
    std::lock_guard lock(mutex);

    if (auto current = active_recovery.lock())
    {
        if (!current->closed.load(std::memory_order_acquire))
            return current;

        /// A timed-out callback cannot be interrupted safely. Reuse that same episode if another
        /// pressure event arrives before it returns, so the late result/exception still has one owner.
        if (current->async_running.load(std::memory_order_acquire)
            || current->execution_running.load(std::memory_order_acquire))
        {
            current->closed.store(false, std::memory_order_release);
            return current;
        }
    }

    auto episode = std::make_shared<MemoryRecoveryEpisode>();
    episode->id = ++next_recovery_id;
    active_recovery = episode;
    forced_spill_remaining = processor_states.size();

    if (processor_states.empty())
    {
        episode->outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
        episode->completed.store(true, std::memory_order_release);
    }
    return episode;
}

void MemorySpillScheduler::executeForcedSpill(const MemoryRecoveryEpisodePtr & episode)
{
    if (!episode
        || episode->closed.load(std::memory_order_acquire)
        || episode->completed.load(std::memory_order_acquire))
        return;

    std::lock_guard execution_lock(forced_spill_execution_mutex);
    if (episode->closed.load(std::memory_order_acquire)
        || episode->completed.load(std::memory_order_acquire))
        return;

    episode->execution_running.store(true, std::memory_order_release);
    try
    {
        std::unordered_set<const void *> visited_targets;
        /// Keep target identities alive until the pass ends, including after processor removal.
        std::vector<std::shared_ptr<IProcessor>> visited_lifetimes;
        while (!episode->closed.load(std::memory_order_acquire)
            && !episode->completed.load(std::memory_order_acquire))
        {
            IProcessor * processor = nullptr;
            IProcessor * expired_processor = nullptr;
            std::shared_ptr<IProcessor> lifetime;
            {
                std::lock_guard lock(mutex);
                auto active = active_recovery.lock();
                if (!active || active.get() != episode.get())
                    break;

                for (auto & [candidate, state] : processor_states)
                {
                    if (state.completed_forced_epoch >= episode->id
                        || state.claimed_forced_epoch >= episode->id)
                        continue;
                    if (state.lifetime_tracked)
                    {
                        lifetime = state.lifetime.lock();
                        if (!lifetime)
                        {
                            completeForcedSpillProcessor(episode, state);
                            expired_processor = candidate;
                            break;
                        }
                    }
                    processor = candidate;
                    state.claimed_forced_epoch = episode->id;
                    state.dedicated_spill_in_progress = true;
                    break;
                }

                if (expired_processor)
                {
                    processor_states.erase(expired_processor);
                    updateTopProcessor();
                }
            }

            if (expired_processor)
                continue;
            if (!processor)
                break;
            if (episode->closed.load(std::memory_order_acquire))
                break;

            const Int64 memory_before = getCurrentQueryMemoryUsage();
            bool spilled = false;
            if (visited_targets.insert(processor->getMemoryPressureSpillTarget()).second)
            {
                if (lifetime)
                    visited_lifetimes.push_back(lifetime);
                const auto pressure_result = processor->spillForMemoryPressure();
                spilled = pressure_result == IProcessor::MemoryPressureSpillResult::Progress;
                if (pressure_result == IProcessor::MemoryPressureSpillResult::Pending)
                {
                    std::lock_guard lock(mutex);
                    auto state = processor_states.find(processor);
                    if (state != processor_states.end())
                    {
                        state->second.dedicated_spill_in_progress = false;
                        state->second.spill_requested = true;
                    }
                    continue;
                }
            }
            const Int64 reclaimed_bytes = std::max<Int64>(memory_before - getCurrentQueryMemoryUsage(), 0);

            std::lock_guard lock(mutex);
            auto state = processor_states.find(processor);
            if (state == processor_states.end())
                continue;
            state->second.dedicated_spill_in_progress = false;
            if (state->second.completed_forced_epoch >= episode->id)
                continue;
            if (spilled || reclaimed_bytes > 0)
            {
                episode->outcome.store(ForcedSpillOutcome::Progress, std::memory_order_relaxed);
                episode->reclaimed_bytes.fetch_add(reclaimed_bytes, std::memory_order_relaxed);
            }
            completeForcedSpillProcessor(episode, state->second);
        }
    }
    catch (...)
    {
        episode->execution_running.store(false, std::memory_order_release);
        episode->cv.notify_all();
        throw;
    }

    episode->execution_running.store(false, std::memory_order_release);
    episode->cv.notify_all();
}

void MemorySpillScheduler::executeForcedSpillUntil(
    const MemoryRecoveryEpisodePtr & episode,
    std::chrono::steady_clock::time_point deadline)
{
    if (!episode
        || episode->closed.load(std::memory_order_acquire)
        || episode->completed.load(std::memory_order_acquire)
        || std::chrono::steady_clock::now() >= deadline)
        return;

    auto self = weak_from_this().lock();
    if (!self)
    {
        executeForcedSpill(episode);
        return;
    }

    rethrowIfFailed(episode);

    bool expected = false;
    const bool start_pass = episode->async_running.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel);
    if (start_pass)
    {
        auto thread_group = getCurrentThreadGroup();
        try
        {
            ThreadFromGlobalPool spill_thread([self, thread_group, episode]
            {
                std::exception_ptr exception;
                try
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::MEMORY_SPILL);
                    self->executeForcedSpill(episode);
                }
                catch (...)
                {
                    exception = std::current_exception();
                }

                if (exception)
                {
                    std::lock_guard lock(episode->mutex);
                    episode->exception = exception;
                }
                episode->async_running.store(false, std::memory_order_release);
                episode->cv.notify_all();
            });
            spill_thread.detach();
        }
        catch (...)
        {
            episode->async_running.store(false, std::memory_order_release);
            episode->cv.notify_all();
            throw;
        }
    }

    std::unique_lock lock(episode->mutex);
    episode->cv.wait_until(lock, deadline, [&]
    {
        return episode->exception
            || episode->completed.load(std::memory_order_acquire)
            || episode->closed.load(std::memory_order_acquire);
    });
    if (episode->exception)
        std::rethrow_exception(episode->exception);
}

MemorySpillScheduler::ForcedSpillResult MemorySpillScheduler::getForcedSpillResult(
    const MemoryRecoveryEpisodePtr & episode) const
{
    if (!episode || !episode->completed.load(std::memory_order_acquire))
        return {};
    return {
        .outcome = episode->outcome.load(std::memory_order_relaxed),
        .reclaimed_bytes = episode->reclaimed_bytes.load(std::memory_order_relaxed),
    };
}

void MemorySpillScheduler::finishMemoryPressure(const MemoryRecoveryEpisodePtr & episode)
{
    if (!episode)
        return;

    /// Stop starting new spill work for this recovery. An already running callback cannot be
    /// cancelled safely; it retains the episode and publishes any exception there when it returns.
    episode->closed.store(true, std::memory_order_release);
    episode->cv.notify_all();
}

void MemorySpillScheduler::rethrowIfFailed(const MemoryRecoveryEpisodePtr & episode) const
{
    if (!episode)
        return;
    std::lock_guard lock(episode->mutex);
    if (episode->exception)
        std::rethrow_exception(episode->exception);
}

Int64 MemorySpillScheduler::getHardLimit()
{
    if (hard_limit < 0) [[unlikely]]
    {
        auto most_hard_limit = getCurrentQueryHardLimit();
        if (most_hard_limit)
            hard_limit = *most_hard_limit;
        else
            hard_limit = 0;
    }
    return hard_limit;
}

void MemorySpillScheduler::remove(IProcessor * processor)
{
    if (!processor->isSpillable())
        return;

    std::lock_guard lock(mutex);
    auto state = processor_states.find(processor);
    auto episode = active_recovery.lock();
    if (state != processor_states.end()
        && episode
        && !episode->completed.load(std::memory_order_relaxed)
        && state->second.completed_forced_epoch < episode->id)
    {
        completeForcedSpillProcessor(episode, state->second);
    }
    processor_states.erase(processor);
    updateTopProcessor();
}

void MemorySpillScheduler::completeForcedSpillProcessor(
    const MemoryRecoveryEpisodePtr & episode,
    ProcessorState & state)
{
    if (!episode || state.completed_forced_epoch >= episode->id)
        return;

    state.completed_forced_epoch = episode->id;
    auto active = active_recovery.lock();
    if (!active || active.get() != episode.get())
        return;

    chassert(forced_spill_remaining > 0);
    --forced_spill_remaining;
    if (forced_spill_remaining == 0)
    {
        if (episode->outcome.load(std::memory_order_relaxed) == ForcedSpillOutcome::Pending)
            episode->outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
        episode->completed.store(true, std::memory_order_release);
        episode->cv.notify_all();
    }
}

void MemorySpillScheduler::updateTopProcessor()
{
    top_processor = nullptr;
    Int64 max_spillable_memory_bytes = 0;
    max_reserved_memory_bytes = 0;
    for (const auto & [proc, state] : processor_states)
    {
        const auto & stats = state.stats;
        max_reserved_memory_bytes = std::max(stats.need_reserved_memory_bytes, max_reserved_memory_bytes);
        if (!top_processor || stats.spillable_memory_bytes > max_spillable_memory_bytes)
        {
            top_processor = proc;
            max_spillable_memory_bytes = stats.spillable_memory_bytes;
        }
    }
}

IProcessor * MemorySpillScheduler::selectSpilledProcessor(
    IProcessor * current_processor, const ProcessorMemoryStats & mem_stats, bool force_spill)
{
    const auto current_mem_used = force_spill ? 0 : getCurrentQueryMemoryUsage();
    const auto limit = force_spill ? 0 : getHardLimit();
    std::lock_guard lock(mutex);
    processor_states[current_processor].stats = mem_stats;

    max_reserved_memory_bytes = std::max(mem_stats.need_reserved_memory_bytes, max_reserved_memory_bytes);
    if (!force_spill && current_mem_used + max_reserved_memory_bytes < limit)
        return nullptr;

    updateTopProcessor();

    if (!force_spill && current_mem_used + max_reserved_memory_bytes < limit)
        return nullptr;
    return top_processor;
}
}
