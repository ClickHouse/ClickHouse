#pragma once

#include <atomic>
#include <memory>
#include <optional>

#include <boost/noncopyable.hpp>


namespace DB
{

/// The outcome of a write, decided by the answers to its attempts.
enum class WriteOutcome : uint8_t
{
    /// The write is applied: it succeeded, or an earlier attempt of it is proven to be applied.
    Applied,
    /// No attempt was applied, and no attempt can be applied later.
    NotApplied,
    /// An attempt may have been applied, and nothing proves whether it was.
    Unknown,
};

/// Tracks what the attempts of one write tell about its outcome. The caller creates it, passes it down with the write,
/// and reads the outcome after the write, also after an exception. The attempts can run on other threads, so it is
/// thread-safe.
class WriteOutcomeTracker
{
public:
    /// The storage tracks the outcome of the write. Without it, an outcome that is not set proves nothing.
    void attach() { attached.store(true); }
    bool isAttached() const { return attached.load(); }

    /// An attempt got no answer that proves that it was not applied.
    void markMayHaveBeenApplied() { may_have_been_applied.store(true); }
    bool mayHaveBeenApplied() const { return may_have_been_applied.load(); }

    void setOutcome(WriteOutcome value) { outcome.store(static_cast<uint8_t>(value)); }
    void resetOutcome() { outcome.store(not_set); }
    /// Empty until a call of the request that publishes the object ends.
    std::optional<WriteOutcome> getOutcome() const;

    /// Makes a tracker the current one of this thread while the scope lives, so that the attempts sent on this thread
    /// reach it. Scopes nest.
    class Scope : private boost::noncopyable
    {
    public:
        explicit Scope(std::shared_ptr<WriteOutcomeTracker> tracker_);
        ~Scope();

    private:
        friend class WriteOutcomeTracker;

        const std::shared_ptr<WriteOutcomeTracker> tracker;
        Scope * const previous;
    };

    /// The tracker of the current thread, or null.
    static std::shared_ptr<WriteOutcomeTracker> current();
    /// The same without shared ownership, for the checks of every attempt.
    static WriteOutcomeTracker * currentRaw();

    /// Decides one call of the request that publishes the object. It resets the outcome, and at an exit without an
    /// outcome, e.g. by an exception, it sets `Unknown` after an attempt that may have been applied, else `NotApplied`.
    /// Without a tracker it does nothing.
    class ExitGuard : private boost::noncopyable
    {
    public:
        explicit ExitGuard(std::shared_ptr<WriteOutcomeTracker> tracker_);
        ~ExitGuard();

        void set(WriteOutcome value);

    private:
        const std::shared_ptr<WriteOutcomeTracker> tracker;
    };

private:
    static constexpr uint8_t not_set = 0xFF;

    std::atomic<bool> attached = false;
    std::atomic<bool> may_have_been_applied = false;
    std::atomic<uint8_t> outcome = not_set;
};

}
