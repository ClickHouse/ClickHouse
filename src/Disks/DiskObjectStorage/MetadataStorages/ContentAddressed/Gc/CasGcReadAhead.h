#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <Common/ThreadPool.h>

#include <future>
#include <memory>
#include <optional>
#include <unordered_map>

namespace DB::Cas
{

/// Read-ahead in front of ONE admitted operation. A caller HINTS keys the sequential code will read
/// next; workers fetch them on `pool`, each through an operation resumed under the SAME admitted
/// generation as the caller's (no liveness -- exactly the fold's own admission); a TAKE returns the
/// fetched result, rethrows the worker's exception, or -- for a key nobody hinted -- performs the
/// request inline. This is a cache of RESULTS, never of decisions: every decode, counter and event
/// stays at the take site, so a round at concurrency 1 (nothing is ever hinted) and a round at 16
/// read the same keys in the same order and decide the same way; only WHEN the bytes were fetched
/// moves.
///
/// Why a result may be fetched early: the objects the fold reads are write-once (a present body is
/// the same body later), an absent position at or below a checkpoint's `committed_through` was
/// durable before the round began (it is a gap whenever it is read), and a manifest body still being
/// uploaded when the early read lands yields the same hold a slightly earlier sequential read yields
/// today. Nothing is hinted above `committed_through`, so no request is issued that the sequential
/// walk would not issue -- with one bounded exception: a ref-log hinting site does not yet know where
/// an epoch's closing seal is (it learns that only by decoding the log at that position), so it may
/// hint ids past the seal, inside the SAME epoch, that turn out not to exist. Those are discarded
/// rather than taken, overshooting by at most one window per epoch crossing, and counted wasted.
///
/// Memory is the CALLER's to bound: `pending` counts hinted-but-untaken slots and `window` is how
/// many a hinting site keeps in flight. A key hinted twice is one request. Results never taken are
/// awaited by the destructor and counted as wasted, or discarded explicitly by `discardRead`/
/// `discardHead`, which counts them at once. Only the owning thread touches the maps; a worker
/// touches only its own slot.
class GcReadAhead
{
public:
    GcReadAhead(CasOperation & op_, CasRequests & requests_, ThreadPool & pool_, size_t concurrency_);
    ~GcReadAhead();

    GcReadAhead(const GcReadAhead &) = delete;
    GcReadAhead & operator=(const GcReadAhead &) = delete;

    void hintRead(const String & key);
    void hintHead(const String & key);

    std::optional<Object> takeRead(const String & key);
    std::optional<Meta> takeHead(const String & key);

    /// Drops a hinted key the caller will never take. The request is already in flight or done; the
    /// wait is bounded by that one request, its result and its exception are dropped, and the slot is
    /// counted as wasted now rather than in the destructor. The exception is dropped because no
    /// sequential walk would have issued this request, so none could have failed on it; the fence and
    /// the budget are re-observed by the very next request anyway. A key nobody hinted is a no-op.
    void discardRead(const String & key);
    void discardHead(const String & key);

    /// Hinted but not yet taken, both verbs together: what a hinting site throttles itself against.
    size_t pending() const { return reads.size() + heads.size(); }

    /// How many requests a hinting site should keep in flight. Zero at concurrency 1, which is what
    /// makes every `while (pending() < window())` loop hint nothing at all on the sequential setting.
    size_t window() const { return concurrency <= 1 ? 0 : 4 * concurrency; }

private:
    template <typename T>
    struct Slot
    {
        std::promise<std::optional<T>> promise;
        std::future<std::optional<T>> future;
        Slot() : future(promise.get_future()) {}
    };

    template <typename T>
    using Slots = std::unordered_map<String, std::shared_ptr<Slot<T>>>;

    template <typename T, typename Request>
    void hint(Slots<T> & slots, const String & key, Request request);

    template <typename T, typename Inline>
    std::optional<T> take(Slots<T> & slots, const String & key, Inline inline_request);

    template <typename T>
    void discard(Slots<T> & slots, const String & key);

    CasOperation & op;
    CasRequests & requests;
    ThreadPool & pool;
    const size_t concurrency;
    /// The generation the caller's operation was admitted under. A worker resumes under exactly this
    /// one, so a fence that moves under the round fails a worker's request the way it fails the
    /// round's own -- never with a fresher admission the round itself would not have had.
    const uint64_t generation;

    Slots<Object> reads;
    Slots<Meta> heads;
};

}
