#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcReadAhead.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>

#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event CASGCReadAheadHit;
    extern const Event CASGCReadAheadMiss;
    extern const Event CASGCReadAheadWasted;
}

namespace DB::Cas
{

GcReadAhead::GcReadAhead(CasOperation & op_, CasRequests & requests_, ThreadPool & pool_, size_t concurrency_)
    : op(op_), requests(requests_), pool(pool_), concurrency(concurrency_), generation(op_.generation())
{
}

GcReadAhead::~GcReadAhead()
{
    /// A worker holds its own slot and touches `requests`, which outlives this object; waiting here is
    /// what keeps every worker inside the round that issued it. `wait`, not `get`: an exception nobody
    /// took is dropped with the result it belongs to, and a destructor may not throw.
    size_t wasted = 0;
    for (auto & [key, slot] : reads)
    {
        slot->future.wait();
        ++wasted;
    }
    for (auto & [key, slot] : heads)
    {
        slot->future.wait();
        ++wasted;
    }
    if (wasted != 0)
        ProfileEvents::increment(ProfileEvents::CASGCReadAheadWasted, wasted);
}

template <typename T, typename Request>
void GcReadAhead::hint(Slots<T> & slots, const String & key, Request request)
{
    if (concurrency <= 1 || slots.contains(key))
        return;

    auto slot = std::make_shared<Slot<T>>();
    slots.emplace(key, slot);
    try
    {
        pool.scheduleOrThrowOnError([slot, key, requests_ptr = &requests, gen = generation, request]
        {
            try
            {
                CasOperation worker = requests_ptr->resume(gen);
                slot->promise.set_value(request(worker, key));
            }
            catch (...)
            {
                slot->promise.set_exception(std::current_exception());
            }
        });
    }
    catch (...)
    {
        /// Nothing will ever satisfy this slot's promise, so a later take would wait on it forever.
        /// Drop it and let the take read inline; the scheduling failure itself propagates to the
        /// hinting site, which is a round-thread site like any other.
        slots.erase(key);
        throw;
    }
}

template <typename T, typename Inline>
std::optional<T> GcReadAhead::take(Slots<T> & slots, const String & key, Inline inline_request)
{
    const auto it = slots.find(key);
    if (it == slots.end())
    {
        ProfileEvents::increment(ProfileEvents::CASGCReadAheadMiss);
        return inline_request(op, key);
    }

    std::shared_ptr<Slot<T>> slot = std::move(it->second);
    slots.erase(it);
    ProfileEvents::increment(ProfileEvents::CASGCReadAheadHit);
    /// Rethrows the worker's exception at the site that would otherwise have read inline, so a
    /// transport failure fails the round from the same place and with the same type it always did.
    return slot->future.get();
}

void GcReadAhead::hintRead(const String & key)
{
    hint<Object>(reads, key,
        [](CasOperation & worker, const String & k) { return worker.read(k, Retry::standard()); });
}

void GcReadAhead::hintHead(const String & key)
{
    hint<Meta>(heads, key,
        [](CasOperation & worker, const String & k) { return worker.head(k, Retry::standard()); });
}

template <typename T>
void GcReadAhead::discard(Slots<T> & slots, const String & key)
{
    const auto it = slots.find(key);
    if (it == slots.end())
        return;
    std::shared_ptr<Slot<T>> slot = std::move(it->second);
    slots.erase(it);
    /// `wait`, not `get`: the result and any exception belong to a request nobody wanted.
    slot->future.wait();
    ProfileEvents::increment(ProfileEvents::CASGCReadAheadWasted);
}

void GcReadAhead::discardRead(const String & key)
{
    discard<Object>(reads, key);
}

void GcReadAhead::discardHead(const String & key)
{
    discard<Meta>(heads, key);
}

std::optional<Object> GcReadAhead::takeRead(const String & key)
{
    return take<Object>(reads, key,
        [](CasOperation & inline_op, const String & k) { return inline_op.read(k, Retry::standard()); });
}

std::optional<Meta> GcReadAhead::takeHead(const String & key)
{
    return take<Meta>(heads, key,
        [](CasOperation & inline_op, const String & k) { return inline_op.head(k, Retry::standard()); });
}

}
