#include <Processors/Executors/Runtime/V2/Engine/Queues/LocalQueue.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>

#include <fmt/format.h>

#include <algorithm>
#include <bit>

namespace DB
{

namespace Runtime::V2
{

namespace
{

static_assert((LocalQueue::capacity & (LocalQueue::capacity - 1)) == 0);

struct Bounds
{
    UInt32 tail;
    UInt32 taken;
};

Bounds unpackBounds(UInt64 word)
{
    return std::bit_cast<Bounds>(word);
}

UInt64 packBounds(Bounds bounds)
{
    return std::bit_cast<UInt64>(bounds);
}

Int32 signedDifference(UInt32 lhs, UInt32 rhs)
{
    return std::bit_cast<Int32>(lhs - rhs);
}

void setSlot(auto & buffer, UInt32 index, ExecutingProcessor * value)
{
    buffer[index % LocalQueue::capacity].value.store(value, std::memory_order::relaxed);
}

ExecutingProcessor * getSlot(const auto & buffer, UInt32 index)
{
    return buffer[index % LocalQueue::capacity].value.load(std::memory_order::relaxed);
}

}

LocalQueue::LocalQueue(std::string name)
{
    Wyrd::setName(head, "{}.head", name);
    Wyrd::setName(bounds, "{}.bounds", name);
    for (UInt32 index = 0; index < capacity; ++index)
        Wyrd::setName(buffer[index].value, "{}.slot[{}]", name, index);
}

UInt32 LocalQueue::pushManyLifo(std::span<ExecutingProcessor * const> many)
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    const UInt32 count = std::min(static_cast<UInt32>(many.size()), capacity - (current_head - tail));
    for (UInt32 offset = 0; offset < count; ++offset)
        setSlot(buffer, current_head + offset, many[offset]);

    head.store(current_head + count);
    return count;
}

ExecutingProcessor * LocalQueue::popLifo()
{
    const UInt32 new_head = head.load() - 1;
    head.store(new_head);

    Wyrd::SpinWait spin_wait;
    UInt64 word = bounds.load();
    while (true)
    {
        const auto [tail, taken] = unpackBounds(word);
        const Int32 available = signedDifference(new_head, taken);
        if (available < 0)
        {
            /// The thief inside may still hand slots back in its trim; after its release the word is final.
            if (tail != taken)
            {
                spin_wait.spin();
                word = bounds.load();
                continue;
            }

            /// No tasks left.
            head.store(new_head + 1);
            return nullptr;
        }
        else if (available > 0)
        {
            /// Pop was committed with store.
            return getSlot(buffer, new_head);
        }
        else
        {
            /// Some thief is working with a smaller segment.
            if (tail != taken)
                return getSlot(buffer, new_head);

            /// Need to synchronize with the thief - it may started the stealing overlapping with new_head.
            if (bounds.compare_exchange_weak(word, packBounds({taken + 1, taken + 1})))
            {
                head.store(new_head + 1);
                return getSlot(buffer, new_head);
            }

            /// Lost to a thief's steal start
            spin_wait.spin();
        }
    }
}

UInt32 LocalQueue::stealFifo(std::span<ExecutingProcessor *> out)
{
    /// Try lock steal range.
    Wyrd::SpinWait spin_wait;
    UInt64 word = bounds.load();
    UInt32 steal_begin = 0;
    UInt32 steal_end = 0;
    while (true)
    {
        const auto [tail, taken] = unpackBounds(word);
        if (tail != taken)
            return 0;

        const Int32 available = signedDifference(head.load(), tail);
        if (available <= 0)
            return 0;

        const UInt32 count = std::min<UInt32>(static_cast<UInt32>(out.size()), available - available / 2);
        steal_begin = tail;
        steal_end = tail + count;
        if (bounds.compare_exchange_weak(word, packBounds({steal_begin, steal_end})))
            break;

        spin_wait.spin();
    }

    /// Lower the range end: between the head read above and the steal start, pop may have taken slots.
    const UInt32 head_now = head.load();
    if (signedDifference(steal_end, head_now) > 0)
    {
        steal_end = signedDifference(head_now, steal_begin + 1) > 0 ? head_now : steal_begin + 1;
        bounds.store(packBounds({steal_begin, steal_end}));
    }

    /// Steal the batch.
    for (UInt32 index = steal_begin; index != steal_end; ++index)
        out[index - steal_begin] = getSlot(buffer, index);

    /// Commit steal by advancing (tail, taken) pair.
    bounds.store(packBounds({steal_end, steal_end}));
    return steal_end - steal_begin;
}

UInt32 LocalQueue::size() const
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    return std::max<Int32>(signedDifference(current_head, tail), 0);
}

UInt32 LocalQueue::freeSlots() const
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    return capacity - (current_head - tail);
}

}

}
