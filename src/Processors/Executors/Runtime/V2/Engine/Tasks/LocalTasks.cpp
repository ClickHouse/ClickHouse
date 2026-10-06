#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>

#include <algorithm>
#include <bit>

namespace DB
{

namespace Runtime::V2
{

namespace
{

static_assert((LocalTasks::capacity & (LocalTasks::capacity - 1)) == 0);

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

void setSlot(auto & buffer, UInt32 index, const Task & task)
{
    buffer[index % LocalTasks::capacity].packed.store(Task::pack(task), std::memory_order::relaxed);
}

Task getSlot(const auto & buffer, UInt32 index)
{
    return Task::unpack(buffer[index % LocalTasks::capacity].packed.load(std::memory_order::relaxed));
}

}

bool LocalTasks::push(const Task & task)
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    if (current_head - tail >= capacity)
        return false;

    setSlot(buffer, current_head, task);
    head.store(current_head + 1);
    return true;
}

UInt32 LocalTasks::pushMany(std::span<const Task> many)
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    const UInt32 count = std::min(static_cast<UInt32>(many.size()), capacity - (current_head - tail));
    for (UInt32 offset = 0; offset < count; ++offset)
        setSlot(buffer, current_head + offset, many[offset]);

    head.store(current_head + count);
    return count;
}

std::optional<Task> LocalTasks::pop()
{
    const UInt32 new_head = head.load() - 1;
    head.store(new_head);

    UInt64 word = bounds.load();
    const auto [tail, taken] = unpackBounds(word);
    const Int32 available = signedDifference(new_head, taken);
    if (available < 0)
    {
        /// No tasks left.
        head.store(new_head + 1);
        return std::nullopt;
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
        if (bounds.compare_exchange_strong(word, packBounds({taken + 1, taken + 1})))
        {
            head.store(new_head + 1);
            return getSlot(buffer, new_head);
        }

        /// Thief locked last task.
        head.store(new_head + 1);
        return std::nullopt;
    }
}

UInt32 LocalTasks::grab(std::span<Task> out)
{
    UInt64 word = bounds.load();

    /// Try lock steal range.
    const auto [tail, taken] = unpackBounds(word);
    if (tail != taken)
        return 0;

    const Int32 available = signedDifference(head.load(), tail);
    if (available <= 0)
        return 0;

    const UInt32 count = std::min<UInt32>(static_cast<UInt32>(out.size()), available - available / 2);
    if (!bounds.compare_exchange_strong(word, packBounds({tail, tail + count})))
        return 0;

    UInt32 grab_begin = tail;
    UInt32 grab_end = tail + count;

    /// Lower the range end: between the head read above and the claim, pop may have taken slots
    const UInt32 head_now = head.load();
    if (signedDifference(grab_end, head_now) > 0)
    {
        grab_end = signedDifference(head_now, grab_begin + 1) > 0 ? head_now : grab_begin + 1;
        bounds.store(packBounds({grab_begin, grab_end}));
    }

    for (UInt32 index = grab_begin; index != grab_end; ++index)
        out[index - grab_begin] = getSlot(buffer, index);

    bounds.store(packBounds({grab_end, grab_end}));
    return grab_end - grab_begin;
}

UInt32 LocalTasks::size() const
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    return std::max<Int32>(signedDifference(current_head, taken), 0);
}

}

}
