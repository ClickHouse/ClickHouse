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

    Wyrd::SpinWait spin_wait;
    UInt64 word = bounds.load();
    while (true)
    {
        const auto [tail, taken] = unpackBounds(word);
        const Int32 below = signedDifference(new_head, taken);
        if (below < 0)
        {
            /// No tasks left.
            head.store(new_head + 1);
            return std::nullopt;
        }
        else if (below > 0)
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
        }

        spin_wait.spin();
    }
}

UInt32 LocalTasks::grab(std::span<Task> out)
{
    Wyrd::SpinWait spin_wait;
    UInt64 word = bounds.load();

    /// Try lock steal range.
    UInt32 first = 0;
    UInt32 end = 0;
    while (true)
    {
        const auto [tail, taken] = unpackBounds(word);
        if (tail != taken)
            return 0;

        const Int32 available = signedDifference(head.load(), taken);
        if (available <= 0)
            return 0;

        const UInt32 count = std::min<UInt32>(static_cast<UInt32>(out.size()), available - available / 2);
        first = taken;
        end = taken + count;
        if (bounds.compare_exchange_weak(word, packBounds({first, end})))
            break;

        spin_wait.spin();
    }

    const UInt32 head_now = head.load();
    if (signedDifference(end, head_now) > 0)
    {
        end = signedDifference(head_now, first + 1) > 0 ? head_now : first + 1;
        bounds.store(packBounds({first, end}));
    }

    for (UInt32 index = first; index != end; ++index)
        out[index - first] = getSlot(buffer, index);

    bounds.store(packBounds({end, end}));

    return end - first;
}

UInt32 LocalTasks::size() const
{
    const UInt32 current_head = head.load();
    const auto [tail, taken] = unpackBounds(bounds.load());
    return std::max<Int32>(signedDifference(current_head, taken), 0);
}

}

}
