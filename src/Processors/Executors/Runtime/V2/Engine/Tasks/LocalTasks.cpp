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
    UInt32 split;
};

Bounds unpackBounds(UInt64 word)
{
    return std::bit_cast<Bounds>(word);
}

UInt64 packBounds(Bounds bounds)
{
    return std::bit_cast<UInt64>(bounds);
}

Int32 distance(UInt32 from, UInt32 to)
{
    return std::bit_cast<Int32>(to - from);
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

void LocalTasks::setSplit(UInt32 new_split)
{
    Wyrd::SpinWait spin_wait;

    UInt64 word = bounds.load();
    while (!bounds.compare_exchange_weak(word, packBounds({unpackBounds(word).tail, new_split})))
        spin_wait.spin();

    owner_split = new_split;
}

void LocalTasks::growShared()
{
    const UInt32 current_head = head.load(std::memory_order::relaxed);
    const UInt32 private_size = current_head - owner_split;
    setSplit(current_head - private_size / 2);
    split_request.store(false, std::memory_order::relaxed);
}

bool LocalTasks::shrinkShared()
{
    const auto [tail, split] = unpackBounds(bounds.load());
    if (tail != split)
    {
        const UInt32 shared_size = split - tail;
        const UInt32 new_split = tail + shared_size / 2;
        setSplit(new_split);

        const auto [tail_now, split_now] = unpackBounds(bounds.load());
        if (tail_now != split)
        {
            if (distance(new_split, tail_now) > 0)
            {
                const UInt32 shared_size_now = split - tail_now;
                setSplit(tail_now + shared_size_now / 2);
            }
            return false;
        }
    }

    all_stolen.store(true);
    owner_all_stolen = true;
    return true;
}

void LocalTasks::afterPush(UInt32 first)
{
    if (owner_all_stolen)
    {
        bounds.store(packBounds({first, first + 1}));
        all_stolen.store(false);
        split_request.store(false, std::memory_order::relaxed);
        owner_split = first + 1;
        owner_all_stolen = false;
    }
    else if (split_request.load(std::memory_order::relaxed))
    {
        growShared();
    }
}

bool LocalTasks::push(const Task & task)
{
    const UInt32 current_head = head.load(std::memory_order::relaxed);
    const auto [tail, split] = unpackBounds(bounds.load());
    if (current_head - tail >= capacity)
        return false;

    setSlot(buffer, current_head, task);

    head.store(current_head + 1, std::memory_order::relaxed);
    afterPush(current_head);
    return true;
}

UInt32 LocalTasks::pushMany(std::span<const Task> many)
{
    const UInt32 current_head = head.load(std::memory_order::relaxed);
    const auto [tail, split] = unpackBounds(bounds.load());
    const UInt32 count = std::min(static_cast<UInt32>(many.size()), capacity - (current_head - tail));
    if (count == 0)
        return 0;

    for (UInt32 offset = 0; offset < count; ++offset)
        setSlot(buffer, current_head + offset, many[offset]);

    head.store(current_head + count, std::memory_order::relaxed);
    afterPush(current_head);
    return count;
}

std::optional<Task> LocalTasks::pop()
{
    if (owner_all_stolen)
        return std::nullopt;

    UInt32 current_head = head.load(std::memory_order::relaxed);
    if (owner_split == current_head)
        if (shrinkShared())
            return std::nullopt;

    --current_head;
    head.store(current_head, std::memory_order::relaxed);
    const Task task = getSlot(buffer, current_head);

    if (split_request.load(std::memory_order::relaxed))
        growShared();

    return task;
}

UInt32 LocalTasks::offload(std::span<Task> out)
{
    if (owner_all_stolen)
        return 0;

    const auto [tail, split] = unpackBounds(bounds.load());
    if (tail == split)
        growShared();

    return grab(out);
}

UInt32 LocalTasks::grab(std::span<Task> out)
{
    if (all_stolen.load(std::memory_order::relaxed))
        return 0;

    UInt64 snapshot = bounds.load();
    const auto [tail, split] = unpackBounds(snapshot);
    const Int32 shared = distance(tail, split);
    if (shared <= 0)
    {
        if (!split_request.load(std::memory_order::relaxed))
            split_request.store(true, std::memory_order::relaxed);

        return 0;
    }

    const UInt32 count = std::min<UInt32>(static_cast<UInt32>(out.size()), (shared + 1) / 2);
    for (UInt32 offset = 0; offset < count; ++offset)
        out[offset] = getSlot(buffer, tail + offset);

    if (!bounds.compare_exchange_strong(snapshot, packBounds({tail + count, split})))
        return 0;

    return count;
}

UInt32 LocalTasks::size() const
{
    const UInt32 current_head = head.load(std::memory_order::relaxed);
    const auto [tail, split] = unpackBounds(bounds.load());
    return std::max<Int32>(distance(tail, current_head), 0);
}

}

}
