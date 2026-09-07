#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

namespace DB
{

namespace
{

struct Bits
{
    uint64_t locked : 1;
    uint64_t finished : 1;
    uint64_t notifications : 62;
};

union Packed
{
    Bits bits;
    uint64_t raw;
};

static_assert(sizeof(Packed) == sizeof(uint64_t));

Packed unpack(uint64_t raw)
{
    Packed packed{};
    packed.raw = raw;
    return packed;
}

uint64_t oneNotification()
{
    Packed packed{};
    packed.bits.notifications = 1;
    return packed.raw;
}

}

uint64_t ProcessorLock::snapshot() const
{
    return word.load();
}

bool ProcessorLock::tryUnlock(uint64_t snapshot)
{
    Packed unlocked = unpack(snapshot);
    unlocked.bits.locked = 0;
    return word.compare_exchange_strong(snapshot, unlocked.raw);
}

bool ProcessorLock::tryLock()
{
    uint64_t expected = word.load();
    while (true)
    {
        Packed locked = unpack(expected);
        if (locked.bits.locked || locked.bits.finished)
            return false;

        locked.bits.locked = 1;
        if (word.compare_exchange_weak(expected, locked.raw))
            return true;
    }
}

bool ProcessorLock::isFinished() const
{
    return unpack(word.load()).bits.finished;
}

void ProcessorLock::finish()
{
    Packed finished = unpack(word.load());
    finished.bits.locked = 0;
    finished.bits.finished = 1;
    word.store(finished.raw);
}

void ProcessorLock::notify()
{
    word.fetch_add(oneNotification());
}

}
