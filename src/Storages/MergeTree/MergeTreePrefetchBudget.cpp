#include <Storages/MergeTree/MergeTreePrefetchBudget.h>

namespace DB
{

MergeTreePrefetchSlot::~MergeTreePrefetchSlot()
{
    release();
}

MergeTreePrefetchSlot::MergeTreePrefetchSlot(MergeTreePrefetchSlot && other) noexcept
    : budget(std::move(other.budget))
    , bytes(other.bytes)
{
    other.bytes = 0;
}

MergeTreePrefetchSlot & MergeTreePrefetchSlot::operator=(MergeTreePrefetchSlot && other) noexcept
{
    if (this != &other)
    {
        release();
        budget = std::move(other.budget);
        bytes = other.bytes;
        other.bytes = 0;
    }
    return *this;
}

MergeTreePrefetchSlot::MergeTreePrefetchSlot(std::shared_ptr<MergeTreePrefetchBudget> budget_, size_t bytes_) noexcept
    : budget(std::move(budget_))
    , bytes(bytes_)
{
}

void MergeTreePrefetchSlot::release()
{
    if (budget)
        budget->release(bytes);
    budget.reset();
    bytes = 0;
}


MergeTreePrefetchBudget::MergeTreePrefetchBudget(size_t max_buffers_, size_t max_bytes_)
    : max_buffers(max_buffers_)
    , max_bytes(max_bytes_)
{
}

MergeTreePrefetchSlot MergeTreePrefetchBudget::tryReserve(std::shared_ptr<MergeTreePrefetchBudget> self, size_t bytes)
{
    std::lock_guard lock(mutex);

    if (max_buffers && reserved_buffers >= max_buffers)
        return {};
    if (reserved_bytes + bytes > max_bytes)
        return {};

    ++reserved_buffers;
    reserved_bytes += bytes;
    return MergeTreePrefetchSlot(std::move(self), bytes);
}

bool MergeTreePrefetchBudget::hasCapacity()
{
    std::lock_guard lock(mutex);
    return (!max_buffers || reserved_buffers < max_buffers) && reserved_bytes < max_bytes;
}

void MergeTreePrefetchBudget::release(size_t bytes)
{
    std::lock_guard lock(mutex);
    --reserved_buffers;
    reserved_bytes -= bytes;
}

}
