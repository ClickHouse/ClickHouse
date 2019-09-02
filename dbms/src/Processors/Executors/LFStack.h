#include <Processors/Executors/StackAllocator.h>

namespace lfs
{

template <typename T>
class LFStack
{
private:

    struct StackCell
    {
        std::atomic<uint64_t> next;
        std::atomic<T *> data;

        constexpr static uint64_t POS_MASK = (1ull << 32u) - 1;
        constexpr static uint64_t TAG_MASK = ~POS_MASK;
    };

    using Allocator = StackAllocator<StackCell>;

    struct StackImpl
    {
        std::atomic<uint64_t> head {0};

        void push(StackCell & cell, uint64_t ptr)
        {
            uint64_t expected = 0;
            cell.next = expected;

            while (!head.compare_exchange_weak(expected, ptr))
                cell.next = expected;
        }

        uint64_t pop(Allocator & allocator_)
        {
            uint64_t expected = 0;
            uint64_t desired = 0;

            while (!head.compare_exchange_weak(expected, desired))
                desired = expected ? allocator_[expected & StackCell::POS_MASK].next.load()
                                   : 0;

            return expected;
        }
    };

    void increaseTag(uint64_t & ptr)
    {
        uint64_t tag = ptr & StackCell::TAG_MASK;

        /// Tag is never zero, so ptr is never zero too.
        do
            tag += (1ull << 32u);
        while ((tag & StackCell::TAG_MASK) == 0);

        ptr = (ptr & StackCell::POS_MASK) | (tag & StackCell::TAG_MASK);
    }

public:
    void push(T * value)
    {
        uint64_t ptr = free_list.pop(allocator);

        if (!ptr)
            ptr = allocator.alloc();

        auto & cell = allocator[ptr & StackCell::POS_MASK];
        cell.data.store(value); //, std::memory_order_relaxed);

        increaseTag(ptr);
        stack.push(cell, ptr);
    }

    bool pop(T *& value)
    {
        uint64_t ptr = stack.pop(allocator);

        if (!ptr)
            return false;

        auto & cell = allocator[ptr & StackCell::POS_MASK];
        value = cell.data.load(); //std::memory_order_relaxed);

        increaseTag(ptr);
        free_list.push(cell, ptr);

        return true;
    }

private:
    Allocator allocator;
    StackImpl stack;
    StackImpl free_list;
};

}
