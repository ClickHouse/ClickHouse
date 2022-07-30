#pragma once
#include <Common/Exception.h>
#include <base/defines.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Simple struct which stores threads with numbers [0 .. num_threads - 1].
/// Allows to push and pop specified thread, or pop any thread if has.
/// All operations (except init) are O(1). No memory allocations after init happen.
struct ThreadsQueue
{
    void init(size_t num_threads)
    {
        stack_size = 0;
        stack.clear();
        thread_pos_in_stack.clear();

        stack.reserve(num_threads);
        thread_pos_in_stack.reserve(num_threads);

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            stack.emplace_back(thread);
            thread_pos_in_stack.emplace_back(thread);
        }
    }

    bool has(size_t thread) const { return thread_pos_in_stack[thread] < stack_size; }
    size_t size() const { return stack_size; }
    bool empty() const { return stack_size == 0; }

    void push(size_t thread)
    {
        if (unlikely(has(thread)))
            throw Exception("Can't push thread because it is already in threads queue", ErrorCodes::LOGICAL_ERROR);

        swapThreads(thread, stack[stack_size]);
        ++stack_size;
    }

    void pop(size_t thread)
    {
        if (unlikely(!has(thread)))
            throw Exception("Can't pop thread because it is not in threads queue", ErrorCodes::LOGICAL_ERROR);

        --stack_size;
        swapThreads(thread, stack[stack_size]);
    }

    size_t popAny()
    {
        if (unlikely(stack_size == 0))
            throw Exception("Can't pop from empty queue", ErrorCodes::LOGICAL_ERROR);

        --stack_size;
        return stack[stack_size];
    }

private:
    std::vector<size_t> stack;
    std::vector<size_t> thread_pos_in_stack;
    size_t stack_size = 0;

    void swapThreads(size_t first, size_t second)
    {
        std::swap(thread_pos_in_stack[first], thread_pos_in_stack[second]);
        std::swap(stack[thread_pos_in_stack[first]], stack[thread_pos_in_stack[second]]);
    }
};

}
