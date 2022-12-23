#pragma once

#include <concepts>
#include <memory>
#include <iostream>
#include <vector>
#include <algorithm>

#include <Common/StackTrace.h>

struct ITraceable {};

template <typename T>
concept TraceableT = std::is_base_of<ITraceable, T>::value;

template <TraceableT T>
struct DataHolder : T
{
    std::vector<StackTrace> stacktraces; // TODO: make unordered
};

// TODO: support cast functions
// TODO: support shared_from_this
template <TraceableT T>
struct TraceableSharedPtr : std::shared_ptr<DataHolder<T>>
{
    using Base = std::shared_ptr<DataHolder<T>>;

    template <typename... Ts>
    TraceableSharedPtr(Ts && ...args)
        : Base(std::forward<Ts>(args)...)
    {}

    TraceableSharedPtr(const TraceableSharedPtr & other) = default;
    TraceableSharedPtr(TraceableSharedPtr && other)  noexcept = default;

    TraceableSharedPtr & operator=(const TraceableSharedPtr & rhs)
    {
        updateStackTraces();
        Base::operator=(rhs);
        return *this;
    }

    TraceableSharedPtr & operator=(TraceableSharedPtr && rhs) noexcept
    {
        updateStackTraces();
        Base::operator=(std::move(rhs));
        return *this;
    }

    std::string dump() const
    {
        return stack.toString();
    }
private:

    std::vector<StackTrace> & getStackTraces()
    {
        return (Base::operator->())->stacktraces;
    }

    void updateStackTraces()
    {
        auto & owners = getStackTraces();
        auto it = std::find(owners.begin(), owners.end(), stack);

        if (it != owners.end())
        {
            std::swap(*it, owners.back());
            owners.pop_back();
        }

        stack = StackTrace();
        owners.push_back(stack);
    }

    StackTrace stack;
};

namespace std // NOLINT
{
    template <TraceableT T, typename... Ts>
    TraceableSharedPtr<T> make_shared(Ts && ...args)
    {
        return TraceableSharedPtr<T>(std::forward<Ts>(args)...);
    }
}

struct DataPart {};
struct TraceableDataPart : ITraceable
{
    std::vector<int> a;
};

void g(TraceableSharedPtr<TraceableDataPart> part)
{
    std::cout << part.dump() << part->a[0];
}

int test()
{
    auto part = std::make_shared<DataPart>();
    auto trace_part = std::make_shared<TraceableDataPart>();
    auto copy = trace_part;
    g(trace_part);
    trace_part = copy;
    trace_part->a.push_back(42);
    trace_part = std::make_shared<TraceableDataPart>(copy);
    std::cout << trace_part.dump();
}
