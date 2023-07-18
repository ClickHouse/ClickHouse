#pragma once

#include <vector>

namespace DB
{

template <typename Data>
class AtomicRoundRobin
{
public:
    explicit AtomicRoundRobin(std::vector<Data> elements_)
        : elements(std::move(elements_))
    {}

    Data get()
    {
        if (elements.empty())
        {
            return {};
        }

        /// Avoid atomic increment if number of proxies is 1.
        auto index = elements.size() > 1 ? (access_counter++) % elements.size() : 0;

        return elements[index];
    }

private:
    std::vector<Data> elements;
    std::atomic<size_t> access_counter;
};

}
