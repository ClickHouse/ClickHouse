#pragma once

#include <queue>

namespace DB
{


template <class T, class Comparator = std::less<T>>
class PriorityQueue
{
public:

    T pop()
    {
        assert(!buffer.empty());
        std::pop_heap(buffer.begin(), buffer.end(), comparator);
        auto element = std::move(buffer.back());
        buffer.pop_back();
        return element;
    }

    void push(T element)
    {
        buffer.push_back(std::move(element));
        std::push_heap(buffer.begin(), buffer.end(), comparator);
    }

    template< class... Args >
    void emplace(Args &&... args)
    {
        buffer.emplace_back(std::forward<Args>(args)...);
        std::push_heap(buffer.begin(), buffer.end(), comparator);
    }

    bool empty() { return buffer.empty(); }
    size_t size() { return buffer.size(); }
    void reserve(size_t count) { buffer.reserve(count); }
    void resize(size_t count)
    {
        buffer.resize(count);
        std::make_heap(buffer.begin(), buffer.end(), comparator);
    }

private:

    Comparator comparator;
    std::vector<T> buffer;


};

}
