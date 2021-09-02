#pragma once

#include <vector>

namespace DB
{


/**
 * Class with O(n) complexity for all methods
 * Size has to be fixed.
 * The main reason to use this is to get rid of any allocations.
 * Used is some executors, where the number of elements is really small.
 */
template <class T>
class PlainMultiSet
{
public:

    explicit PlainMultiSet(size_t capacity_)
    {
        buffer.resize(capacity_);
    }


    bool tryPush(T element)
    {
        for (auto & item : buffer)
        {
            if (item.state == State::EMPTY)
            {
                item.state = State::FILLED;
                item.value = std::move(element);
                ++count;
                return true;
            }
        }


        return false;
    }

    bool has(T element)
    {
        for (auto & item : buffer)
            if (item.state == State::FILLED && item.value == element)
                return true;

        return false;
    }


    template <class Predicate>
    std::vector<T> getAll(Predicate && predicate)
    {
        std::vector<T> suitable;
        for (auto & item : buffer)
            if (item.state == State::FILLED && predicate(item.value))
                suitable.emplace_back(item.value);

        return suitable;
    }


    bool tryErase(const T & element)
    {
        for (auto & item : buffer)
        {
            if (item.state == State::FILLED && item.value == element)
            {
                item.state = State::EMPTY;
                item.value = T{};
                --count;
                return true;
            }
        }

        return false;
    }

    size_t size()
    {
        return count;
    }

    void reserve(size_t new_capacity)
    {
        if (buffer.size() >= new_capacity)
            return;

        std::vector<Item> new_buffer(std::move(buffer));
        new_buffer.reserve(new_capacity);

        std::swap(new_buffer, buffer);
    }

private:
    enum class State
    {
        EMPTY,
        FILLED
    };

    struct Item
    {
        T value;
        State state{State::EMPTY};
    };

    size_t count{0};
    std::vector<Item> buffer;
};

}
