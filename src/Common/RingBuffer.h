#pragma once

#include <vector>
#include <iostream>

namespace DB
{

/**
 * A ring buffer of fixed size.
 * With an ability to expand / narrow.
 * When narrowing only first N elements remain.
 */

template <class T>
class RingBuffer
{
public:
    explicit RingBuffer(size_t capacity_) : capacity(capacity_)
    {
        buffer.assign(capacity, {});
    }

    size_t size() const { return count; }

    bool empty() const { return count == 0; }

    bool tryPush(T element)
    {
        if (count == capacity) {
            return false;
        }
        buffer[advance(count)] = std::move(element);
        ++count;
        return true;
    }

    bool tryPop(T * element)
    {
        if (empty()) {
            return false;
        }
        *element = std::move(buffer[position]);
        --count;
        position = advance();
        return true;
    }

    /// In case of T = std::shared_ptr<Something> it won't cause any allocations
    template <typename Predicate>
    bool eraseAll(Predicate && predicate)
    {
        /// Shift all elements to the beginning of the buffer
        std::rotate(buffer.begin(), buffer.begin() + position, buffer.end());
        position = 0;

        /// Remove elements
        auto end_removed = std::remove_if(buffer.begin(), buffer.begin() + count, predicate);

        if (end_removed == buffer.begin() + count)
            return false;

        size_t new_count = std::distance(buffer.begin(), end_removed);
        for (size_t i = new_count; i < count; ++i)
            buffer[i] = T{};

        count = new_count;
        return true;
    }

    template <class Predicate>
    std::vector<T> getAll(Predicate && predicate)
    {
        std::vector<T> suitable;

        for (size_t i = 0; i < count; ++i)
        {
            auto item = buffer[advance(i)];
            if (predicate(item))
                suitable.emplace_back(item);
        }

        return suitable;
    }

    template <typename Predicate>
    bool has(Predicate && predicate)
    {
        for (size_t i = 0; i < count; ++i)
            if (predicate(buffer[advance(i)]))
                return true;

        return false;
    }


    void resize(size_t new_capacity)
    {
        if (new_capacity > capacity)
            expand(new_capacity);
        else if (new_capacity < capacity)
            narrow(new_capacity);
    }

private:

    size_t advance(size_t amount = 1)
    {
        if (position + amount >= capacity)
            return position + amount - capacity;
        return position + amount;
    }

    void expand(size_t new_capacity)
    {
        bool overflow = (position + count) > capacity;
        buffer.resize(new_capacity);

        if (overflow)
        {
            size_t count_before_end = capacity - position;
            for (size_t i = 0; i < count_before_end; ++i)
                buffer[new_capacity - i] = buffer[capacity - i];
            position = new_capacity - count_before_end;
        }

        capacity = new_capacity;
    }

    void narrow(size_t new_capacity)
    {
        std::vector<T> new_buffer(new_capacity);

        count = std::min(new_capacity, count);
        for (size_t i = 0; i < count; ++i)
            new_buffer[i] = buffer[advance(i)];

        std::swap(buffer, new_buffer);

        position = 0;
        capacity = new_capacity;
    }


    std::vector<T> buffer;
    size_t position{0};
    size_t count{0};
    size_t capacity{0};
};



}
