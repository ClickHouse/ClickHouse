#pragma once

#include <Common/ICachePolicy.h>

#include <list>
#include <unordered_map>

#include <Common/logger_useful.h>

namespace DB
{
/// Cache policy LRU evicts entries which are not used for a long time.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
/// To work with the thread-safe implementation of this class use a class "CacheBase" with first parameter "LRU"
/// and next parameters in the same order as in the constructor of the current class.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialWeightFunction<TMapped>>
class LRUCachePolicy : public ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    using Base = ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
    using typename Base::OnWeightLossFunction;

    /** Initialize LRUCachePolicy with max_size and max_elements_size.
      * max_elements_size == 0 means no elements size restrictions.
      */
    explicit LRUCachePolicy(size_t max_size_, size_t max_elements_size_ = 0)
        : max_size(std::max(static_cast<size_t>(1), max_size_)), max_elements_size(max_elements_size_)
    {
    }

    template <class... Args>
    LRUCachePolicy(OnWeightLossFunction on_weight_loss_function_, Args... args) : LRUCachePolicy(args...)
    {
        Base::on_weight_loss_function = on_weight_loss_function_;
    }

    size_t weight([[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) const override
    {
        return current_size;
    }

    size_t count([[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) const override
    {
        return cells.size();
    }

    size_t maxSize([[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) const override
    {
        return max_size;
    }

    void reset([[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        queue.clear();
        cells.clear();
        current_size = 0;
    }

    void remove(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size -= cell.size;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    MappedPtr get(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            return MappedPtr();
        }

        Cell & cell = it->second;

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return cell.value;
    }

    void set(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto [it, inserted] = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        Cell & cell = it->second;

        if (inserted)
        {
            try
            {
                cell.queue_iterator = queue.insert(queue.end(), key);
            }
            catch (...)
            {
                cells.erase(it);
                throw;
            }
        }
        else
        {
            current_size -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;

        removeOverflow();
    }

protected:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    LRUQueue queue;

    struct Cell
    {
        MappedPtr value;
        size_t size;
        LRUQueueIterator queue_iterator;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    Cells cells;

    /// Total weight of values.
    size_t current_size = 0;
    const size_t max_size;
    const size_t max_elements_size;

    WeightFunction weight_function;

    void removeOverflow()
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();

        while ((current_size > max_size || (max_elements_size != 0 && queue_size > max_elements_size)) && (queue_size > 0))
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            const auto & cell = it->second;

            current_size -= cell.size;
            current_weight_lost += cell.size;

            cells.erase(it);
            queue.pop_front();
            --queue_size;
        }

        Base::on_weight_loss_function(current_weight_lost);

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }
};

}
