#pragma once

#include <Common/ICachePolicy.h>

#include <list>
#include <unordered_map>

#include <Common/logger_useful.h>

namespace DB
{

/// Cache policy SLRU evicts entries which were used only once and are not used for a long time,
/// this policy protects entries which were used more then once from a sequential scan.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
/// To work with the thread-safe implementation of this class use a class "CacheBase" with first parameter "SLRU"
/// and next parameters in the same order as in the constructor of the current class.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialWeightFunction<TMapped>>
class SLRUCachePolicy : public ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    using Base = ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
    using typename Base::OnWeightLossFunction;

    /** Initialize SLRUCachePolicy with max_size and max_protected_size.
      * max_protected_size shows how many of the most frequently used entries will not be evicted after a sequential scan.
      * max_protected_size == 0 means that the default protected size is equal to half of the total max size.
      */
    /// TODO: construct from special struct with cache policy parametrs (also with max_protected_size).
    SLRUCachePolicy(size_t max_size_, size_t max_elements_size_ = 0)
        : max_protected_size(max_size_ / 2)
        , max_size(std::max(max_protected_size + 1, max_size_))
        , max_elements_size(max_elements_size_)
        {}

    template <class... Args>
    SLRUCachePolicy(OnWeightLossFunction on_weight_loss_function_, Args... args) : SLRUCachePolicy(args...)
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

    size_t maxSize() const override
    {
        return max_size;
    }

    void reset([[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        cells.clear();
        probationary_queue.clear();
        protected_queue.clear();
        current_size = 0;
        current_protected_size = 0;
    }

    void remove(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size -= cell.size;
        if (cell.is_protected)
        {
            current_protected_size -= cell.size;
        }
        auto & queue = cell.is_protected ? protected_queue : probationary_queue;
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

        if (cell.is_protected)
        {
            protected_queue.splice(protected_queue.end(), protected_queue, cell.queue_iterator);
        }
        else
        {
            cell.is_protected = true;
            current_protected_size += cell.size;
            protected_queue.splice(protected_queue.end(), probationary_queue, cell.queue_iterator);
        }

        removeOverflow(protected_queue, max_protected_size, current_protected_size, /*is_protected=*/true);

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
                cell.queue_iterator = probationary_queue.insert(probationary_queue.end(), key);
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
            if (cell.is_protected)
            {
                current_protected_size -= cell.size;
                protected_queue.splice(protected_queue.end(), protected_queue, cell.queue_iterator);
            }
            else
            {
                cell.is_protected = true;
                protected_queue.splice(protected_queue.end(), probationary_queue, cell.queue_iterator);
            }
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;
        current_protected_size += cell.is_protected ? cell.size : 0;

        removeOverflow(protected_queue, max_protected_size, current_protected_size, /*is_protected=*/true);
        removeOverflow(probationary_queue, max_size, current_size, /*is_protected=*/false);
    }

protected:
    using SLRUQueue = std::list<Key>;
    using SLRUQueueIterator = typename SLRUQueue::iterator;

    SLRUQueue probationary_queue;
    SLRUQueue protected_queue;

    struct Cell
    {
        bool is_protected;
        MappedPtr value;
        size_t size;
        SLRUQueueIterator queue_iterator;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    Cells cells;

    size_t current_protected_size = 0;
    size_t current_size = 0;
    const size_t max_protected_size;
    const size_t max_size;
    const size_t max_elements_size;

    WeightFunction weight_function;

    void removeOverflow(SLRUQueue & queue, const size_t max_weight_size, size_t & current_weight_size, bool is_protected)
    {
        size_t current_weight_lost = 0;
        size_t queue_size = queue.size();

        auto need_remove = [&]() -> bool
        {
            if (is_protected)
            {
                return ((max_elements_size != 0 && cells.size() - probationary_queue.size() > max_elements_size - probationary_queue.size())
                        || (current_weight_size > max_weight_size))
                    && (queue_size > 0);
            }
            return ((max_elements_size != 0 && cells.size() > max_elements_size) || (current_weight_size > max_weight_size))
                && (queue_size > 0);
        };

        while (need_remove())
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("SLRUCache"), "SLRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            auto & cell = it->second;

            current_weight_size -= cell.size;

            if (cell.is_protected)
            {
                cell.is_protected = false;
                probationary_queue.splice(probationary_queue.end(), queue, cell.queue_iterator);
            }
            else
            {
                current_weight_lost += cell.size;
                cells.erase(it);
                queue.pop_front();
            }

            --queue_size;
        }

        if (!is_protected)
        {
            Base::on_weight_loss_function(current_weight_lost);
        }

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("SLRUCache"), "SLRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }
};

}
