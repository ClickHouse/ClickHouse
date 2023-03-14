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
/// Cache starts to evict entries when their total weight exceeds max_size_in_bytes.
/// Value weight should not change after insertion.
/// To work with the thread-safe implementation of this class use a class "CacheBase" with first parameter "SLRU"
/// and next parameters in the same order as in the constructor of the current class.
template <typename Key, typename Mapped, typename HashFunction = std::hash<Key>, typename WeightFunction = EqualWeightFunction<Mapped>>
class SLRUCachePolicy : public ICachePolicy<Key, Mapped, HashFunction, WeightFunction>
{
public:
    using Base = ICachePolicy<Key, Mapped, HashFunction, WeightFunction>;
    using typename Base::MappedPtr;
    using typename Base::KeyMapped;
    using typename Base::OnWeightLossFunction;

    /** Initialize SLRUCachePolicy with max_size_in_bytes and max_protected_size.
      * max_protected_size shows how many of the most frequently used entries will not be evicted after a sequential scan.
      * max_protected_size == 0 means that the default protected size is equal to half of the total max size.
      */
    /// TODO: construct from special struct with cache policy parameters (also with max_protected_size).
    SLRUCachePolicy(size_t max_size_in_bytes_, size_t max_count_, double size_ratio, OnWeightLossFunction on_weight_loss_function_)
        : max_protected_size(static_cast<size_t>(max_size_in_bytes_ * std::min(1.0, size_ratio)))
        , max_size_in_bytes(max_size_in_bytes_)
        , max_count(max_count_)
        , on_weight_loss_function(on_weight_loss_function_)
    {
    }

    size_t weight(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return current_size_in_bytes;
    }

    size_t count(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return cells.size();
    }

    size_t maxSize(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return max_size_in_bytes;
    }

    void reset(std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        cells.clear();
        probationary_queue.clear();
        protected_queue.clear();
        current_size_in_bytes = 0;
        current_protected_size = 0;
    }

    void remove(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size_in_bytes -= cell.size;
        if (cell.is_protected)
        {
            current_protected_size -= cell.size;
        }
        auto & queue = cell.is_protected ? protected_queue : probationary_queue;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    MappedPtr get(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return {};

        Cell & cell = it->second;

        if (cell.is_protected)
            protected_queue.splice(protected_queue.end(), protected_queue, cell.queue_iterator);
        else
        {
            cell.is_protected = true;
            current_protected_size += cell.size;
            protected_queue.splice(protected_queue.end(), probationary_queue, cell.queue_iterator);
            removeOverflow(protected_queue, max_protected_size, current_protected_size, /*is_protected=*/true);
        }

        return cell.value;
    }

    std::optional<KeyMapped> getWithKey(const Key & key, std::lock_guard<std::mutex> & /*cache_lock*/) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return std::nullopt;

        Cell & cell = it->second;

        if (cell.is_protected)
            protected_queue.splice(protected_queue.end(), protected_queue, cell.queue_iterator);
        else
        {
            cell.is_protected = true;
            current_protected_size += cell.size;
            protected_queue.splice(protected_queue.end(), probationary_queue, cell.queue_iterator);
            removeOverflow(protected_queue, max_protected_size, current_protected_size, /*is_protected=*/true);
        }

        return std::make_optional<KeyMapped>({it->first, cell.value});
    }

    void set(const Key & key, const MappedPtr & mapped, std::lock_guard<std::mutex> & /* cache_lock */) override
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
            current_size_in_bytes -= cell.size;
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
        current_size_in_bytes += cell.size;
        current_protected_size += cell.is_protected ? cell.size : 0;

        removeOverflow(protected_queue, max_protected_size, current_protected_size, /*is_protected=*/true);
        removeOverflow(probationary_queue, max_size_in_bytes, current_size_in_bytes, /*is_protected=*/false);
    }

    std::vector<KeyMapped> dump() const override
    {
        std::vector<KeyMapped> res;
        for (const auto & [key, cell] : cells)
            res.push_back({key, cell.value});
        return res;
    }

private:
    using SLRUQueue = std::list<Key>;
    using SLRUQueueIterator = typename SLRUQueue::iterator;

    SLRUQueue probationary_queue;
    SLRUQueue protected_queue;

    struct Cell
    {
        bool is_protected = false;
        MappedPtr value;
        size_t size;
        SLRUQueueIterator queue_iterator;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    Cells cells;

    size_t current_protected_size = 0;
    size_t current_size_in_bytes = 0;
    const size_t max_protected_size;
    const size_t max_size_in_bytes;
    const size_t max_count;

    WeightFunction weight_function;
    OnWeightLossFunction on_weight_loss_function;

    void removeOverflow(SLRUQueue & queue, const size_t max_weight_size, size_t & current_weight_size, bool is_protected)
    {
        size_t current_weight_lost = 0;
        size_t queue_size = queue.size();

        std::function<bool()> need_remove;
        if (is_protected)
        {
            /// Check if after remove all elements from probationary part there will be no more than max elements
            /// in protected queue and weight of all protected elements will be less then max protected weight.
            /// It's not possible to check only cells.size() > max_count
            /// because protected elements move to probationary part and still remain in cache.
            need_remove = [&]()
            {
                return ((max_count != 0 && cells.size() - probationary_queue.size() > max_count)
                || (current_weight_size > max_weight_size)) && (queue_size > 0);
            };
        }
        else
        {
            need_remove = [&]()
            {
                return ((max_count != 0 && cells.size() > max_count)
                || (current_weight_size > max_weight_size)) && (queue_size > 0);
            };
        }

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
            on_weight_loss_function(current_weight_lost);

        if (current_size_in_bytes > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("SLRUCache"), "SLRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }
};

}
