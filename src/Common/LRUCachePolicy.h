#pragma once

#include <Common/ICachePolicy.h>

#include <list>
#include <unordered_map>

namespace DB
{
/// Cache policy LRU evicts entries which are not used for a long time. Also see cache policy SLRU for reference.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size) of that value.
/// Cache starts to evict entries when their total weight exceeds max_size_in_bytes.
/// Value weight should not change after insertion.
/// To work with the thread-safe implementation of this class use a class "CacheBase" with first parameter "LRU"
/// and next parameters in the same order as in the constructor of the current class.
template <typename Key, typename Mapped, typename HashFunction = std::hash<Key>, typename WeightFunction = EqualWeightFunction<Mapped>>
class LRUCachePolicy : public ICachePolicy<Key, Mapped, HashFunction, WeightFunction>
{
public:
    using Base = ICachePolicy<Key, Mapped, HashFunction, WeightFunction>;
    using typename Base::MappedPtr;
    using typename Base::KeyMapped;
    using typename Base::OnWeightLossFunction;

    /** Initialize LRUCachePolicy with max_size_in_bytes and max_count.
     *  max_size_in_bytes == 0 means the cache accepts no entries.
      * max_count == 0 means no elements size restrictions.
      */
    LRUCachePolicy(size_t max_size_in_bytes_, size_t max_count_, OnWeightLossFunction on_weight_loss_function_)
        : Base(std::make_unique<NoCachePolicyUserQuota>())
        , max_size_in_bytes(max_size_in_bytes_)
        , max_count(max_count_)
        , on_weight_loss_function(on_weight_loss_function_)
    {
    }

    size_t sizeInBytes() const override
    {
        return current_size_in_bytes;
    }

    size_t count() const override
    {
        return cells.size();
    }

    size_t maxSizeInBytes() const override
    {
        return max_size_in_bytes;
    }

    void setMaxCount(size_t max_count_) override
    {
        max_count = max_count_;
        removeOverflow();
    }

    void setMaxSizeInBytes(size_t max_size_in_bytes_) override
    {
        max_size_in_bytes = max_size_in_bytes_;
        removeOverflow();
    }

    void clear() override
    {
        queue.clear();
        cells.clear();
        current_size_in_bytes = 0;
    }

    void remove(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size_in_bytes -= cell.size;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    void remove(std::function<bool(const Key &, const MappedPtr &)> predicate) override
    {
        for (auto it = cells.begin(); it != cells.end();)
        {
            if (predicate(it->first, it->second.value))
            {
                Cell & cell = it->second;
                current_size_in_bytes -= cell.size;
                queue.erase(cell.queue_iterator);
                it = cells.erase(it);
            }
            else
                ++it;
        }
    }

    MappedPtr get(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return {};

        Cell & cell = it->second;

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return cell.value;
    }

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return std::nullopt;

        Cell & cell = it->second;

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return std::make_optional<KeyMapped>({it->first, cell.value});
    }

    void set(const Key & key, const MappedPtr & mapped) override
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
            current_size_in_bytes -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size_in_bytes += cell.size;

        removeOverflow();
    }

    std::vector<KeyMapped> dump() const override
    {
        std::vector<KeyMapped> res;
        for (const auto & [key, cell] : cells)
            res.push_back({key, cell.value});
        return res;
    }

private:
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
    size_t current_size_in_bytes = 0;
    size_t max_size_in_bytes;
    size_t max_count;

    WeightFunction weight_function;
    OnWeightLossFunction on_weight_loss_function;

    void removeOverflow()
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();

        while ((current_size_in_bytes > max_size_in_bytes || (max_count != 0 && queue_size > max_count)) && (queue_size > 0))
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
                std::terminate(); // Queue became inconsistent

            const auto & cell = it->second;

            current_size_in_bytes -= cell.size;
            current_weight_lost += cell.size;

            cells.erase(it);
            queue.pop_front();
            --queue_size;
        }

        on_weight_loss_function(current_weight_lost);

        if (current_size_in_bytes > (1ull << 63))
            std::terminate(); // Queue became inconsistent
    }
};

}
