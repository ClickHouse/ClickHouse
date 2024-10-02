#pragma once

#include <Common/ICachePolicy.h>

#include <list>
#include <unordered_map>

namespace DB
{

/* Cache policy SIEVE evicts entries which are not used for a long time.
 * WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size) of that value.
 * Cache starts to evict entries when their total weight exceeds max_size_in_bytes.
 * Value weight should not change after insertion.
 * To work with the thread-safe implementation of this class use a class "CacheBase" with first parameter "SIEVE"
 * and next parameters in the same order as in the constructor of the current class.
 * For more details, see https://junchengyang.com/publication/nsdi24-SIEVE.pdf
 */
template <typename Key, typename Mapped, typename HashFunction = std::hash<Key>, typename WeightFunction = EqualWeightFunction<Mapped>>
class SIEVECachePolicy : public ICachePolicy<Key, Mapped, HashFunction, WeightFunction>
{
public:
    using Base = ICachePolicy<Key, Mapped, HashFunction, WeightFunction>;
    using typename Base::MappedPtr;
    using typename Base::KeyMapped;
    using typename Base::OnWeightLossFunction;

    SIEVECachePolicy(
        size_t max_size_in_bytes_,
        size_t max_count_,
        OnWeightLossFunction on_weight_loss_function_)
        : Base(std::make_unique<NoCachePolicyUserQuota>())
        , max_size_in_bytes(max_size_in_bytes_)
        , max_count(max_count_)
        , on_weight_loss_function(on_weight_loss_function_)
    {
        hand = queue.begin();
    }

    size_t sizeInBytes() const override { return current_size_in_bytes; }

    size_t count() const override { return cells.size(); }

    size_t maxSizeInBytes() const override { return max_size_in_bytes; }

    void setMaxCount(size_t max_count_) override
    {
        max_count = max_count_;
        removeOverflow(false);
    }

    void setMaxSizeInBytes(size_t max_size_in_bytes_) override
    {
        max_size_in_bytes = max_size_in_bytes_;
        removeOverflow(false);
    }

    void clear() override
    {
        queue.clear();
        cells.clear();
        current_size_in_bytes = 0;
        hand = queue.begin();
    }

    void remove(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;

        auto & cell = it->second;
        current_size_in_bytes -= cell.size;

        if (hand == cell.queue_iterator)
            hand = std::next(cell.queue_iterator) == queue.end() ? queue.begin() : ++cell.queue_iterator;

        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    void remove(std::function<bool(const Key & key, const MappedPtr & mapped)> predicate) override
    {
        for (auto it = cells.begin(); it != cells.end();)
        {
            if (predicate(it->first, it->second.value))
            {
                auto & cell = it->second;
                current_size_in_bytes -= cell.size;

                if (hand == cell.queue_iterator)
                    hand = std::next(cell.queue_iterator) == queue.end() ? queue.begin() : ++cell.queue_iterator;

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
        cell.visited = true;

        return cell.value;
    }

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return std::nullopt;

        Cell & cell = it->second;
        cell.visited = true;

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
            cell.visited = true;
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size_in_bytes += cell.size;

        removeOverflow(true);
    }

    std::vector<KeyMapped> dump() const override
    {
        std::vector<KeyMapped> res;
        for (const auto & [key, cell] : cells)
            res.push_back({key, cell.value});
        return res;
    }

    std::vector<Key> dumpQueue() const
    {
        std::vector<Key> res;
        for (const auto & key : queue)
            res.push_back(key);
        return res;
    }

    /// For unit tests.
    std::optional<bool> isVisited(const Key & key) const
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return std::nullopt;
        const Cell & cell = it->second;
        return cell.visited;
    }

    std::optional<Key> getHand() const
    {
        if (hand == queue.end())
            return std::nullopt;
        return *hand;
    }

private:
    using SIEVEQueue = std::list<Key>;
    using SIEVEQueueIterator = typename SIEVEQueue::iterator;

    struct Cell
    {
        MappedPtr value;
        size_t size;
        bool visited = false;
        SIEVEQueueIterator queue_iterator;
    };
    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    SIEVEQueue queue;
    typename SIEVEQueue::iterator hand;
    Cells cells;

    /// Total weight of values.
    size_t current_size_in_bytes = 0;
    size_t max_size_in_bytes = 0;
    size_t max_count = 0;

    WeightFunction weight_function;
    OnWeightLossFunction on_weight_loss_function;

    void removeOverflow(bool ignore_last_element)
    {
        /// SIEVE algorithm:
        /// `hand` goes through the queue.
        /// If it sees a cell with visited = true, it sets visited = false for it and continues.
        /// If it sees a cell with visited = false, it evicts this cell.

        size_t current_weight_lost = 0;

        while (queue.size() > 0
               && (current_size_in_bytes > max_size_in_bytes || (max_count != 0 && queue.size() > max_count)))
        {
            if (hand == queue.end()
                || (ignore_last_element && queue.size() > 1 && hand == std::prev(queue.end())))
            {
                /// Reset hand to the start if we reach the end.
                hand = queue.begin();
            }

            const Key & key = *hand;
            auto it = cells.find(key);
            if (it == cells.end())
            {
                std::terminate(); // Queue became inconsistent
            }

            auto & cell = it->second;

            if (cell.visited == false)
            {
                current_size_in_bytes -= cell.size;
                current_weight_lost += cell.size;

                hand = queue.erase(hand);
                cells.erase(it);
            }
            else
            {
                cell.visited = false;
                ++hand;
            }
        }

        if (hand == queue.end())
            hand = queue.begin();

        on_weight_loss_function(current_weight_lost);

        if (current_size_in_bytes > (1ull << 63))
            std::terminate(); // Queue became inconsistent
    }
};

}
