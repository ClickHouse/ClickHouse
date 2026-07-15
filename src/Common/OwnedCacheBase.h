#pragma once

#include <Common/CacheBase.h>

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

template <typename Key, typename Mapped>
struct OwnedCacheCell
{
    Key key;
    std::shared_ptr<Mapped> value;
    std::optional<UUID> owner;
};

template <typename WeightFunction>
struct OwnedCacheCellWeight
{
    template <typename Cell>
    size_t operator()(const Cell & cell) const
    {
        return cell.value ? WeightFunction{}(*cell.value) : 0;
    }
};

template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = EqualWeightFunction<TMapped>>
class OwnedCacheBase
    : private CacheBase<TKey, OwnedCacheCell<TKey, TMapped>, HashFunction, OwnedCacheCellWeight<WeightFunction>>
{
private:
    using Cell = OwnedCacheCell<TKey, TMapped>;
    using Base = CacheBase<TKey, Cell, HashFunction, OwnedCacheCellWeight<WeightFunction>>;
    using CellPtr = typename Base::MappedPtr;

public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    struct KeyMapped
    {
        Key key;
        MappedPtr mapped;
    };

    struct OwnerQuota
    {
        std::optional<UUID> owner;
        size_t max_size_in_bytes = 0;
    };

    static constexpr auto NO_MAX_COUNT = Base::NO_MAX_COUNT;
    static constexpr auto DEFAULT_SIZE_RATIO = Base::DEFAULT_SIZE_RATIO;

    explicit OwnedCacheBase(
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        size_t max_count = NO_MAX_COUNT,
        double size_ratio = DEFAULT_SIZE_RATIO)
        : Base(size_in_bytes_metric, count_metric, max_size_in_bytes, max_count, size_ratio)
    {
    }

    explicit OwnedCacheBase(
        std::string_view cache_policy_name,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        size_t max_count,
        double size_ratio)
        : Base(cache_policy_name, size_in_bytes_metric, count_metric, max_size_in_bytes, max_count, size_ratio)
    {
    }

    MappedPtr get(const Key & key)
    {
        auto cell = Base::get(key);
        return cell ? cell->value : nullptr;
    }

    bool contains(const Key & key) const
    {
        return Base::contains(key);
    }

    void setOwned(const Key & key, MappedPtr mapped, OwnerQuota owner_quota, bool zero_max_count_is_disabled = false)
    {
        std::lock_guard operation_lock(operation_mutex);

        const size_t entry_weight = mapped ? WeightFunction{}(*mapped) : 0;
        if (Base::maxSizeInBytes() == 0 || (zero_max_count_is_disabled && Base::maxCount() == 0))
            return;

        if (entry_weight > Base::maxSizeInBytes())
            return;

        {
            std::lock_guard lock(owner_mutex);
            if (!canFitOwnerQuotaLocked(key, owner_quota, entry_weight))
                return;
        }

        auto cell = std::make_shared<Cell>(Cell{key, std::move(mapped), owner_quota.owner});
        Base::set(key, cell);

        std::lock_guard lock(owner_mutex);
        removeAccountingLocked(key);
        addAccountingLocked(key, owner_quota.owner, entry_weight);
    }

    bool removeIfMatches(const Key & key, const MappedPtr & expected)
    {
        std::lock_guard operation_lock(operation_mutex);

        CellPtr removed_cell;
        const bool removed = Base::removeIfMatches(key, [&](const CellPtr & cell)
        {
            if (!cell || cell->value.get() != expected.get())
                return false;

            removed_cell = cell;
            return true;
        });

        if (removed && removed_cell)
        {
            std::lock_guard lock(owner_mutex);
            removeAccountingLocked(removed_cell->key);
        }

        return removed;
    }

    void clear()
    {
        std::lock_guard operation_lock(operation_mutex);
        Base::clear();

        std::lock_guard lock(owner_mutex);
        bytes_by_owner.clear();
        weight_by_key.clear();
    }

    void updateConfiguration(size_t max_size_in_bytes, size_t max_count)
    {
        std::lock_guard operation_lock(operation_mutex);
        Base::setMaxSizeInBytes(max_size_in_bytes);
        Base::setMaxCount(max_count);
    }

    size_t sizeInBytes() const
    {
        return Base::sizeInBytes();
    }

    size_t count() const
    {
        return Base::count();
    }

    size_t maxSizeInBytes() const
    {
        return Base::maxSizeInBytes();
    }

    size_t maxCount() const
    {
        return Base::maxCount();
    }

    std::vector<KeyMapped> dump() const
    {
        auto cells = Base::dump();
        std::vector<KeyMapped> result;
        result.reserve(cells.size());

        for (const auto & cell : cells)
        {
            if (cell.mapped)
                result.push_back({cell.key, cell.mapped->value});
        }

        return result;
    }

protected:
    void onEntryRemoval(size_t /*weight_loss*/, const CellPtr & cell) override
    {
        if (!cell)
            return;

        std::lock_guard lock(owner_mutex);
        removeAccountingLocked(cell->key);
    }

private:
    struct OwnerWeight
    {
        std::optional<UUID> owner;
        size_t weight = 0;
    };

    bool canFitOwnerQuotaLocked(const Key & key, const OwnerQuota & owner_quota, size_t entry_weight) const TSA_REQUIRES(owner_mutex)
    {
        if (!owner_quota.owner.has_value() || owner_quota.max_size_in_bytes == 0)
            return true;

        if (entry_weight > owner_quota.max_size_in_bytes)
            return false;

        size_t current_size = 0;
        if (auto owner_it = bytes_by_owner.find(*owner_quota.owner); owner_it != bytes_by_owner.end())
            current_size = owner_it->second;

        if (auto key_it = weight_by_key.find(key); key_it != weight_by_key.end() && key_it->second.owner == owner_quota.owner)
        {
            if (current_size <= key_it->second.weight)
                current_size = 0;
            else
                current_size -= key_it->second.weight;
        }

        return current_size + entry_weight <= owner_quota.max_size_in_bytes;
    }

    void addAccountingLocked(const Key & key, const std::optional<UUID> & owner, size_t weight) TSA_REQUIRES(owner_mutex)
    {
        weight_by_key[key] = {owner, weight};
        if (owner.has_value())
            bytes_by_owner[*owner] += weight;
    }

    void removeAccountingLocked(const Key & key) TSA_REQUIRES(owner_mutex)
    {
        auto key_it = weight_by_key.find(key);
        if (key_it == weight_by_key.end())
            return;

        if (key_it->second.owner.has_value())
        {
            auto owner_it = bytes_by_owner.find(*key_it->second.owner);
            if (owner_it != bytes_by_owner.end())
            {
                if (owner_it->second <= key_it->second.weight)
                    bytes_by_owner.erase(owner_it);
                else
                    owner_it->second -= key_it->second.weight;
            }
        }

        weight_by_key.erase(key_it);
    }

    mutable std::mutex operation_mutex;

    mutable std::mutex owner_mutex;
    std::unordered_map<UUID, size_t> bytes_by_owner TSA_GUARDED_BY(owner_mutex);
    std::unordered_map<Key, OwnerWeight, HashFunction> weight_by_key TSA_GUARDED_BY(owner_mutex);
};

}
