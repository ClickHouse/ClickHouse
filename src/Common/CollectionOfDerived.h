#pragma once

#include <base/defines.h>

#include <Common/Exception.h>

#include <algorithm>
#include <memory>
#include <typeindex>
#include <vector>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/* This is a collections of objects derived from ItemBase.
*  Collection contains no more than one instance for each derived type.
*  The derived type is used to access the instance.
*/

template<class ItemBase>
class CollectionOfDerivedItems
{
public:
    using Self = CollectionOfDerivedItems<ItemBase>;
    using ItemPtr = std::shared_ptr<ItemBase>;

private:
    struct Rec
    {
        std::type_index type_idx;
        ItemPtr ptr;

        bool operator<(const Rec & other) const
        {
            return type_idx < other.type_idx;
        }

        bool operator<(const std::type_index & value) const
        {
            return type_idx < value;
        }

        bool operator==(const Rec & other) const
        {
            return type_idx == other.type_idx;
        }
    };
    using Records = std::vector<Rec>;

public:
    void swap(Self & other) noexcept
    {
        records.swap(other.records);
    }

    void clear()
    {
        records.clear();
    }

    bool empty() const
    {
        return records.empty();
    }

    size_t size() const
    {
        return records.size();
    }

    Self clone() const
    {
        Self result;
        result.records.reserve(records.size());
        for (const auto & rec : records)
            result.records.emplace_back(rec.type_idx, rec.ptr->clone());
        return result;
    }

    // append items for other inscnace only if there is no such item in current instance
    void appendIfUniq(Self && other)
    {
        auto middle_idx = records.size();
        std::move(other.records.begin(), other.records.end(), std::back_inserter(records));
        // merge is stable
        std::inplace_merge(records.begin(), records.begin() + middle_idx, records.end());
        // remove duplicates
        records.erase(std::unique(records.begin(), records.end()), records.end());

        assert(std::is_sorted(records.begin(), records.end()));
        assert(isUniqTypes());
    }

    template <class T>
    void add(std::shared_ptr<T> info)
    {
        static_assert(std::is_base_of_v<ItemBase, T>, "Template parameter must inherit items base class");
        return addImpl(std::type_index(typeid(T)), std::move(info));
    }

    template <class T>
    std::shared_ptr<T> get() const
    {
        static_assert(std::is_base_of_v<ItemBase, T>, "Template parameter must inherit items base class");
        auto it = getImpl(std::type_index(typeid(T)));
        if (it == records.cend())
            return nullptr;
        auto cast = std::dynamic_pointer_cast<T>(it->ptr);
        chassert(cast);
        return cast;
    }

    template <class T>
    std::shared_ptr<T> extract()
    {
        static_assert(std::is_base_of_v<ItemBase, T>, "Template parameter must inherit items base class");
        auto it = getImpl(std::type_index(typeid(T)));
        if (it == records.cend())
            return nullptr;
        auto cast = std::dynamic_pointer_cast<T>(it->ptr);
        chassert(cast);

        records.erase(it);
        return cast;
    }

    std::string debug() const
    {
        std::string result;

        for (auto & rec : records)
        {
            result.append(rec.type_idx.name());
            result.append(" ");
        }

        return result;
    }

private:
    bool isUniqTypes() const
    {
        auto uniq_it = std::adjacent_find(records.begin(), records.end());
        return uniq_it == records.end();
    }

    void addImpl(std::type_index type_idx, ItemPtr item)
    {
        auto it = std::lower_bound(records.begin(), records.end(), type_idx);

        if (it == records.end())
        {
            records.emplace_back(type_idx, item);
            return;
        }

        if (it->type_idx == type_idx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "inserted items must be unique by their type, type {} is inserted twice", type_idx.name());


        records.emplace(it, type_idx, item);
    }

    typename Records::const_iterator getImpl(std::type_index type_idx) const
    {
        auto it = std::lower_bound(records.cbegin(), records.cend(), type_idx);

        if (it == records.cend())
            return records.cend();

        if (it->type_idx != type_idx)
            return records.cend();

        return it;
    }

    Records records;
};

}
