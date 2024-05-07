#pragma once

#include <algorithm>
#include <memory>
#include <typeindex>
#include <vector>


namespace DB
{

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
    void swap(Self & other)
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
        for (const auto & rec: records)
            result.records.emplace_back(rec.type_idx, rec.ptr->clone());
        return result;
    }

    void append(Self && other)
    {
        std::move(other.records.begin(), other.records.end(), std::back_inserter(records));
        std::sort(records.begin(), records.end());
        chassert(isUniqTypes());
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

        chassert(it->type_idx != type_idx);

        records.emplace(it, type_idx, item);

        chassert(isUniqTypes());
    }

    Records::const_iterator getImpl(std::type_index type_idx) const
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
