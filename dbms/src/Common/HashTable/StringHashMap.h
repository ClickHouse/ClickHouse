#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/StringHashTable.h>

template <typename Key, typename TMapped>
struct StringHashMapCell : public HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<Key, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
};

template <typename TMapped>
struct StringHashMapCell<StringKey16, TMapped> : public HashMapCell<StringKey16, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<StringKey16, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->value.first, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey16 & key, const HashTableNoState & /*state*/) { return key.low == 0; }
    void setZero() { this->value.first.low = 0; }
};

template <typename TMapped>
struct StringHashMapCell<StringKey24, TMapped> : public HashMapCell<StringKey24, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCell<StringKey24, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
    bool isZero(const HashTableNoState & state) const { return isZero(this->value.first, state); }
    // Assuming String does not contain zero bytes. NOTE: Cannot be used in serialized method
    static bool isZero(const StringKey24 & key, const HashTableNoState & /*state*/) { return key.a == 0; }
    void setZero() { this->value.first.a = 0; }
};

template <typename TMapped>
struct StringHashMapCell<StringRef, TMapped> : public HashMapCellWithSavedHash<StringRef, TMapped, StringHashTableHash, HashTableNoState>
{
    using Base = HashMapCellWithSavedHash<StringRef, TMapped, StringHashTableHash, HashTableNoState>;
    using Base::Base;
    static constexpr bool need_zero_value_storage = false;
};

template <typename TMapped, typename Allocator>
struct StringHashMapSubMaps
{
    using T0 = StringHashTableEmpty<StringHashMapCell<StringRef, TMapped>>;
    using T1 = HashMapTable<StringKey8, StringHashMapCell<StringKey8, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T2 = HashMapTable<StringKey16, StringHashMapCell<StringKey16, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using T3 = HashMapTable<StringKey24, StringHashMapCell<StringKey24, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
    using Ts = HashMapTable<StringRef, StringHashMapCell<StringRef, TMapped>, StringHashTableHash, StringHashTableGrower<>, Allocator>;
};

template <typename TMapped, typename Allocator = HashTableAllocator>
class StringHashMap : public StringHashTable<StringHashMapSubMaps<TMapped, Allocator>>
{
public:
    using Base = StringHashTable<StringHashMapSubMaps<TMapped, Allocator>>;
    using Self = StringHashMap;
    using Key = StringRef;
    using key_type = StringRef;
    using mapped_type = TMapped;
    using value_type = typename Base::Ts::value_type;

    using Base::Base;

    template <typename Func>
    void ALWAYS_INLINE mergeToEmplace(Self & that, Func && func)
    {
        if (this->m0.size() && that.m0.size())
            func(that.m0.value.getSecond(), this->m0.value.getSecond(), false);
        else if (this->m0.size())
            func(that.m0.value.getSecond(), this->m0.value.getSecond(), true);
        this->m1.mergeToEmplace(that.m1, func);
        this->m2.mergeToEmplace(that.m2, func);
        this->m3.mergeToEmplace(that.m3, func);
        this->ms.mergeToEmplace(that.ms, func);
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToFind(Self & that, Func && func)
    {
        if (this->m0.size() && that.m0.size())
            func(that.m0.value.getSecond(), this->m0.value.getSecond(), true);
        else if (this->m0.size())
            func(this->m0.value.getSecond(), this->m0.value.getSecond(), false);
        this->m1.mergeToFind(that.m1, func);
        this->m2.mergeToFind(that.m2, func);
        this->m3.mergeToFind(that.m3, func);
        this->ms.mergeToFind(that.ms, func);
    }

    struct ValueHolder
    {
        StringRef key;
        mapped_type * mapped;
        auto * operator-> () { return this; }
        mapped_type & getSecond() { return *mapped; }
        const mapped_type & getSecond() const { return *mapped; }
        value_type getValue() const { return {key, *mapped}; }
        ValueHolder() : key{}, mapped{} {}
        template <typename Value>
        ValueHolder(Value & value) : key(toStringRef(value.getFirst())), mapped(&value.getSecond())
        {
        }
        template <typename Value>
        void operator=(Value & value)
        {
            key = toStringRef(value.getFirst());
            mapped = &value.getSecond();
        }
        // Only used to check if it's end() in find
        bool operator==(const ValueHolder & that) const { return key.size == 0 && that.key.size == 0; }
        bool operator!=(const ValueHolder & that) const { return !(*this == that); }
    };

    struct MappedHolder
    {
        mapped_type * mapped;
        auto * operator-> () { return this; }
        mapped_type & getSecond() { return *mapped; }
        const mapped_type & getSecond() const { return *mapped; }
        MappedHolder() : mapped{} {}
        template <typename Value>
        MappedHolder(Value & value) : mapped(&value.getSecond())
        {
        }
        template <typename Value>
        void operator=(Value & value)
        {
            mapped = &value.getSecond();
        }
        // Only used to check if it's end() in find
        bool operator==(const MappedHolder & that) const { return mapped == that.mapped; }
        bool operator!=(const MappedHolder & that) const { return !(*this == that); }
    };

    template <typename Holder>
    struct EmplaceCallable
    {
        Holder & it;
        bool & inserted;
        EmplaceCallable(Holder & it_, bool & inserted_) : it(it_), inserted(inserted_) {}
        template <typename Map, typename Key>
        void ALWAYS_INLINE operator()(Map & map, const Key & x, size_t hash)
        {
            typename Map::iterator impl_it;
            map.emplace(x, impl_it, inserted, hash);
            it = *impl_it;
        }
    };

    template <typename Holder>
    struct EmplaceCallableWithPool
    {
        Holder & it;
        bool & inserted;
        DB::Arena & pool;
        EmplaceCallableWithPool(Holder & it_, bool & inserted_, DB::Arena & pool_) : it(it_), inserted(inserted_), pool(pool_) {}
        template <typename Map, typename Key>
        void ALWAYS_INLINE operator()(Map & map, const Key & x, size_t hash)
        {
            typename Map::iterator impl_it;
            map.emplace(x, impl_it, inserted, hash, pool);
            it = *impl_it;
        }
    };

    using EmplaceValueCallable = EmplaceCallable<ValueHolder>;
    using EmplaceMappedCallable = EmplaceCallable<MappedHolder>;
    using EmplaceValueCallableWithPool = EmplaceCallableWithPool<ValueHolder>;
    using EmplaceMappedCallableWithPool = EmplaceCallableWithPool<MappedHolder>;

    void ALWAYS_INLINE emplace(Key x, ValueHolder & it, bool & inserted) { this->dispatch(x, EmplaceValueCallable{it, inserted}); }
    void ALWAYS_INLINE emplace(Key x, ValueHolder & it, bool & inserted, DB::Arena & pool)
    {
        this->dispatch(x, EmplaceValueCallableWithPool{it, inserted, pool});
    }
    void ALWAYS_INLINE emplace(Key x, MappedHolder & it, bool & inserted) { this->dispatch(x, EmplaceMappedCallable{it, inserted}); }
    void ALWAYS_INLINE emplace(Key x, MappedHolder & it, bool & inserted, DB::Arena & pool)
    {
        this->dispatch(x, EmplaceMappedCallableWithPool{it, inserted, pool});
    }

    struct FindValueCallable
    {
        template <typename Map, typename Key>
        ValueHolder ALWAYS_INLINE operator()(Map & map, const Key & x, size_t hash)
        {
            typename Map::iterator it = map.find(x, hash);
            return it != map.end() ? ValueHolder(*it) : ValueHolder();
        }
    };

    ValueHolder ALWAYS_INLINE find(Key x) { return this->dispatch(x, FindValueCallable{}); }
    ValueHolder ALWAYS_INLINE end() { return ValueHolder{}; }

    using iterator = MappedHolder;

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        bool inserted;
        MappedHolder it;
        emplace(x, it, inserted);
        if (inserted)
            new (&it->getSecond()) mapped_type();
        return it->getSecond();
    }

    template <typename Func>
    void ALWAYS_INLINE forEachValue(Func && func)
    {
        ValueHolder value;
        if (this->m0.size())
            func(this->m0.value);
        for (auto & v : this->m1)
        {
            value = v;
            func(value);
        }
        for (auto & v : this->m2)
        {
            value = v;
            func(value);
        }
        for (auto & v : this->m3)
        {
            value = v;
            func(value);
        }
        for (auto & v : this->ms)
            func(v);
    }

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        if (this->m0.size())
            func(this->m0.value.getSecond());
        for (auto & v : this->m1)
            func(v.getSecond());
        for (auto & v : this->m2)
            func(v.getSecond());
        for (auto & v : this->m3)
            func(v.getSecond());
        for (auto & v : this->ms)
            func(v.getSecond());
    }
};
