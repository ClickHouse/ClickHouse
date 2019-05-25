#pragma once

#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashTable.h>

template <typename TMapped, typename Allocator = HashTableAllocator, template <typename...> typename ImplTable = StringHashMap>
class TwoLevelStringHashMap : public TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>
{
public:
    using Key = StringRef;
    using key_type = Key;
    using Self = TwoLevelStringHashMap;
    using Base = TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, StringHashMap<TMapped, Allocator>>;
    using Base::Base;
    using mapped_type = TMapped;
    using value_type = typename Base::value_type;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            return this->impls[i].forEachMapped(func);
    }

    using RawImpl = StringHashMap<TMapped, Allocator>;
    using MappedHolder = typename RawImpl::MappedHolder;
    using ValueHolder = typename RawImpl::ValueHolder;
    using FindValueCallable = typename RawImpl::FindValueCallable;
    using EmplaceValueCallable = typename RawImpl::template EmplaceCallable<ValueHolder>;
    using EmplaceValueCallableWithPool = typename RawImpl::template EmplaceCallableWithPool<ValueHolder>;
    using EmplaceMappedCallable = typename RawImpl::template EmplaceCallable<MappedHolder>;
    using EmplaceMappedCallableWithPool = typename RawImpl::template EmplaceCallableWithPool<MappedHolder>;

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
};
