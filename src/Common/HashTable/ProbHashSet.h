#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/BloomFilt.h>


template <typename Key, typename Hash = DefaultHash<Key>, typename Allocator = HashTableAllocator>
class ProbHashSet : protected Hash,
                    protected Allocator
{
private:
    bool fl = true;
    size_t m_size = 0;
    BloomFilt<Key, Hash, Allocator> bl_filt;

public:
    using key_type = Key;
    using value_type = Key;
    using LookupResult = bool *;

    size_t hash(const Key & x) const { return Hash::operator()(x); }

    ProbHashSet() : bl_filt(1000) {}

    ProbHashSet(size_t reserve_for_num_elements) : bl_filt(reserve_for_num_elements)
    { }



    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted) {
        const auto & key = keyHolderGetKey(key_holder);
        m_size++;
        bl_filt.insert(key);
        inserted = true;
        it = (&fl);
        //emplace(key_holder, it, inserted, hash(key));
    }

    //  template <typename KeyHolder>
    // void ALWAYS_INLINE emplace([[maybe_unused]] KeyHolder && key_holder, LookupResult & it,
    //                               bool & inserted, [[maybe_unused]]  size_t hash_value)
    // {
    //     m_size++;
    //     inserted = true;
    //     it = fl;

    // }

    LookupResult ALWAYS_INLINE find(const Key & x)
    {
        if (bl_filt.lookup(x)) {
            return &fl;
        } else {
            return nullptr;
        }
    }

     size_t size() const
    {
        return m_size;
    }

    size_t getBufferSizeInBytes() const
    {
        return bl_filt.getBufferSizeInBytes();
    }

};
