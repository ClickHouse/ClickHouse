#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/BloomFilt.h>
#include <Common/HashTable/CuckoFilt.h>
#include <memory>


template <typename Key, typename Hash = DefaultHash<Key>, typename Allocator = HashTableAllocator>
class ProbHashSetBloomFilt : protected Hash,
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

    ProbHashSetBloomFilt(size_t size_of_filter_ = 1000, float precision_ = 0.01f) : bl_filt(size_of_filter_, precision_) {}


    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted) {
        const auto & key = keyHolderGetKey(key_holder);
        m_size++;
        bl_filt.insert(key);
        inserted = true;
        it = (&fl);
    }

    LookupResult ALWAYS_INLINE find(const Key & x) {
        if (bl_filt.lookup(x)) {
            return &fl;
        } else {
            return nullptr;
        }
    }

     size_t size() const {
        return m_size;
    }

    size_t getBufferSizeInBytes() const {
        return bl_filt.getBufferSizeInBytes();
    }

    void clear() {
        bl_filt.clear();
    }

};


template <typename Key, typename Hash = DefaultHash<Key>, typename Allocator = HashTableAllocator>
class ProbHashSetCuckooFilt : protected Hash,
                    protected Allocator
{
    using Conteiner = CuckooFilter<Key, Hash, Allocator>;
    using ConteinerPtr = std::shared_ptr<CuckooFilter<Key, Hash, Allocator>>;
private:
    bool fl = true;
    size_t m_size = 0;
    std::vector<ConteinerPtr> cu_filts;
    size_t size_of_filter;
    float  precision;

public:
    using key_type = Key;
    using value_type = Key;
    using LookupResult = bool *;

    size_t hash(const Key & x) const { return Hash::operator()(x); }

    ProbHashSetCuckooFilt(size_t size_of_filter_ = 1000, float precision_ = 0.01f) :  size_of_filter(size_of_filter_),  precision(precision_) {
        cu_filts.emplace_back(std::make_shared<Conteiner>(size_of_filter, 0, precision));
    }


    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted) {
        const auto & key = keyHolderGetKey(key_holder);
        while (!cu_filts.back()->insert(key)) {
            cu_filts.emplace_back(std::make_shared<Conteiner>(size_of_filter - m_size));
        }
        m_size++;
        inserted = true;
        it = (&fl);
    }

    LookupResult ALWAYS_INLINE find(const Key & x) {
        for(size_t i = 0; i < cu_filts.size(); i++) {
            if(cu_filts[i]->lookup(x))
                return &fl;
        }
        return nullptr;
    }

     size_t size() const {
        return m_size;
    }

    size_t getBufferSizeInBytes() const {
        size_t res = 0;
        for(size_t i = 0; i < cu_filts.size(); i++) {
            res += cu_filts[i]->getBufferSizeInBytes();
        }
        return res;
    }

    void clear() {
        cu_filts.clear();
        cu_filts.emplace_back(std::make_shared<Conteiner>(size_of_filter, 0, precision));
          
    }

};
