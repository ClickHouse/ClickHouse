/**
 * MIT License
 * 
 * Copyright (c) 2017 Tessil
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#ifndef TSL_HOPSCOTCH_MAP_H
#define TSL_HOPSCOTCH_MAP_H


#include <algorithm>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <list>
#include <memory>
#include <type_traits>
#include <utility>
#include "hopscotch_hash.h"


namespace tsl {

/**
 * Implementation of a hash map using the hopscotch hashing algorithm.
 * 
 * The Key and the value T must be either nothrow move-constructible, copy-constuctible or both.
 * 
 * The size of the neighborhood (NeighborhoodSize) must be > 0 and <= 62 if StoreHash is false.
 * When StoreHash is true, 32-bits of the hash will be stored alongside the neighborhood limiting
 * the NeighborhoodSize to <= 30. There is no memory usage difference between 
 * 'NeighborhoodSize 62; StoreHash false' and 'NeighborhoodSize 30; StoreHash true'.
 * 
 * Storing the hash may improve performance on insert during the rehash process if the hash takes time
 * to compute. It may also improve read performance if the KeyEqual function takes time (or incurs a cache-miss).
 * If used with simple Hash and KeyEqual it may slow things down.
 * 
 * StoreHash can only be set if the GrowthPolicy is set to tsl::power_of_two_growth_policy.
 * 
 * GrowthPolicy defines how the map grows and consequently how a hash value is mapped to a bucket. 
 * By default the map uses tsl::power_of_two_growth_policy. This policy keeps the number of buckets 
 * to a power of two and uses a mask to map the hash to a bucket instead of the slow modulo.
 * You may define your own growth policy, check tsl::power_of_two_growth_policy for the interface.
 * 
 * If the destructors of Key or T throw an exception, behaviour of the class is undefined.
 * 
 * Iterators invalidation:
 *  - clear, operator=, reserve, rehash: always invalidate the iterators.
 *  - insert, emplace, emplace_hint, operator[]: if there is an effective insert, invalidate the iterators 
 *    if a displacement is needed to resolve a collision (which mean that most of the time, 
 *    insert will invalidate the iterators). Or if there is a rehash.
 *  - erase: iterator on the erased element is the only one which become invalid.
 */
template<class Key, 
         class T, 
         class Hash = std::hash<Key>,
         class KeyEqual = std::equal_to<Key>,
         class Allocator = std::allocator<std::pair<Key, T>>,
         unsigned int NeighborhoodSize = 62,
         bool StoreHash = false,
         class GrowthPolicy = tsl::power_of_two_growth_policy>
class hopscotch_map {
private:    
    template<typename U>
    using has_is_transparent = tsl::detail_hopscotch_hash::has_is_transparent<U>;
    
    class KeySelect {
    public:
        using key_type = Key;
        
        const key_type& operator()(const std::pair<Key, T>& key_value) const {
            return key_value.first;
        }
        
        key_type& operator()(std::pair<Key, T>& key_value) {
            return key_value.first;
        }
    };  
    
    class ValueSelect {
    public:
        using value_type = T;
        
        const value_type& operator()(const std::pair<Key, T>& key_value) const {
            return key_value.second;
        }
        
        value_type& operator()(std::pair<Key, T>& key_value) {
            return key_value.second;
        }
    };
    
    
    using overflow_container_type = std::list<std::pair<Key, T>, Allocator>;
    using ht = detail_hopscotch_hash::hopscotch_hash<std::pair<Key, T>, KeySelect, ValueSelect,
                                                     Hash, KeyEqual, 
                                                     Allocator, NeighborhoodSize, 
                                                     StoreHash, GrowthPolicy,
                                                     overflow_container_type>;
    
public:
    using key_type = typename ht::key_type;
    using mapped_type = T;
    using value_type = typename ht::value_type;
    using size_type = typename ht::size_type;
    using difference_type = typename ht::difference_type;
    using hasher = typename ht::hasher;
    using key_equal = typename ht::key_equal;
    using allocator_type = typename ht::allocator_type;
    using reference = typename ht::reference;
    using const_reference = typename ht::const_reference;
    using pointer = typename ht::pointer;
    using const_pointer = typename ht::const_pointer;
    using iterator = typename ht::iterator;
    using const_iterator = typename ht::const_iterator;
    
    
    
    /*
     * Constructors
     */
    hopscotch_map() : hopscotch_map(ht::DEFAULT_INIT_BUCKETS_SIZE) {
    }
    
    explicit hopscotch_map(size_type bucket_count, 
                        const Hash& hash = Hash(),
                        const KeyEqual& equal = KeyEqual(),
                        const Allocator& alloc = Allocator()) : 
                        m_ht(bucket_count, hash, equal, alloc, ht::DEFAULT_MAX_LOAD_FACTOR)
    {
    }
    
    hopscotch_map(size_type bucket_count,
                  const Allocator& alloc) : hopscotch_map(bucket_count, Hash(), KeyEqual(), alloc)
    {
    }
    
    hopscotch_map(size_type bucket_count,
                  const Hash& hash,
                  const Allocator& alloc) : hopscotch_map(bucket_count, hash, KeyEqual(), alloc)
    {
    }
    
    explicit hopscotch_map(const Allocator& alloc) : hopscotch_map(ht::DEFAULT_INIT_BUCKETS_SIZE, alloc) {
    }
    
    template<class InputIt>
    hopscotch_map(InputIt first, InputIt last,
                size_type bucket_count = ht::DEFAULT_INIT_BUCKETS_SIZE,
                const Hash& hash = Hash(),
                const KeyEqual& equal = KeyEqual(),
                const Allocator& alloc = Allocator()) : hopscotch_map(bucket_count, hash, equal, alloc)
    {
        insert(first, last);
    }
    
    template<class InputIt>
    hopscotch_map(InputIt first, InputIt last,
                size_type bucket_count,
                const Allocator& alloc) : hopscotch_map(first, last, bucket_count, Hash(), KeyEqual(), alloc)
    {
    }
    
    template<class InputIt>
    hopscotch_map(InputIt first, InputIt last,
                size_type bucket_count,
                const Hash& hash,
                const Allocator& alloc) : hopscotch_map(first, last, bucket_count, hash, KeyEqual(), alloc)
    {
    }

    hopscotch_map(std::initializer_list<value_type> init,
                    size_type bucket_count = ht::DEFAULT_INIT_BUCKETS_SIZE,
                    const Hash& hash = Hash(),
                    const KeyEqual& equal = KeyEqual(),
                    const Allocator& alloc = Allocator()) : 
                    hopscotch_map(init.begin(), init.end(), bucket_count, hash, equal, alloc)
    {
    }

    hopscotch_map(std::initializer_list<value_type> init,
                    size_type bucket_count,
                    const Allocator& alloc) : 
                    hopscotch_map(init.begin(), init.end(), bucket_count, Hash(), KeyEqual(), alloc)
    {
    }

    hopscotch_map(std::initializer_list<value_type> init,
                    size_type bucket_count,
                    const Hash& hash,
                    const Allocator& alloc) : 
                    hopscotch_map(init.begin(), init.end(), bucket_count, hash, KeyEqual(), alloc)
    {
    }

    
    hopscotch_map& operator=(std::initializer_list<value_type> ilist) {
        m_ht.clear();
        
        m_ht.reserve(ilist.size());
        m_ht.insert(ilist.begin(), ilist.end());
        
        return *this;
    }
    
    allocator_type get_allocator() const { return m_ht.get_allocator(); }
    
    
    /*
     * Iterators
     */
    iterator begin() noexcept { return m_ht.begin(); }
    const_iterator begin() const noexcept { return m_ht.begin(); }
    const_iterator cbegin() const noexcept { return m_ht.cbegin(); }
    
    iterator end() noexcept { return m_ht.end(); }
    const_iterator end() const noexcept { return m_ht.end(); }
    const_iterator cend() const noexcept { return m_ht.cend(); }
    
    
    /*
     * Capacity
     */
    bool empty() const noexcept { return m_ht.empty(); }
    size_type size() const noexcept { return m_ht.size(); }
    size_type max_size() const noexcept { return m_ht.max_size(); }
    
    /*
     * Modifiers
     */
    void clear() noexcept { m_ht.clear(); }
    
    
    
    
    std::pair<iterator, bool> insert(const value_type& value) { 
        return m_ht.insert(value); 
    }
        
    template<class P, typename std::enable_if<std::is_constructible<value_type, P&&>::value>::type* = nullptr>
    std::pair<iterator, bool> insert(P&& value) { 
        return m_ht.insert(std::forward<P>(value)); 
    }
    
    std::pair<iterator, bool> insert(value_type&& value) { 
        return m_ht.insert(std::move(value)); 
    }
    
    
    iterator insert(const_iterator hint, const value_type& value) { 
        return m_ht.insert(hint, value); 
    }
        
    template<class P, typename std::enable_if<std::is_constructible<value_type, P&&>::value>::type* = nullptr>
    iterator insert(const_iterator hint, P&& value) { 
        return m_ht.insert(hint, std::forward<P>(value));
    }
    
    iterator insert(const_iterator hint, value_type&& value) { 
        return m_ht.insert(hint, std::move(value)); 
    }
    
    
    template<class InputIt>
    void insert(InputIt first, InputIt last) { 
        m_ht.insert(first, last); 
    }
    
    void insert(std::initializer_list<value_type> ilist) { 
        m_ht.insert(ilist.begin(), ilist.end()); 
    }

    
    
    
    template<class M>
    std::pair<iterator, bool> insert_or_assign(const key_type& k, M&& obj) { 
        return m_ht.insert_or_assign(k, std::forward<M>(obj)); 
    }

    template<class M>
    std::pair<iterator, bool> insert_or_assign(key_type&& k, M&& obj) { 
        return m_ht.insert_or_assign(std::move(k), std::forward<M>(obj)); 
    }
    
    template<class M>
    iterator insert_or_assign(const_iterator hint, const key_type& k, M&& obj) {
        return m_ht.insert_or_assign(hint, k, std::forward<M>(obj));
    }
    
    template<class M>
    iterator insert_or_assign(const_iterator hint, key_type&& k, M&& obj) {
        return m_ht.insert_or_assign(hint, std::move(k), std::forward<M>(obj));
    }
    
    
    
    
    /**
     * Due to the way elements are stored, emplace will need to move or copy the key-value once.
     * The method is equivalent to insert(value_type(std::forward<Args>(args)...));
     * 
     * Mainly here for compatibility with the std::unordered_map interface.
     */
    template<class... Args>
    std::pair<iterator, bool> emplace(Args&&... args) { 
        return m_ht.emplace(std::forward<Args>(args)...); 
    }
    
    
    
    
    /**
     * Due to the way elements are stored, emplace_hint will need to move or copy the key-value once.
     * The method is equivalent to insert(hint, value_type(std::forward<Args>(args)...));
     * 
     * Mainly here for compatibility with the std::unordered_map interface.
     */
    template<class... Args>
    iterator emplace_hint(const_iterator hint, Args&&... args) {
        return m_ht.emplace_hint(hint, std::forward<Args>(args)...);
    }
    
    
    
    
    template<class... Args>
    std::pair<iterator, bool> try_emplace(const key_type& k, Args&&... args) { 
        return m_ht.try_emplace(k, std::forward<Args>(args)...);
    }
    
    template<class... Args>
    std::pair<iterator, bool> try_emplace(key_type&& k, Args&&... args) {
        return m_ht.try_emplace(std::move(k), std::forward<Args>(args)...);
    }
    
    template<class... Args>
    iterator try_emplace(const_iterator hint, const key_type& k, Args&&... args) {
        return m_ht.try_emplace(hint, k, std::forward<Args>(args)...);
    }
    
    template<class... Args>
    iterator try_emplace(const_iterator hint, key_type&& k, Args&&... args) {
        return m_ht.try_emplace(hint, std::move(k), std::forward<Args>(args)...);
    }
    
    

    
    iterator erase(iterator pos) { return m_ht.erase(pos); }
    iterator erase(const_iterator pos) { return m_ht.erase(pos); }
    iterator erase(const_iterator first, const_iterator last) { return m_ht.erase(first, last); }
    size_type erase(const key_type& key) { return m_ht.erase(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup to the value if you already have the hash.
     */    
    size_type erase(const key_type& key, std::size_t precalculated_hash) { 
        return m_ht.erase(key, precalculated_hash); 
    }
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type erase(const K& key) { return m_ht.erase(key); }
    
    /**
     * @copydoc erase(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup to the value if you already have the hash.
     */    
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type erase(const K& key, std::size_t precalculated_hash) { 
        return m_ht.erase(key, precalculated_hash); 
    }
    
    
    
    
    void swap(hopscotch_map& other) { other.m_ht.swap(m_ht); }
    
    /*
     * Lookup
     */
    T& at(const Key& key) { return m_ht.at(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    T& at(const Key& key, std::size_t precalculated_hash) { return m_ht.at(key, precalculated_hash); }
    
    
    const T& at(const Key& key) const { return m_ht.at(key); }
    
    /**
     * @copydoc at(const Key& key, std::size_t precalculated_hash)
     */
    const T& at(const Key& key, std::size_t precalculated_hash) const { return m_ht.at(key, precalculated_hash); }
    
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    T& at(const K& key) { return m_ht.at(key); }

    /**
     * @copydoc at(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */    
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    T& at(const K& key, std::size_t precalculated_hash) { return m_ht.at(key, precalculated_hash); }
    
    
    /**
     * @copydoc at(const K& key)
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    const T& at(const K& key) const { return m_ht.at(key); }
    
    /**
     * @copydoc at(const K& key, std::size_t precalculated_hash)
     */    
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    const T& at(const K& key, std::size_t precalculated_hash) const { return m_ht.at(key, precalculated_hash); }
    
    
    
    
    T& operator[](const Key& key) { return m_ht[key]; }    
    T& operator[](Key&& key) { return m_ht[std::move(key)]; }
    
    
    
    
    size_type count(const Key& key) const { return m_ht.count(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    size_type count(const Key& key, std::size_t precalculated_hash) const { 
        return m_ht.count(key, precalculated_hash); 
    }
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type count(const K& key) const { return m_ht.count(key); }
    
    /**
     * @copydoc count(const K& key) const
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */     
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type count(const K& key, std::size_t precalculated_hash) const { return m_ht.count(key, precalculated_hash); }
    
    
    
    
    iterator find(const Key& key) { return m_ht.find(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    iterator find(const Key& key, std::size_t precalculated_hash) { return m_ht.find(key, precalculated_hash); }
    
    const_iterator find(const Key& key) const { return m_ht.find(key); }
    
    /**
     * @copydoc find(const Key& key, std::size_t precalculated_hash)
     */
    const_iterator find(const Key& key, std::size_t precalculated_hash) const { 
        return m_ht.find(key, precalculated_hash);
    }
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    iterator find(const K& key) { return m_ht.find(key); }
    
    /**
     * @copydoc find(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    iterator find(const K& key, std::size_t precalculated_hash) { return m_ht.find(key, precalculated_hash); }
    
    /**
     * @copydoc find(const K& key)
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    const_iterator find(const K& key) const { return m_ht.find(key); }
    
    /**
     * @copydoc find(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    const_iterator find(const K& key, std::size_t precalculated_hash) const { 
        return m_ht.find(key, precalculated_hash); 
    }
    
    
    
    
    std::pair<iterator, iterator> equal_range(const Key& key) { return m_ht.equal_range(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    std::pair<iterator, iterator> equal_range(const Key& key, std::size_t precalculated_hash) { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    
    std::pair<const_iterator, const_iterator> equal_range(const Key& key) const { return m_ht.equal_range(key); }
    
    /**
     * @copydoc equal_range(const Key& key, std::size_t precalculated_hash)
     */
    std::pair<const_iterator, const_iterator> equal_range(const Key& key, std::size_t precalculated_hash) const { 
        return m_ht.equal_range(key, precalculated_hash); 
    }

    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    std::pair<iterator, iterator> equal_range(const K& key) { return m_ht.equal_range(key); }
    
    
    /**
     * @copydoc equal_range(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    std::pair<iterator, iterator> equal_range(const K& key, std::size_t precalculated_hash) { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    
    /**
     * @copydoc equal_range(const K& key)
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    std::pair<const_iterator, const_iterator> equal_range(const K& key) const { return m_ht.equal_range(key); }
    
    /**
     * @copydoc equal_range(const K& key, std::size_t precalculated_hash)
     */    
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    std::pair<const_iterator, const_iterator> equal_range(const K& key, std::size_t precalculated_hash) const { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    
    
    
    
    /*
     * Bucket interface 
     */
    size_type bucket_count() const { return m_ht.bucket_count(); }
    size_type max_bucket_count() const { return m_ht.max_bucket_count(); }
    
    
    /*
     *  Hash policy 
     */
    float load_factor() const { return m_ht.load_factor(); }
    float max_load_factor() const { return m_ht.max_load_factor(); }
    void max_load_factor(float ml) { m_ht.max_load_factor(ml); }
    
    void rehash(size_type count) { m_ht.rehash(count); }
    void reserve(size_type count) { m_ht.reserve(count); }
    
    
    /*
     * Observers
     */
    hasher hash_function() const { return m_ht.hash_function(); }
    key_equal key_eq() const { return m_ht.key_eq(); }
    
    /*
     * Other
     */
    
    /**
     * Convert a const_iterator to an iterator.
     */
    iterator mutable_iterator(const_iterator pos) {
        return m_ht.mutable_iterator(pos);
    }
    
    size_type overflow_size() const noexcept { return m_ht.overflow_size(); }
    
    friend bool operator==(const hopscotch_map& lhs, const hopscotch_map& rhs) {
        if(lhs.size() != rhs.size()) {
            return false;
        }
        
        for(const auto& element_lhs : lhs) {
            const auto it_element_rhs = rhs.find(element_lhs.first);
            if(it_element_rhs == rhs.cend() || element_lhs.second != it_element_rhs->second) {
                return false;
            }
        }
        
        return true;
    }

    friend bool operator!=(const hopscotch_map& lhs, const hopscotch_map& rhs) {
        return !operator==(lhs, rhs);
    }

    friend void swap(hopscotch_map& lhs, hopscotch_map& rhs) {
        lhs.swap(rhs);
    }


    
private:
    ht m_ht;
};

} // end namespace tsl

#endif
