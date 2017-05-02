#pragma once

#include <iostream>
#include <list>
#include <vector>

#include <boost/range/adaptor/reversed.hpp>

#include <Common/UInt128.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

/*
 * Implementation of the Filtered Space-Saving for TopK streaming analysis.
 *   http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf
 * It implements suggested reduce-and-combine algorithm from Parallel Space Saving:
 *   https://arxiv.org/pdf/1401.0702.pdf
 */

namespace DB
{

template <typename TKey, typename Hash = DefaultHash<TKey>>
class SpaceSaving
{
public:
    struct Counter {
        Counter() {}

        Counter(const TKey & k, UInt64 c = 0, UInt64 e = 0)
          : key(k), slot(0), count(c), error(e) {}

        void write(DB::WriteBuffer & wb) const
        {
              DB::writeBinary(key, wb);
              DB::writeVarUInt(count, wb);
              DB::writeVarUInt(error, wb);
        }

        void read(DB::ReadBuffer & rb)
        {
            DB::readBinary(key, rb);
            DB::readVarUInt(count, rb);
            DB::readVarUInt(error, rb);
        }

        // greater() taking slot error into account
        bool operator >(const Counter &b) const
        {
            return (count > b.count) || (count == b.count && error < b.error);
        }

        TKey key;
        size_t slot;
        UInt64 count, error;
    };

    // Suggested constants in the paper "Finding top-k elements in data streams", chap 6. equation (24)
    SpaceSaving(size_t c = 10) : counterMap(), counterList(), alphaMap(6 * c), cap(c) {}
    ~SpaceSaving() { destroyElements(); }

    inline size_t size() const
    {
        return counterList.size();
    }

    inline size_t capacity() const
    {
        return cap;
    }

    void resize(size_t c)
    {
        counterList.reserve(c);
        alphaMap.resize(c * 6);
        cap = c;
    }

    Counter * insert(const TKey & key, UInt64 increment = 1, UInt64 error = 0)
    {
        // Increase weight of a key that already exists
        // It uses hashtable for both value mapping as a presence test (c_i != 0)
        auto hash = counterMap.hash(key);
        auto it = counterMap.find(key, hash);
        if (it != counterMap.end()) {
            auto c = it->second;
            c->count += increment;
            c->error += error;
            percolate(c);
            return c;
        }

        // Key doesn't exist, but can fit in the top K
        if (size() < capacity()) {
            auto c = new Counter(key, increment, error);
            push(c);
            return c;
        }

        auto min = counterList.back();
        auto & alpha = alphaMap[hash % alphaMap.size()];
        if (alpha + increment < min->count) {
            alpha += increment;
            return nullptr;
        }

        // Erase the current minimum element
        auto minHash = counterMap.hash(min->key);
        it = counterMap.find(min->key, minHash);
        if (it != counterMap.end()) {
            auto cell = it.getPtr();
            cell->setZero();
        }

        // Replace minimum with newly inserted element
        bool inserted = false;
        counterMap.emplace(key, it, inserted, hash);
        if (inserted) {
            alphaMap[minHash % alphaMap.size()] = min->count;
            min->key = key;
            min->count = alpha + increment;
            min->error = alpha + error;
            it->second = min;
            percolate(min);
        }

        return min;
    }

    /*
     * Parallel Space Saving reduction and combine step from:
     *  https://arxiv.org/pdf/1401.0702.pdf
     */
    void merge(const SpaceSaving<TKey, Hash> & rhs)
    {
        UInt64 m1 = 0, m2 = 0;
        if (size() == capacity()) {
            m1 = counterList.back()->count;
        }
        if (rhs.size() == rhs.capacity()) {
            m2 = rhs.counterList.back()->count;
        }

        /*
         * Updated algorithm to mutate current table in place
         * without mutating rhs table or creating new one
         * in the first step we expect that no elements overlap
         * and in the second sweep we correct the error if they do.
         */
        if (m2 > 0) {
            for (auto c : counterList) {
                c->count += m2;
                c->error += m2;
            }
        }

        // The list is sorted in descending order, we have to scan in reverse
        for (auto c : boost::adaptors::reverse(rhs.counterList)) {
            if (counterMap.find(c->key) != counterMap.end()) {
                // Subtract m2 previously added, guaranteed not negative
                insert(c->key, c->count - m2, c->error - m2);
            } else {
                // Counters not monitored in S1
                insert(c->key, c->count + m1, c->error + m1);
            }
        }
    }

    std::vector<Counter> topK(size_t k) const
    {
        std::vector<Counter> res;
        for (auto c : counterList) {
            res.push_back(*c);
            if (res.size() == k) {
                break;
            }
        }
        return res;
    }

    void write(DB::WriteBuffer & wb) const
    {
        DB::writeVarUInt(size(), wb);
        for (auto c : counterList) {
            c->write(wb);
        }
        for (auto a : alphaMap) {
            DB::writeVarUInt(a, wb);
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        destroyElements();
        size_t count = 0;
        DB::readVarUInt(count, rb);

        for (size_t i = 0; i < count; ++i) {
            auto c = new Counter();
            c->read(rb);
            push(c);
        }

        for (size_t i = 0; i < capacity() * 6; ++i) {
            UInt64 alpha = 0;
            DB::readVarUInt(alpha, rb);
            alphaMap.push_back(alpha);
        }
    }

protected:
    void push(Counter * c) {
        c->slot = counterList.size();
        counterList.push_back(c);
        counterMap[c->key] = c;
        percolate(c);
    }

    // This is equivallent to one step of bubble sort
    void percolate(Counter * c) {
        while (c->slot > 0) {
            auto next = counterList[c->slot - 1];
            if (*c > *next) {
                std::swap(next->slot, c->slot);
                std::swap(counterList[next->slot], counterList[c->slot]);
            } else {
                break;
            }
        }
    }

private:
    void destroyElements() {
        for (auto c : counterList) {
            delete c;
        }
        counterMap.clear();
        counterList.clear();
        alphaMap.clear();
    }

    HashMap<TKey, Counter *, Hash> counterMap;
    std::vector<Counter *> counterList;
    std::vector<UInt64> alphaMap;
    size_t cap;
};

};