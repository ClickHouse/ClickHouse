#pragma once

#include <boost/noncopyable.hpp>

#include <Common/HyperLogLogCounter.h>
#include <Common/HashTable/SmallTable.h>
#include <Common/MemoryTracker.h>


namespace DB
{


/** For a small number of keys - an array of fixed size "on the stack".
  * For large, HyperLogLog is allocated.
  * See also the more practical implementation in CombinedCardinalityEstimator.h,
  *  where a hash table is also used for medium-sized sets.
  */
template
<
    typename Key,
    UInt8 small_set_size,
    UInt8 K,
    typename Hash = IntHash32<Key>,
    typename DenominatorType = double>
class HyperLogLogWithSmallSetOptimization : private boost::noncopyable
{
private:
    using Small = SmallSet<Key, small_set_size>;
    using Large = HyperLogLogCounter<K, Hash, UInt32, DenominatorType>;

    Small small;
    Large * large = nullptr;

    bool isLarge() const
    {
        return large != nullptr;
    }

    void toLarge()
    {
        CurrentMemoryTracker::alloc(sizeof(Large));

        /// At the time of copying data from `tiny`, setting the value of `large` is still not possible (otherwise it will overwrite some data).
        Large * tmp_large = new Large;

        for (const auto & x : small)
            tmp_large->insert(x);

        large = tmp_large;
    }

public:
    ~HyperLogLogWithSmallSetOptimization()
    {
        if (isLarge())
        {
            delete large;

            CurrentMemoryTracker::free(sizeof(Large));
        }
    }

    void insert(Key value)
    {
        if (!isLarge())
        {
            if (small.find(value) == small.end())
            {
                if (!small.full())
                    small.insert(value);
                else
                {
                    toLarge();
                    large->insert(value);
                }
            }
        }
        else
            large->insert(value);
    }

    UInt64 size() const
    {
        return !isLarge() ? small.size() : large->size();
    }

    void merge(const HyperLogLogWithSmallSetOptimization & rhs)
    {
        if (rhs.isLarge())
        {
            if (!isLarge())
                toLarge();

            large->merge(*rhs.large);
        }
        else
        {
            for (const auto & x : rhs.small)
                insert(x);
        }
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        bool is_large;
        readBinary(is_large, in);

        if (is_large)
        {
            toLarge();
            large->read(in);
        }
        else
            small.read(in);
    }

    void readAndMerge(DB::ReadBuffer & in)
    {
        bool is_rhs_large;
        readBinary(is_rhs_large, in);

        if (!isLarge() && is_rhs_large)
            toLarge();

        if (!is_rhs_large)
        {
            typename Small::Reader reader(in);
            while (reader.next())
                insert(reader.get());
        }
        else
            large->readAndMerge(in);
    }

    void write(DB::WriteBuffer & out) const
    {
        writeBinary(isLarge(), out);

        if (isLarge())
            large->write(out);
        else
            small.write(out);
    }
};


}
