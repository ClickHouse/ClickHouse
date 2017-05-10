#pragma once

#include <Common/HashTable/SmallTable.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogCounter.h>
#include <Common/MemoryTracker.h>
#include <Core/Defines.h>


namespace DB
{

namespace details
{

enum class ContainerType : UInt8 { SMALL = 1, MEDIUM = 2, LARGE = 3 };

static inline ContainerType max(const ContainerType & lhs, const ContainerType & rhs)
{
    UInt8 res = std::max(static_cast<UInt8>(lhs), static_cast<UInt8>(rhs));
    return static_cast<ContainerType>(res);
}

}

/** For a small number of keys - an array of fixed size "on the stack".
  * For the average, HashSet is allocated.
  * For large, HyperLogLog is allocated.
  */
template
<
    typename Key,
    typename HashContainer,
    UInt8 small_set_size_max,
    UInt8 medium_set_power2_max,
    UInt8 K,
    typename Hash = IntHash32<Key>,
    typename HashValueType = UInt32,
    typename BiasEstimator = TrivialBiasEstimator,
    HyperLogLogMode mode = HyperLogLogMode::FullFeatured,
    typename DenominatorType = double
>
class CombinedCardinalityEstimator
{
public:
    using Self = CombinedCardinalityEstimator
        <
            Key,
            HashContainer,
            small_set_size_max,
            medium_set_power2_max,
            K,
            Hash,
            HashValueType,
            BiasEstimator,
            mode,
            DenominatorType
        >;

private:
    using Small = SmallSet<Key, small_set_size_max>;
    using Medium = HashContainer;
    using Large = HyperLogLogCounter<K, Hash, HashValueType, DenominatorType, BiasEstimator, mode>;

public:
    CombinedCardinalityEstimator()
    {
        setContainerType(details::ContainerType::SMALL);
    }

    ~CombinedCardinalityEstimator()
    {
        destroy();
    }

    void insert(Key value)
    {
        auto container_type = getContainerType();

        if (container_type == details::ContainerType::SMALL)
        {
            if (small.find(value) == small.end())
            {
                if (!small.full())
                    small.insert(value);
                else
                {
                    toMedium();
                    getContainer<Medium>().insert(value);
                }
            }
        }
        else if (container_type == details::ContainerType::MEDIUM)
        {
            auto & container = getContainer<Medium>();
            if (container.size() < medium_set_size_max)
                container.insert(value);
            else
            {
                toLarge();
                getContainer<Large>().insert(value);
            }
        }
        else if (container_type == details::ContainerType::LARGE)
            getContainer<Large>().insert(value);
    }

    UInt32 size() const
    {
        auto container_type = getContainerType();

        if (container_type == details::ContainerType::SMALL)
            return small.size();
        else if (container_type == details::ContainerType::MEDIUM)
            return getContainer<Medium>().size();
        else if (container_type == details::ContainerType::LARGE)
            return getContainer<Large>().size();
        else
            throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
    }

    void merge(const Self & rhs)
    {
        auto container_type = getContainerType();
        auto max_container_type = details::max(container_type, rhs.getContainerType());

        if (container_type != max_container_type)
        {
            if (max_container_type == details::ContainerType::MEDIUM)
                toMedium();
            else if (max_container_type == details::ContainerType::LARGE)
                toLarge();
        }

        if (rhs.getContainerType() == details::ContainerType::SMALL)
        {
            for (const auto & x : rhs.small)
                insert(x);
        }
        else if (rhs.getContainerType() == details::ContainerType::MEDIUM)
        {
            for (const auto & x : rhs.getContainer<Medium>())
                insert(x);
        }
        else if (rhs.getContainerType() == details::ContainerType::LARGE)
            getContainer<Large>().merge(rhs.getContainer<Large>());
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        UInt8 v;
        readBinary(v, in);
        auto container_type = static_cast<details::ContainerType>(v);

        if (container_type == details::ContainerType::SMALL)
            small.read(in);
        else if (container_type == details::ContainerType::MEDIUM)
        {
            toMedium();
            getContainer<Medium>().read(in);
        }
        else if (container_type == details::ContainerType::LARGE)
        {
            toLarge();
            getContainer<Large>().read(in);
        }
    }

    void readAndMerge(DB::ReadBuffer & in)
    {
        auto container_type = getContainerType();

        /// If readAndMerge is called with an empty state, just deserialize
        /// the state is specified as a parameter.
        if ((container_type == details::ContainerType::SMALL) && small.empty())
        {
            read(in);
            return;
        }

        UInt8 v;
        readBinary(v, in);
        auto rhs_container_type = static_cast<details::ContainerType>(v);

        auto max_container_type = details::max(container_type, rhs_container_type);

        if (container_type != max_container_type)
        {
            if (max_container_type == details::ContainerType::MEDIUM)
                toMedium();
            else if (max_container_type == details::ContainerType::LARGE)
                toLarge();
        }

        if (rhs_container_type == details::ContainerType::SMALL)
        {
            typename Small::Reader reader(in);
            while (reader.next())
                insert(reader.get());
        }
        else if (rhs_container_type == details::ContainerType::MEDIUM)
        {
            typename Medium::Reader reader(in);
            while (reader.next())
                insert(reader.get());
        }
        else if (rhs_container_type == details::ContainerType::LARGE)
            getContainer<Large>().readAndMerge(in);
    }

    void write(DB::WriteBuffer & out) const
    {
        auto container_type = getContainerType();
        writeBinary(static_cast<UInt8>(container_type), out);

        if (container_type == details::ContainerType::SMALL)
            small.write(out);
        else if (container_type == details::ContainerType::MEDIUM)
            getContainer<Medium>().write(out);
        else if (container_type == details::ContainerType::LARGE)
            getContainer<Large>().write(out);
    }

private:
    void toMedium()
    {
        if (getContainerType() != details::ContainerType::SMALL)
            throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);

        auto tmp_medium = std::make_unique<Medium>();

        for (const auto & x : small)
            tmp_medium->insert(x);

        medium = tmp_medium.release();
        setContainerType(details::ContainerType::MEDIUM);

        CurrentMemoryTracker::alloc(sizeof(medium));
    }

    void toLarge()
    {
        auto container_type = getContainerType();

        if ((container_type != details::ContainerType::SMALL) && (container_type != details::ContainerType::MEDIUM))
            throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);

        auto tmp_large = std::make_unique<Large>();

        if (container_type == details::ContainerType::SMALL)
        {
            for (const auto & x : small)
                tmp_large->insert(x);
        }
        else if (container_type == details::ContainerType::MEDIUM)
        {
            for (const auto & x : getContainer<Medium>())
                tmp_large->insert(x);

            destroy();
        }

        large = tmp_large.release();
        setContainerType(details::ContainerType::LARGE);

        CurrentMemoryTracker::alloc(sizeof(large));

    }

    void NO_INLINE destroy()
    {
        auto container_type = getContainerType();

        clearContainerType();

        if (container_type == details::ContainerType::MEDIUM)
        {
            delete medium;
            medium = nullptr;

            CurrentMemoryTracker::free(sizeof(medium));
        }
        else if (container_type == details::ContainerType::LARGE)
        {
            delete large;
            large = nullptr;

            CurrentMemoryTracker::free(sizeof(large));
        }
    }

    template<typename T>
    inline T & getContainer()
    {
        return *reinterpret_cast<T *>(address & mask);
    }

    template<typename T>
    inline const T & getContainer() const
    {
        return *reinterpret_cast<T *>(address & mask);
    }

    void setContainerType(details::ContainerType t)
    {
        address &= mask;
        address |= static_cast<UInt8>(t);
    }

    inline details::ContainerType getContainerType() const
    {
        return static_cast<details::ContainerType>(address & ~mask);
    }

    void clearContainerType()
    {
        address &= mask;
    }

private:
    Small small;
    union
    {
        Medium * medium;
        Large * large;
        UInt64 address = 0;
    };
    static const UInt64 mask = 0xFFFFFFFFFFFFFFFC;
    static const UInt32 medium_set_size_max = 1UL << medium_set_power2_max;
};

}
