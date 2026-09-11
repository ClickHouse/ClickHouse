#pragma once

#include <Common/HashTable/SmallTable.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogCounter.h>
#include <Core/Defines.h>

#if defined(__FILC__)
#include <stdfil.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace details
{

enum class ContainerType : uint8_t
{
    SMALL = 1,
    MEDIUM = 2,
    LARGE = 3
};

static inline ContainerType max(const ContainerType & lhs, const ContainerType & rhs)
{
    uint8_t res = std::max(static_cast<uint8_t>(lhs), static_cast<uint8_t>(rhs));
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

    using value_type = Key;

private:
    using Small = SmallSet<Key, small_set_size_max>;
    using Medium = HashContainer;
    using Large = HyperLogLogCounter<K, Key, Hash, HashValueType, DenominatorType, BiasEstimator, mode>;

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

    /// Equivalent to calling insert for each value, but dispatches on the container type
    /// once per batch and runs a loop per container, which also lets the container's insert be inlined.
    void insertMany(const Key * values, size_t n)
    {
        size_t i = 0;
        while (i < n)
        {
            auto container_type = getContainerType();

            if (container_type == details::ContainerType::SMALL)
            {
                for (; i < n; ++i)
                {
                    if (small.find(values[i]) != small.end())
                        continue;

                    if (small.full())
                    {
                        toMedium();
                        break;
                    }

                    small.insert(values[i]);
                }
            }
            else if (container_type == details::ContainerType::MEDIUM)
            {
                auto & container = getContainer<Medium>();

                if (container.size() >= medium_set_size_max)
                {
                    toLarge();
                    continue;
                }

                size_t max_unique_values = std::min(n - i, medium_set_size_max - container.size());
                size_t batch_end = i + max_unique_values;

                /// Add up to max_unique_values without checking the size in the loop.
                for (; i < batch_end; ++i)
                {
                    container.insert(values[i]);
                }

                for (; i < n; ++i)
                {
                    if (container.size() >= medium_set_size_max)
                    {
                        toLarge();
                        break;
                    }

                    container.insert(values[i]);
                }
            }
            else if (container_type == details::ContainerType::LARGE)
            {
                auto & container = getContainer<Large>();
                for (; i < n; ++i)
                    container.insert(values[i]);
            }
        }
    }

    UInt64 size() const
    {
        auto container_type = getContainerType();

        if (container_type == details::ContainerType::SMALL)
            return small.size();
        if (container_type == details::ContainerType::MEDIUM)
            return getContainer<Medium>().size();
        if (container_type == details::ContainerType::LARGE)
            return getContainer<Large>().size();
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
                insert(x.getValue());
        }
        else if (rhs.getContainerType() == details::ContainerType::MEDIUM)
        {
            for (const auto & x : rhs.getContainer<Medium>())
                insert(x.getValue());
        }
        else if (rhs.getContainerType() == details::ContainerType::LARGE)
            getContainer<Large>().merge(rhs.getContainer<Large>());
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        UInt8 v = 0;
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
            tmp_medium->insert(x.getValue());

        setContainer(tmp_medium.release());
        setContainerType(details::ContainerType::MEDIUM);
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
                tmp_large->insert(x.getValue());
        }
        else if (container_type == details::ContainerType::MEDIUM)
        {
            for (const auto & x : getContainer<Medium>())
                tmp_large->insert(x.getValue());

            destroy();
        }

        setContainer(tmp_large.release());
        setContainerType(details::ContainerType::LARGE);
    }

    void NO_INLINE destroy()
    {
        auto container_type = getContainerType();

        clearContainerType();

        if (container_type == details::ContainerType::MEDIUM)
        {
            delete containerPtr<Medium>();
            setContainer<Medium>(nullptr);
        }
        else if (container_type == details::ContainerType::LARGE)
        {
            delete containerPtr<Large>();
            setContainer<Large>(nullptr);
        }
    }

    /// The container pointer with the type tag stripped from its low bits.
    template <typename T>
    T * containerPtr() const
    {
#if defined(__FILC__)
        return static_cast<T *>(zandptr(container, mask));
#else
        return reinterpret_cast<T *>(address & mask);
#endif
    }

    template <typename T>
    T & getContainer()
    {
        return *containerPtr<T>();
    }

    template <typename T>
    const T & getContainer() const
    {
        return *containerPtr<T>();
    }

    /// Store an untagged container pointer. The type tag is (re)applied by `setContainerType`.
    template <typename T>
    void setContainer(T * ptr)
    {
#if defined(__FILC__)
        container = ptr;
#else
        address = reinterpret_cast<UInt64>(ptr);
#endif
    }

    void setContainerType(details::ContainerType t)
    {
#if defined(__FILC__)
        /// `zorptr`/`zandptr` change the address bits while preserving the allocation capability,
        /// so the tagged pointer can still be dereferenced after the tag is stripped again.
        container = zorptr(zandptr(container, mask), static_cast<UInt8>(t));
#else
        address &= mask;
        address |= static_cast<UInt8>(t);
#endif
    }

    details::ContainerType getContainerType() const
    {
#if defined(__FILC__)
        return static_cast<details::ContainerType>(reinterpret_cast<UInt64>(container) & ~mask);
#else
        return static_cast<details::ContainerType>(address & ~mask);
#endif
    }

    void clearContainerType()
    {
#if defined(__FILC__)
        container = zandptr(container, mask);
#else
        address &= mask;
#endif
    }

    Small small;
#if defined(__FILC__)
    /// Under FilC a pointer carries an allocation capability that an integer cannot hold, so the
    /// tagged container pointer is kept as a real pointer instead of the integer-punning union.
    void * container = nullptr;
#else
    union
    {
        Medium * medium;
        Large * large;
        UInt64 address = 0;
    };
#endif
    static const UInt64 mask = 0xFFFFFFFFFFFFFFFC;
    static const UInt32 medium_set_size_max = 1ULL << medium_set_power2_max;
};

}
