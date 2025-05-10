#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/PODArray_fwd.h>
// #include <Functions/FunctionsHashing.h>
#include "base/types.h"
#include <sys/types.h>

#include <cmath>
#include <numbers>
#include <vector>

// #include <MurmurHash3.h>
#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

}

template <typename T, typename Hash, typename Allocator>
class DynamicBloomFilter : protected Hash
{
    inline static constexpr size_t MAX_HASH_FUNCTIONS = 16;
    inline static constexpr double MIN_TARGET_FPR = 0.00002;

    using HashValues = std::array<UInt64, MAX_HASH_FUNCTIONS>;

    static size_t maxElementsInBloomFilter(size_t count_bits, size_t count_hash_functions)
    {
        return static_cast<size_t>((static_cast<double>(count_bits) / count_hash_functions) * std::numbers::ln2);
    }

    class BloomFilterBitArray
    {
    public:
        BloomFilterBitArray(size_t bits_size, size_t count_hash_functions)
            : bit_array(bits_size)
            , max_elements(maxElementsInBloomFilter(bits_size, count_hash_functions))
            , count_hash_funcs(count_hash_functions)
        {
        }

        bool lookup(const HashValues& hash_values) const
        {
            for (size_t i = 0; i < count_hash_funcs; ++i)
            {
                if (!bit_array[hash_values[i] % bit_array.size()])  // Optimize modulo if bit_array.size() is a power of 2
                {
                    return false;
                }
            }
            return true;
        }

        void insert(const HashValues& hash_values)
        {
            if (count_elements >= max_elements)
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                    "Inserting element to full BloomFilterBitArray. count_elements = {}, max_elements = {}", count_elements, max_elements);
            }

            for (size_t i = 0; i < count_hash_funcs; ++i)
            {
                bit_array[hash_values[i] % bit_array.size()] = true;
            }
            ++count_elements;
        }

        size_t size() const
        {
            return count_elements;
        }

        bool isFull() const
        {
            return count_elements >= max_elements;
        }

        size_t bitArraySize() const
        {
            return bit_array.size();
        }

        size_t bitArrayCapacity() const
        {
            return bit_array.capacity();
        }

        void clear()
        {
            bit_array.assign(bit_array.size(), false);
            count_elements = 0;
        }

    private:
        // DB::PaddedPODArray<size_t, 4096, Allocator> bit_array;
        std::vector<bool> bit_array;
        size_t max_elements;
        size_t count_elements = 0;
        size_t count_hash_funcs;
    };

public:
    explicit DynamicBloomFilter(double targetFPR = 0.001, size_t min_bits_size_ = 65536) : min_bits_size(min_bits_size_)
    {
        LOG_INFO(getLogger("Igor"), "Creating DynamicBloomFilter");
        if (targetFPR < MIN_TARGET_FPR)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Too small FPR");
        }

        count_hash_funcs = static_cast<size_t>(std::ceil(std::log2(1 / targetFPR)));
        LOG_INFO(getLogger("Igor"), "Count of hash functions in DynamicBloomFilter: {}", count_hash_funcs);
        if (count_hash_funcs > MAX_HASH_FUNCTIONS)
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too small FPR");
        }

        bit_arrays.emplace_back(min_bits_size, count_hash_funcs);
        LOG_INFO(getLogger("Igor"), "DynamicBloomFilter is created");
    }

    size_t hash(const T & x) const { return Hash::operator()(x); }

    void insert(const T & elem)
    {
        auto hash_values = getHashValues(hash(elem));

        ++size;

        if (lookupImpl(hash_values))
        {
            return; // Already in filter. Maybe false positive
        }

        if (bit_arrays.back().isFull())
        {
            bit_arrays.emplace_back(bit_arrays.back().bitArraySize() * 2, count_hash_funcs);
        }

        bit_arrays.back().insert(hash_values);
    }

    bool lookup(const T & elem) const
    {
        auto hash_values = getHashValues(hash(elem));
        return lookupImpl(hash_values);
    }

    double bitsPerItem() const
    {
        size_t result = 0;
        for (const auto& bit_array : bit_arrays)
        {
            result += bit_array.bitArraySize();
        }
        return static_cast<double>(result) / size;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t result = 0;
        for (const auto& bit_array : bit_arrays)
        {
            result += bit_array.bitArraySize();
        }
        return result;
    }

    void clear()
    {
        bit_arrays.clear();
        bit_arrays.emplace_back(min_bits_size, count_hash_funcs);
        size = 0;
    }

private:
    HashValues getHashValues(size_t hash_elem) const
    {
        HashValues result;

        UInt64 params[2];
        // DB::MurmurHash3Impl128::apply(reinterpret_cast<const char*>(&hash_elem), sizeof(hash_elem), reinterpret_cast<char*>(params));
        const auto* ptr = reinterpret_cast<const char*>(&hash_elem);
        params[0] = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), 42);
        params[1] = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), 52);

        for (UInt64 i = 0; i < count_hash_funcs; ++i)
        {
            result[i] = params[0] + i * params[1];
        }
        return result;
    }

    bool lookupImpl(const HashValues & hash_values) const
    {
        for (const auto& bit_array : bit_arrays)
        {
            if (bit_array.lookup(hash_values))
            {
                return true;
            }
        }
        return false;
    }

    std::vector<BloomFilterBitArray> bit_arrays;  // Maybe use std::array to store BloomFilterBitArray inside the object
    size_t size = 0;
    size_t count_hash_funcs = 0;
    size_t min_bits_size;
};
