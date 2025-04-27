#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include "base/types.h"

#include <vector>

#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

template <typename T, typename Hash, typename Allocator>
class CuckooFilter : protected Hash
{
    class Buckets
    {
    public:
        static constexpr size_t kBucketSize = 4;

        std::vector<char> data;
        size_t slot_size = 0;

        Buckets() = default;
        Buckets(size_t size_of_filter_, size_t slot_size_) : data(size_of_filter_ / 8, 0), slot_size(slot_size_)
        {
            LOG_INFO(getLogger("Igor"), "slot_size: {}", slot_size);
        }

        inline size_t countBuckets() const
        {
            return data.size() / getBucketSize();
        }

        inline size_t getBucketSize() const
        {
            return slot_size * kBucketSize;
        }

        inline char* getBucket(size_t bucket_index)
        {
            return data.data() + getBucketSize() * bucket_index;
        }

        inline const char* getBucket(size_t bucket_index) const
        {
            return data.data() + getBucketSize() * bucket_index;
        }

        UInt64 getFingerprintFromBucket(const char* bucket, size_t index) const
        {
            UInt64 result = 0;
            memcpy(&result, bucket + index * slot_size, slot_size);
            return result;
        }

        bool lookupInBucket(size_t bucket_index, UInt64 fingerprint) const
        {
            bool result = false;
            const char* bucket = getBucket(bucket_index);
            for (size_t i = 0; i < kBucketSize; ++i)
            {
                UInt64 curr = getFingerprintFromBucket(bucket, i);
                result |= (fingerprint == curr);
            }
            return result;
        }

        void setFingerprint(char* bucket, size_t index, UInt64 fingerprint)
        {
            memcpy(bucket + index * slot_size, &fingerprint, slot_size);
        }

        bool tryInsert(size_t bucket_index, UInt64 fingerprint)
        {
            char* bucket = getBucket(bucket_index);
            for (size_t i = 0; i < kBucketSize; ++i)
            {
                UInt64 curr = getFingerprintFromBucket(bucket, i);
                if (curr == fingerprint)
                {
                    return true;
                }
                else if (curr == 0)
                {
                    setFingerprint(bucket, i, fingerprint);
                    return true;
                }
            }
            return false;
        }

        UInt64 insertOrEvict(size_t bucket_index, UInt64 fingerprint)
        {
            char* bucket = getBucket(bucket_index);
            for (size_t i = 0; i < kBucketSize; ++i)
            {
                UInt64 curr = getFingerprintFromBucket(bucket, i);
                if (curr == fingerprint)
                {
                    return 0;
                }
                else if (curr == 0)
                {
                    setFingerprint(bucket, i, fingerprint);
                    return 0;
                }
            }

            // Evicting
            UInt64 evicted_fingerprint = getFingerprintFromBucket(bucket, 0);
            setFingerprint(bucket, 0, fingerprint);
            return evicted_fingerprint;
        }
    };

    size_t getFingerprintSize(double targetFPR)
    {
        auto estimate = static_cast<size_t>(std::ceil(std::log2(1 / targetFPR))) + static_cast<size_t>(std::ceil(std::log2(2 * Buckets::kBucketSize)));
        return ((estimate - 1) / 8 + 1) * 8;    // To ensure that fingerprint is a whole number of bytes
    }

public:
    explicit CuckooFilter(double targetFPR = 0.001, size_t min_bits_size_ = 65536)
    {
        LOG_INFO(getLogger("Igor"), "Creating CuckooFilter");

        fingerprint_size = getFingerprintSize(targetFPR);
        LOG_INFO(getLogger("Igor"), "Fingerprint size: {}", fingerprint_size);
        if (fingerprint_size > 64)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Too small FPR");
        }

        size_of_filter = 1;
        while (size_of_filter < min_bits_size_)
        {
            size_of_filter <<= 1;
        }
        size_of_filter = std::max(size_of_filter, fingerprint_size * Buckets::kBucketSize);
        full_bound = static_cast<size_t>(static_cast<double>(size_of_filter) * kLoadFactor);
        LOG_INFO(getLogger("Igor"), "Size of Filter: {}, full bound: {}", size_of_filter, full_bound);

        buckets = Buckets(size_of_filter, fingerprint_size / 8);

        count_buckets = buckets.countBuckets(); // Should be power of two
        LOG_INFO(getLogger("Igor"), "count_buckets: {}", count_buckets);
    }

    size_t hash(const T & x) const { return Hash::operator()(x); }

    bool insert(const T & elem)
    {
        auto [hash_elem, fingerprint] = getHashAndFingerprint(hash(elem));

        if (fingerprint == victim)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Try to insert to filter after fail");
        }

        auto [place1, place2] = getBucketsForElement(hash_elem, fingerprint);

        if (lookupImpl(place1, place2, fingerprint))
        {
            return true;    // Already in filter. Maybe false positive
        }

        if (buckets.tryInsert(place1, fingerprint) || buckets.tryInsert(place2, fingerprint))
        {
            ++insertions_count;
            return true;
        }

        size_t start_bucket = place1;
        size_t current_bucket = start_bucket;

        while ((fingerprint = buckets.insertOrEvict(current_bucket, fingerprint)) != 0)
        {
            current_bucket = getSecondPlace(current_bucket, fingerprint);

            if (current_bucket == start_bucket)
            {
                // Cycle -> insertion failed
                victim = fingerprint;
                return false;
            }
        }
        return true;
    }

    bool lookup(const T & elem) const
    {
        auto [hash_elem, fingerprint] = getHashAndFingerprint(hash(elem));

        if (fingerprint == victim)
        {
            return true;
        }

        size_t place = hash_elem & (count_buckets - 1);

        if (buckets.lookupInBucket(place, fingerprint))
        {
            return true;
        }

        if (buckets.lookupInBucket(getSecondPlace(place, fingerprint), fingerprint))
        {
            return true;
        }

        return false;
    }

    size_t getBufferSizeInBytes() const
    {
        return buckets.data.size();
    }

    void clear()
    {
        buckets.data.assign(size_of_filter, 0);
        insertions_count = 0;
        victim = 0;
    }

    bool isFull() const
    {
        return insertions_count >= full_bound || victim != 0;
    }

private:
    std::pair<UInt64, UInt64> getHashAndFingerprint(size_t hash_elem) const
    {
        const auto* ptr = reinterpret_cast<const char*>(&hash_elem);

        auto hash = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), 42);
        auto fingerprint = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), 52);
        fingerprint &= (1 << fingerprint_size) - 1;

        if (fingerprint == 0)
        {
            fingerprint = 1;    // zero is sentinel
        }

        return {hash, fingerprint};
    }

    UInt64 getFingerprintHash(UInt64 fingerprint) const
    {
        return CityHash_v1_0_2::CityHash64WithSeed(reinterpret_cast<const char*>(&fingerprint), sizeof(size_t), 62);
    }

    std::pair<size_t, size_t> getBucketsForElement(size_t hash_elem, UInt64 fingerprint) const
    {
        size_t place1 = hash_elem & (count_buckets - 1);
        size_t place2 = getSecondPlace(place1, fingerprint);
        return {place1, place2};
    }

    size_t getSecondPlace(size_t place, UInt64 fingerprint) const
    {
        return place ^ (getFingerprintHash(fingerprint) & (count_buckets - 1));
    }

    bool lookupImpl(size_t place1, size_t place2, UInt64 fingerprint)
    {
        return buckets.lookupInBucket(place1, fingerprint) || buckets.lookupInBucket(place2, fingerprint);
    }

    static constexpr double kLoadFactor = 0.95;

    Buckets buckets;
    size_t insertions_count = 0;
    size_t fingerprint_size = 0;
    size_t size_of_filter = 0;
    size_t count_buckets = 0;

    UInt64 victim = 0;

    size_t full_bound = 0;  // if insertions_count >= full_bound -> full
};
