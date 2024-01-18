#pragma once
#include <cctype>
#include <functional>
#include <type_traits>
#include <vector>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include "Arena.h"
#include "Exception.h"
#include "ThreadStatus.h"
#include "config.h"
#include "typeid_cast.h"

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}
namespace ColumnsHashing
{
class HashMethodContext;
}

struct HashValueIdCacheLine
{
    size_t allocated_num = 0;
    alignas(64) UInt64 values[8] = {-1UL, -1UL, -1UL, -1UL, -1UL, -1UL, -1UL, -1UL};
    alignas(64) UInt64 value_ids[8] = {-1UL, -1UL, -1UL, -1UL, -1UL, -1UL, -1UL, -1UL};
};

DECLARE_DEFAULT_CODE(
inline void concateValueIds(const UInt64 * __restrict local_value_ids, UInt64 * __restrict value_ids, UInt32 value_id_bits, size_t n)
{
    for (size_t j = 0; j < n; ++j)
        value_ids[j] = value_ids[j] << value_id_bits | local_value_ids[j];
}

template<typename T>
inline void extendValuesToUInt64(const UInt8 * __restrict values, UInt64 * __restrict ids, size_t n)
{
    if constexpr (sizeof(T) < 8)
    {
        for (size_t j = 0; j < n; ++j)
        {
            ids[j] = static_cast<UInt64>(*reinterpret_cast<const T *>(values)) & ((1UL << 8 * sizeof(T)) - 1);
            values += sizeof(T);
        }
    }
    else
    {
        memcpy(ids, values, n * sizeof(T));
    }
}

inline void shortValuesToUInt64(const UInt8 * __restrict values, size_t value_bytes, UInt64 * __restrict ids, size_t n)
{
    for (size_t i = 0; i < n; ++i)
    {
        auto * id_pos = reinterpret_cast<UInt8 *>(&ids[i]);
        for (size_t j = 0; j < value_bytes; ++j)
        {
            id_pos[j] = values[i * value_bytes + j];
        }
    }
}

inline void getValueIdsByRange(UInt64 * __restrict value_ids, UInt64 range_start, size_t n)
{
    for (size_t i = 0; i < n; ++i)
        value_ids[i] -= range_start;
}

inline void computeStringsLengthFromOffsets(const IColumn::Offsets & offsets, Int64 start, Int64 n, UInt64 * lens)
{
    for (Int64 i = 0; i < n; ++i)
        lens[i] = offsets[start + i] - offsets[start + i - 1];
}
)

DECLARE_AVX512BW_SPECIFIC_CODE(
inline void concateValueIds(const UInt64 * __restrict local_value_ids, UInt64 * __restrict value_ids, UInt32 value_id_bits, size_t n)
{
    size_t i = 0;
    auto tmp_value_ids = value_ids;
    auto tmp_local_value_ids = local_value_ids;
    for (; i + 8 < n; i += 8)
    {
        auto value_ids_v = _mm512_loadu_epi64(tmp_value_ids);
        auto multipy_result = _mm512_slli_epi64(value_ids_v, value_id_bits);
        auto local_value_ids_v = _mm512_loadu_epi64(tmp_local_value_ids);
        auto add_result = _mm512_or_epi64(multipy_result, local_value_ids_v);
        _mm512_storeu_epi64(tmp_value_ids, add_result);
        tmp_value_ids += 8;
        tmp_local_value_ids += 8;
    }

    for (; i < n; ++i)
        value_ids[i] = value_ids[i] << value_id_bits | local_value_ids[i];
}

inline void getValueIdsByRange(UInt64 * __restrict value_ids, UInt64 range_start, size_t n)
{
    size_t i = 0;
    auto tmp_value_ids = value_ids;
    for (; i + 8 < n; i += 8)
    {
        auto value_ids_v = _mm512_loadu_epi64(tmp_value_ids);
        auto range_start_v = _mm512_set1_epi64(range_start);
        auto new_value_ids_v = _mm512_sub_epi64(value_ids_v, range_start_v);
        _mm512_storeu_epi64(tmp_value_ids, new_value_ids_v);
        tmp_value_ids += 8;
    }
    for (; i < n; ++i)
        value_ids[i] -= range_start;
}

inline bool quickLookupValueId(HashValueIdCacheLine & cache,
    const StringRef & raw_value,
    UInt64 serialized_value,
    UInt64 & value_id,
    UInt64 & allocated_value_id,
    std::function<void (const StringRef &, UInt64)> add_new_value_id)
{
    if (cache.allocated_num > 8)
        return false;
    auto value_v = _mm512_set1_epi64(serialized_value);
    auto cached_values = _mm512_loadu_epi64(reinterpret_cast<const UInt8 *>(cache.values));
    auto cmp_mask = _mm512_cmpeq_epi64_mask(cached_values, value_v);
    auto hit_pos = _tzcnt_u32(cmp_mask);
    if (hit_pos >= 8) [[unlikely]]
    {
        if (cache.allocated_num >= 8)
        {
            cache.allocated_num += 1;
            return false;
        }
        value_id = allocated_value_id++;
        cache.value_ids[cache.allocated_num] = value_id;
        cache.values[cache.allocated_num++] = serialized_value;

        add_new_value_id(raw_value, value_id);
    }
    else
    {
        value_id = cache.value_ids[hit_pos];
    }
    return true;
}
)

class IHashValueIdGenerator
{
public:
    explicit IHashValueIdGenerator(const IColumn * /*col_*/, AdaptiveKeysHolder::State *state_, UInt32 value_id_bits_, size_t max_distinct_values_)
        : state(state_), value_id_bits(value_id_bits_), max_distinct_values(max_distinct_values_)
    {
    }
    virtual ~IHashValueIdGenerator() = default;

    void computeValueId(const IColumn * col, UInt64 * value_ids)
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
            return;

        if (enable_range_mode && (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID || allocated_value_id > range_max - range_min + 1))
        {
            for (UInt64 i = range_min; i <= range_max; ++i)
            {
                size_t alloc_size = 0;
                if (__builtin_add_overflow(sizeof(UInt64), pad_left + pad_right, &alloc_size))
                    throw DB::Exception(
                        DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Amount of memory requested to allocate is more than allowed");
                /// need to pad the address.
                auto * range_values_ptr = pool.alloc(alloc_size) + pad_left;
                UInt64 value_id = i - range_min + is_nullable;
                memcpy(range_values_ptr, reinterpret_cast<const UInt8 *>(&i), sizeof(UInt64));
                StringRef raw_value(range_values_ptr, row_bytes);
                emplaceValueId(raw_value, value_id);
            }
            enable_range_mode = false;
            enable_value_id_cache_line = false;
        }

        computeValueIdImpl(col, value_ids);

        if (!enable_range_mode && enable_value_id_cache_line && allocated_value_id > value_id_cache_line_num * 16)
        {
            enable_value_id_cache_line = false;
        }

        if (allocated_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
            release();
        }
    }

    void release()
    {
        releaseImpl();
    }

    virtual void emplaceValueId(const StringRef & raw_value, UInt64 value_id) = 0;

protected:
    AdaptiveKeysHolder::State *state;
    UInt32 value_id_bits;
    size_t max_distinct_values;

    /// If the values are in range [range_min, range_max], we can use the value - range_min as the
    /// value id directly. It's more efficient than using a hash map.
    /// range mode is only be used for fixed length rows.
    UInt64 range_min = -1UL;
    UInt64 range_max = 0;
    size_t row_bytes = 0; // the length of each row. must be fixed.
    bool enable_range_mode = false;
    bool is_nullable = false;

    const size_t max_sample_rows = 10000;

    /// The value id is allocated from 1. 0 is reserved for null.
    UInt64 allocated_value_id = 0;

    /// If the value id set is really small (size < value_id_cache_line_num * sizeof(HashValueIdCacheLine::value_ids))
    /// then we could try accelate the lookup by avx512 instruction.
    static constexpr size_t value_id_cache_line_num = 1;
    static constexpr size_t value_id_cache_line_num_mask = value_id_cache_line_num - 1;
    HashValueIdCacheLine value_ids_cache_line[value_id_cache_line_num];
    bool enable_value_id_cache_line = true;

    virtual void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) = 0;

    /// To use StringHashMap, need to make the memory address padded.
    static constexpr size_t pad_right = integerRoundUp(PADDING_FOR_SIMD - 1, 1);
    static constexpr size_t pad_left = integerRoundUp(PADDING_FOR_SIMD, 1);
    Arena pool;

    /// need to pad the address.
    ALWAYS_INLINE StringRef buildMapStringKey(const StringRef & raw_value)
    {
        size_t alloc_size = 0;
        if (__builtin_add_overflow(raw_value.size, pad_left + pad_right, &alloc_size))
            throw DB::Exception(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Amount of memory requested to allocate is more than allowed");
        /// need to pad the address.
        auto * new_str = pool.alloc(alloc_size) + pad_left;
        memcpy(new_str, raw_value.data, raw_value.size);
        return StringRef(new_str, raw_value.size);

    }

    /// If the row is null, assign 0 for this row.
    ALWAYS_INLINE void applyNullMap(UInt64 * __restrict value_ids, const UInt8 * __restrict null_map, size_t n)
    {
        if (null_map)
        {
            for (size_t i = 0; i < n; ++i)
                value_ids[i] &= ~(0 - static_cast<UInt64>(null_map[i]));
        }
    }

    virtual void releaseImpl() = 0;
};

class StringHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit StringHashValueIdGenerator(const IColumn * col_, AdaptiveKeysHolder::State * state_, UInt32 value_id_bits_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, value_id_bits_, max_distinct_values_)
    {
        setup(col_);
    }

private:
    StringHashMap<UInt64> value_ids_index;

    void setup(const IColumn * col);

    void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) override
    {
        if (col->empty())
            return;
        const auto * nested_col = col;
        const ColumnUInt8 * null_map = nullptr;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
            null_map = &(null_col->getNullMapColumn());
        }

        const auto * str_col = typeid_cast<const ColumnString *>(nested_col);
        if (!str_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid column: {}", col->getName());
        const auto & chars = str_col->getChars();
        const UInt8 * char_pos = chars.data();
        UInt64 * value_ids_pos = value_ids;

        const auto & offsets = str_col->getOffsets();
        IColumn::Offset prev_offset = 0;

        constexpr size_t batch_size = 512;
        alignas(64) UInt64 tmp_value_ids[batch_size] = {0};
        alignas(64) UInt64 str_lens[batch_size] = {0};
        size_t i = 0;
        size_t n = str_col->size();

        const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() : nullptr;
        for (; i + batch_size < n; i += batch_size)
        {
#if USE_MULTITARGET_CODE
            if (isArchSupported(TargetArch::AVX512BW))
                TargetSpecific::Default::computeStringsLengthFromOffsets(offsets, i, batch_size, str_lens);
            else
#endif
            {
                TargetSpecific::Default::computeStringsLengthFromOffsets(offsets, i, batch_size, str_lens);
            }
            null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
            computeValueIdForString(char_pos, str_lens, batch_size, tmp_value_ids, null_map_pos);
#if USE_MULTITARGET_CODE
            if (isArchSupported(TargetArch::AVX512BW))
                TargetSpecific::AVX512BW::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, batch_size);
            else
#endif
            {
                TargetSpecific::Default::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, batch_size);
            }
            value_ids_pos += batch_size;
            char_pos += offsets[i + batch_size - 1] - prev_offset;
            prev_offset = offsets[i + batch_size - 1];

            if (allocated_value_id > max_distinct_values)
            {
                state->hash_mode = AdaptiveKeysHolder::State::HASH;
                return;
            }
        }

        if (i < n)
        {
            auto remained = n - i;
            null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
            TargetSpecific::Default::computeStringsLengthFromOffsets(offsets, i, remained, str_lens);
            computeValueIdForString(char_pos, str_lens, remained, tmp_value_ids, null_map_pos);
            TargetSpecific::Default::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, remained);
        }
    }

    ALWAYS_INLINE void computeValueIdForString(const UInt8 * data_pos, const UInt64 * str_lens, size_t n, UInt64 * value_ids, const UInt8 * null_map)
    {
        for (size_t i = 0; i < n; ++i)
        {
            getValueId(StringRef(data_pos, str_lens[i] - 1), value_ids[i]);
            data_pos += str_lens[i];
        }
        applyNullMap(value_ids, null_map, n);
    }

    void emplaceValueId(const StringRef & raw_value, UInt64 value_id) override
    {
        emplaceValueIdImpl(raw_value, value_id);
    }

    ALWAYS_INLINE void emplaceValueIdImpl(const StringRef & raw_value, UInt64 value_id)
    {
        StringHashMap<UInt64>::LookupResult m_it;
        bool inserted = false;

        auto key = buildMapStringKey(raw_value);

        value_ids_index.emplace(key, m_it, inserted);
        if (!inserted)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert value id {} for ", value_id);
        }
        m_it->getMapped() = value_id;
    }

    ALWAYS_INLINE void getValueId(const StringRef & raw_value, UInt64 & value_id)
    {
        auto it = value_ids_index.find(raw_value);
        if (it) [[likely]]
        {
            value_id = it->getMapped();
        }
        else
        {
            value_id = allocated_value_id++;
            emplaceValueIdImpl(raw_value, value_id);
        }
    }

    void releaseImpl() override
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
        {
            value_ids_index.clearAndShrink();
        }
    }
};

class FixedStringHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit FixedStringHashValueIdGenerator(const IColumn *col_, AdaptiveKeysHolder::State *state_, UInt32 value_id_bits_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, value_id_bits_, max_distinct_values_)
    {
        setup(col_);
    }

private:
    StringHashMap<UInt64> value_ids_index;
    void setup(const IColumn * col)
    {
        is_nullable = col->isNullable();
        const auto * nested_col = col;
        if (is_nullable)
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
        }
        const auto * str_col = typeid_cast<const ColumnFixedString *>(nested_col);
        const auto & chars = str_col->getChars();
        const UInt8 * char_pos = chars.data();

        enable_range_mode = true;
        auto str_len = str_col->getN();
        row_bytes = str_len;
        if (str_len > 8)
        {
            enable_range_mode = false;
        }
        else
        {
            size_t i = 0;
            size_t n = std::min(max_sample_rows, str_col->size());
            for (i = 0; i < n; i++)
            {
                UInt64 val = 0;
                TargetSpecific::Default::shortValuesToUInt64(char_pos, str_len, &val, 1);
                if (val > range_max)
                    range_max = val;
                if (val < range_min)
                    range_min = val;
                if (range_max - range_min + 1 + is_nullable > max_distinct_values)
                {
                    enable_range_mode = false;
                    break;
                }
                char_pos += str_len;
            }
        }
        enable_range_mode = enable_range_mode && range_max > range_min && range_max - range_min + 1 + is_nullable <= max_distinct_values;
        if (enable_range_mode)
        {
            allocated_value_id = range_max - range_min + 1 + is_nullable;
        }
        else
        {
            allocated_value_id = is_nullable;
        }
        if (allocated_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    void computeValueIdImpl(const IColumn *col, UInt64 * value_ids) override
    {
        const auto * nested_col = col;
        const ColumnUInt8 * null_map = nullptr;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
            null_map = &(null_col->getNullMapColumn());
        }

        const auto * str_col = typeid_cast<const ColumnFixedString *>(nested_col);
        const auto & chars = str_col->getChars();
        const UInt8 * char_pos = chars.data();
        UInt64 * value_ids_pos = value_ids;
        auto str_len = str_col->getN();

        constexpr size_t batch_step = 512;
        alignas(64) UInt64 tmp_value_ids[batch_step] = {0};
        size_t i = 0;
        size_t n = str_col->size();

        for (; i + batch_step < n; i += batch_step)
        {
            const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;

            if (str_len <= 8 && enable_range_mode)
                computeValueIdsInRangeMode<UInt8, false>(tmp_value_ids, batch_step, char_pos, str_len, null_map_pos);
            else
                computeValueIdsInNormalMode(tmp_value_ids, batch_step, char_pos, str_len, null_map_pos);

#if USE_MULTITARGET_CODE
            if (isArchSupported(TargetArch::AVX512BW))
                TargetSpecific::AVX512BW::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, batch_step);
            else
#endif
            {
                TargetSpecific::Default::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, batch_step);
            }
            char_pos += str_len * batch_step;
            value_ids_pos += batch_step;

            if (allocated_value_id > max_distinct_values)
            {
                state->hash_mode = AdaptiveKeysHolder::State::HASH;
                return;
            }
        }

        if (i < n)
        {
            const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
            if (str_len <= 8 && enable_range_mode)
                computeValueIdsInRangeMode<UInt8, false>(tmp_value_ids, n - i, char_pos, str_len, null_map_pos);
            else
                computeValueIdsInNormalMode(tmp_value_ids, n - i, char_pos, str_len, null_map_pos);
            TargetSpecific::Default::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, n - i);
        }
    }

    void emplaceValueId(const StringRef & raw_value, UInt64 value_id) override
    {
        emplaceValueIdImpl(raw_value, value_id);
    }

    ALWAYS_INLINE void emplaceValueIdImpl(const StringRef & raw_value, UInt64 value_id)
    {
        StringHashMap<UInt64>::LookupResult m_it;
        bool inserted = false;

        auto key = buildMapStringKey(raw_value);

        value_ids_index.emplace(key, m_it, inserted);
        if (!inserted)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert value id {} for ", value_id);
        }
        m_it->getMapped() = value_id;
    }

    ALWAYS_INLINE void getValueId(const StringRef & raw_value, UInt64 & value_id)
    {
        auto it = value_ids_index.find(raw_value);
        if (it) [[likely]]
        {
            value_id = it->getMapped();
        }
        else
        {
            value_id = allocated_value_id++;
            emplaceValueIdImpl(raw_value, value_id);
        }
    }

    template<bool use_avx_512 = false>
    ALWAYS_INLINE void getValueId(const StringRef &raw_value, UInt64 serialized_value [[maybe_unused]], UInt64 & value_id)
    {

#if USE_MULTITARGET_CODE
        if constexpr (use_avx_512)
        {
            if (enable_value_id_cache_line)
            {
                auto & cache = value_ids_cache_line[serialized_value & value_id_cache_line_num_mask];
                auto ok = TargetSpecific::AVX512BW::quickLookupValueId(
                    cache,
                    raw_value,
                    serialized_value,
                    value_id,
                    allocated_value_id,
                    [&](const StringRef & raw_value_, UInt64 value_id_) { emplaceValueId(raw_value_, value_id_); });
                if (ok)
                    return;
            }
        }
#endif

        getValueId(raw_value, value_id);
    }

    ALWAYS_INLINE void computeValueIdsInNormalMode(UInt64 * __restrict value_ids, size_t n, const UInt8 * __restrict data_pos, size_t element_bytes, const UInt8 * __restrict null_map)
    {
        for (size_t i = 0; i < n; ++i)
        {
            getValueId(StringRef(data_pos, element_bytes), value_ids[i]);
            data_pos += element_bytes;
        }
        applyNullMap(value_ids, null_map, n);
    }

    template<typename T, bool data_pos_aligned>
    ALWAYS_INLINE void computeValueIdsInRangeMode(UInt64 * __restrict value_ids, size_t n, const UInt8 * __restrict data_pos, size_t element_bytes, const UInt8 * __restrict null_map)
    {
        UInt64 range_delta = range_min - is_nullable;
        if (element_bytes <= 8 && data_pos_aligned)
            TargetSpecific::Default::extendValuesToUInt64<T>(data_pos, value_ids, n);
        else
            TargetSpecific::Default::shortValuesToUInt64(data_pos, element_bytes, value_ids, n);
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
            TargetSpecific::AVX512BW::getValueIdsByRange(value_ids, range_delta, n);
        else
#endif
        {
            TargetSpecific::Default::getValueIdsByRange(value_ids, range_delta, n);
        }

        UInt64 range_length = range_max - range_min + is_nullable;
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
        {
            for (size_t i = 0; i < n; ++i)
            {
                if (is_nullable > value_ids[i] || value_ids[i] > range_length)
                {
                    getValueId<true>(StringRef(data_pos, element_bytes), value_ids[i] + range_delta, value_ids[i]);
                }
                data_pos += element_bytes;
            }
        }
        else
#endif
        {
            for (size_t i = 0; i < n; ++i)
            {
                if (is_nullable > value_ids[i] || value_ids[i] > range_length)
                {
                    getValueId<false>(StringRef(data_pos, element_bytes), value_ids[i] + range_delta, value_ids[i]);
                }
                data_pos += element_bytes;
            }
        }
        applyNullMap(value_ids, null_map, n);
    }

    void releaseImpl() override
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
        {
            value_ids_index.clearAndShrink();
        }
    }
};

template <typename ElementType, typename ColumnType, bool is_basic_number>
class NumericHashValueIdGenerator : public IHashValueIdGenerator
{
public:
    explicit NumericHashValueIdGenerator(const IColumn * col_, AdaptiveKeysHolder::State * state_, UInt32 value_id_bits_, size_t max_distinct_values_)
        : IHashValueIdGenerator(col_, state_, value_id_bits_, max_distinct_values_)
    {
        setup(col_);
    }
private:
    HashMapWithSavedHash<ElementType, UInt64> value_ids_index;

    void setup(const IColumn * col)
    {
        /// Find the min and max value in the column
        is_nullable = col->isNullable();
        const auto * nested_col = col;
        if (is_nullable)
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
        }
        size_t element_bytes = sizeof(ElementType);
        row_bytes = element_bytes;
        const auto * num_col = typeid_cast<const ColumnType *>(nested_col);
        const UInt8 * data_pos = reinterpret_cast<const UInt8 *>(num_col->getData().data());
        size_t i = 0;
        size_t n = std::min(max_sample_rows, num_col->size());
        enable_range_mode = n > 0;
        for (i = 0; i < n; ++i)
        {
            if (element_bytes > 8)
            {
                enable_range_mode = false;
                break;
            }
            UInt64 val = 0;
            TargetSpecific::Default::extendValuesToUInt64<ElementType>(data_pos, &val, 1);
            if (val > range_max)
                range_max = val;
            if (val < range_min)
                range_min = val;
            if (range_max - range_min + 1 + is_nullable > max_distinct_values)
            {
                enable_range_mode = false;
                break;
            }
            data_pos += element_bytes;
        }
        enable_range_mode = enable_range_mode && range_max > range_min && range_max - range_min + 1 + is_nullable <= max_distinct_values;
        if (enable_range_mode)
        {
            allocated_value_id =  range_max - range_min + 1 + is_nullable;
        }
        else
        {
            allocated_value_id = is_nullable;
        }

        if (allocated_value_id > max_distinct_values)
        {
            state->hash_mode = AdaptiveKeysHolder::State::HASH;
        }
    }

    void computeValueIdImpl(const IColumn * col, UInt64 * value_ids) override
    {
        const auto * nested_col = col;
        const ColumnUInt8 * null_map = nullptr;
        if (col->isNullable())
        {
            const auto * null_col = typeid_cast<const ColumnNullable *>(col);
            if (!null_col)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is nullable but not ColumnNullable", col->getName());
            nested_col = &(null_col->getNestedColumn());
            null_map = &(null_col->getNullMapColumn());
        }
        const auto * num_col = typeid_cast<const ColumnType *>(nested_col);
        size_t element_bytes = sizeof(ElementType);
        const UInt8 * data_pos = reinterpret_cast<const UInt8 *>(num_col->getData().data());
        UInt64 * value_ids_pos = value_ids;
        size_t i = 0;
        size_t n = num_col->size();

        constexpr size_t batch_step = 512;
        alignas(64) UInt64 tmp_value_ids[batch_step] = {0};
        for (; i + batch_step < n; i += batch_step)
        {
            const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;

            if constexpr (is_basic_number)
                if (enable_range_mode)
                    computeValueIdsInRangeMode<ElementType, true>(tmp_value_ids, batch_step, data_pos, element_bytes, null_map_pos);
                else
                {
                    computeValueIdsInNormalMode(tmp_value_ids, batch_step, data_pos, element_bytes, null_map_pos);
                }
            else
                computeValueIdsInNormalMode(tmp_value_ids, batch_step, data_pos, element_bytes, null_map_pos);


#if USE_MULTITARGET_CODE
            if (isArchSupported(TargetArch::AVX512BW))
                TargetSpecific::AVX512BW::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, batch_step);
            else
#endif
            {
                TargetSpecific::Default::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, batch_step);
            }

            data_pos += element_bytes * batch_step;
            value_ids_pos += batch_step;

            if (allocated_value_id > max_distinct_values)
            {
                state->hash_mode = AdaptiveKeysHolder::State::HASH;
                return;
            }
        }

        if (i < n)
        {
            const UInt8 * null_map_pos = is_nullable ? null_map->getData().data() + i : nullptr;
            if constexpr (is_basic_number)
                if (enable_range_mode)
                    computeValueIdsInRangeMode<ElementType, true>(tmp_value_ids, n - i, data_pos, element_bytes, null_map_pos);
                else
                    computeValueIdsInNormalMode(tmp_value_ids, n - i, data_pos, element_bytes, null_map_pos);
            else
                computeValueIdsInNormalMode(tmp_value_ids, n - i, data_pos, element_bytes, null_map_pos);
            TargetSpecific::Default::concateValueIds(tmp_value_ids, value_ids_pos, value_id_bits, n - i);
        }
    }

    void emplaceValueId(const StringRef & raw_value, UInt64 value_id) override
    {
        emplaceValueIdImpl(raw_value, value_id);
    }

    ALWAYS_INLINE void emplaceValueIdImpl(const StringRef & raw_value, UInt64 value_id)
    {
        const ElementType * value = reinterpret_cast<const ElementType *>(raw_value.data);
        typename HashMapWithSavedHash<ElementType, UInt64>::LookupResult m_it;
        bool inserted = false;
        value_ids_index.emplace(*value, m_it, inserted);
        m_it->getMapped() = value_id;
    }

    ALWAYS_INLINE void getValueId(const StringRef & raw_value, UInt64 & value_id)
    {
        const ElementType * value = reinterpret_cast<const ElementType *>(raw_value.data);
        auto it = value_ids_index.find(*value);
        if (it) [[likely]]
        {
            value_id = it->getMapped();
        }
        else
        {
            value_id = allocated_value_id++;
            emplaceValueIdImpl(raw_value, value_id);
        }
    }

    ALWAYS_INLINE void computeValueIdsInNormalMode(UInt64 * __restrict value_ids, size_t n, const UInt8 * __restrict data_pos, size_t element_bytes, const UInt8 * __restrict null_map)
    {
        for (size_t i = 0; i < n; ++i)
        {
            getValueId(StringRef(data_pos, element_bytes), value_ids[i]);
            data_pos += element_bytes;
        }
        applyNullMap(value_ids, null_map, n);
    }

    template<typename T, bool data_pos_aligned>
    ALWAYS_INLINE void computeValueIdsInRangeMode(UInt64 * __restrict value_ids, size_t n, const UInt8 * __restrict data_pos, size_t element_bytes, const UInt8 * __restrict null_map)
    {
        UInt64 range_delta = range_min - is_nullable;
        if (element_bytes <= 8 && data_pos_aligned)
            TargetSpecific::Default::extendValuesToUInt64<T>(data_pos, value_ids, n);
        else
            TargetSpecific::Default::shortValuesToUInt64(data_pos, element_bytes, value_ids, n);
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
            TargetSpecific::AVX512BW::getValueIdsByRange(value_ids, range_delta, n);
        else
#endif
        {
            TargetSpecific::Default::getValueIdsByRange(value_ids, range_delta, n);
        }

        UInt64 range_length = range_max - range_min + is_nullable;
        for (size_t i = 0; i < n; ++i)
        {
            if (is_nullable > value_ids[i] || value_ids[i] > range_length)
                getValueId(StringRef(data_pos, element_bytes), value_ids[i]);
            data_pos += element_bytes;
        }
        applyNullMap(value_ids, null_map, n);
    }

    void releaseImpl() override
    {
        if (state->hash_mode != AdaptiveKeysHolder::State::VALUE_ID)
        {
            value_ids_index.clearAndShrink();
        }
    }
};

class HashValueIdGeneratorFactory
{
public:
    static HashValueIdGeneratorFactory & instance();
    std::unique_ptr<IHashValueIdGenerator> getGenerator(AdaptiveKeysHolder::State *state_, UInt32 values_id_bits_, const IColumn * col);
};
}
