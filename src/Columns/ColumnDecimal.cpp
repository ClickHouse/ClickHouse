#include <Common/Exception.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/WeakHash.h>
#include <Common/HashTable/Hash.h>

#include <common/unaligned.h>
#include <common/sort.h>
#include <common/scope_guard.h>


#include <IO/WriteHelpers.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnCompressed.h>
#include <DataStreams/ColumnGathererStream.h>


template <typename T> bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

template class DecimalPaddedPODArray<Decimal32>;
template class DecimalPaddedPODArray<Decimal64>;
template class DecimalPaddedPODArray<Decimal128>;
template class DecimalPaddedPODArray<Decimal256>;
template class DecimalPaddedPODArray<DateTime64>;

template <typename T>
int ColumnDecimal<T>::compareAt(size_t n, size_t m, const IColumn & rhs_, int) const
{
    auto & other = static_cast<const Self &>(rhs_);
    const T & a = data[n];
    const T & b = other.data[m];

    if (scale == other.scale)
        return a > b ? 1 : (a < b ? -1 : 0);
    return decimalLess<T>(b, a, other.scale, scale) ? 1 : (decimalLess<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <typename T>
void ColumnDecimal<T>::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                     PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                     int direction, int nan_direction_hint) const
{
    return this->template doCompareColumn<ColumnDecimal<T>>(static_cast<const Self &>(rhs), rhs_row_num, row_indexes,
                                                         compare_results, direction, nan_direction_hint);
}

template <typename T>
bool ColumnDecimal<T>::hasEqualValues() const
{
    return this->template hasEqualValuesImpl<ColumnDecimal<T>>();
}

template <typename T>
StringRef ColumnDecimal<T>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    auto * pos = arena.allocContinue(sizeof(T), begin);
    memcpy(pos, &data[n], sizeof(T));
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char * ColumnDecimal<T>::deserializeAndInsertFromArena(const char * pos)
{
    data.push_back(unalignedLoad<T>(pos));
    return pos + sizeof(T);
}

template <typename T>
const char * ColumnDecimal<T>::skipSerializedInArena(const char * pos) const
{
    return pos + sizeof(T);
}

template <typename T>
UInt64 ColumnDecimal<T>::get64([[maybe_unused]] size_t n) const
{
    if constexpr (sizeof(T) > sizeof(UInt64))
        throw Exception(String("Method get64 is not supported for ") + getFamilyName(), ErrorCodes::NOT_IMPLEMENTED);
    else
        return static_cast<NativeT>(data[n]);
}

template <typename T>
void ColumnDecimal<T>::updateHashWithValue(size_t n, SipHash & hash) const
{
    hash.update(data[n].value);
}

template <typename T>
void ColumnDecimal<T>::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = data.size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const T * begin = data.data();
    const T * end = begin + s;
    UInt32 * hash_data = hash.getData().data();

    while (begin < end)
    {
        *hash_data = intHashCRC32(*begin, *hash_data);
        ++begin;
        ++hash_data;
    }
}

template <typename T>
void ColumnDecimal<T>::updateHashFast(SipHash & hash) const
{
    hash.update(reinterpret_cast<const char *>(data.data()), size() * sizeof(data[0]));
}

template <typename T>
void ColumnDecimal<T>::getPermutation(bool reverse, size_t limit, int , IColumn::Permutation & res) const
{
#if 1 /// TODO: perf test
    if (data.size() <= std::numeric_limits<UInt32>::max())
    {
        PaddedPODArray<UInt32> tmp_res;
        permutation(reverse, limit, tmp_res);

        res.resize(tmp_res.size());
        for (size_t i = 0; i < tmp_res.size(); ++i)
            res[i] = tmp_res[i];
        return;
    }
#endif

    permutation(reverse, limit, res);
}

template <typename T>
void ColumnDecimal<T>::updatePermutation(bool reverse, size_t limit, int, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    if (equal_ranges.empty())
        return;

    if (limit >= data.size() || limit >= equal_ranges.back().second)
        limit = 0;

    size_t number_of_ranges = equal_ranges.size();
    if (limit)
        --number_of_ranges;

    EqualRanges new_ranges;
    SCOPE_EXIT({equal_ranges = std::move(new_ranges);});

    for (size_t i = 0; i < number_of_ranges; ++i)
    {
        const auto& [first, last] = equal_ranges[i];
        if (reverse)
            std::sort(res.begin() + first, res.begin() + last,
                [this](size_t a, size_t b) { return data[a] > data[b]; });
        else
            std::sort(res.begin() + first, res.begin() + last,
                [this](size_t a, size_t b) { return data[a] < data[b]; });

        auto new_first = first;
        for (auto j = first + 1; j < last; ++j)
        {
            if (data[res[new_first]] != data[res[j]])
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }
        if (last - new_first > 1)
            new_ranges.emplace_back(new_first, last);
    }

    if (limit)
    {
        const auto & [first, last] = equal_ranges.back();

        if (limit < first || limit > last)
            return;

        /// Since then we are working inside the interval.

        if (reverse)
            partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last,
                [this](size_t a, size_t b) { return data[a] > data[b]; });
        else
            partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last,
                [this](size_t a, size_t b) { return data[a] < data[b]; });
        auto new_first = first;
        for (auto j = first + 1; j < limit; ++j)
        {
            if (data[res[new_first]] != data[res[j]])
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }
        auto new_last = limit;
        for (auto j = limit; j < last; ++j)
        {
            if (data[res[new_first]] == data[res[j]])
            {
                std::swap(res[new_last], res[j]);
                ++new_last;
            }
        }
        if (new_last - new_first > 1)
            new_ranges.emplace_back(new_first, new_last);
    }
}

template <typename T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = limit ? std::min(data.size(), limit) : data.size();
    if (perm.size() < size)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(size, scale);
    typename Self::Container & res_data = res->getData();

    for (size_t i = 0; i < size; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
MutableColumnPtr ColumnDecimal<T>::cloneResized(size_t size) const
{
    auto res = this->create(0, scale);

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);

        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count)
        {
            void * tail = &new_col.data[count];
            memset(tail, 0, (size - count) * sizeof(T));
        }
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::insertData(const char * src, size_t /*length*/)
{
    T tmp;
    memcpy(&tmp, src, sizeof(T));
    data.emplace_back(tmp);
}

template <typename T>
void ColumnDecimal<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnDecimal & src_vec = assert_cast<const ColumnDecimal &>(src);

    if (start + length > src_vec.data.size())
        throw Exception("Parameters start = " + toString(start) + ", length = " + toString(length) +
            " are out of bound in ColumnDecimal<T>::insertRangeFrom method (data.size() = " + toString(src_vec.data.size()) + ").",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);

    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(0, scale);
    Container & res_data = res->getData();

    if (result_size_hint)
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = data.data();

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnDecimal<T>::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::replicate(const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(0, scale);
    if (0 == size)
        return res;

    typename Self::Container & res_data = res->getData();
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_data.push_back(data[i]);
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::compress() const
{
    size_t source_size = data.size() * sizeof(T);

    /// Don't compress small blocks.
    if (source_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto compressed = ColumnCompressed::compressBuffer(data.data(), source_size, false);

    if (!compressed)
        return ColumnCompressed::wrap(this->getPtr());

    return ColumnCompressed::create(data.size(), compressed->size(),
        [compressed = std::move(compressed), column_size = data.size(), scale = this->scale]
        {
            auto res = ColumnDecimal<T>::create(column_size, scale);
            ColumnCompressed::decompressBuffer(
                compressed->data(), res->getData().data(), compressed->size(), column_size * sizeof(T));
            return res;
        });
}

template <typename T>
void ColumnDecimal<T>::getExtremes(Field & min, Field & max) const
{
    if (data.empty())
    {
        min = NearestFieldType<T>(T(0), scale);
        max = NearestFieldType<T>(T(0), scale);
        return;
    }

    T cur_min = data[0];
    T cur_max = data[0];

    for (const T & x : data)
    {
        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = NearestFieldType<T>(cur_min, scale);
    max = NearestFieldType<T>(cur_max, scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
template class ColumnDecimal<Decimal256>;
template class ColumnDecimal<DateTime64>;

}
