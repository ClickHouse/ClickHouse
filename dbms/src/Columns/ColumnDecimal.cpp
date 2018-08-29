//#include <cstring>
#include <cmath>

#include <Common/Exception.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>

#include <IO/WriteHelpers.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <DataStreams/ColumnGathererStream.h>

template <typename T> bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

template <typename T>
int ColumnDecimal<T>::compareAt(size_t n, size_t m, const IColumn & rhs_, int ) const
{
    auto other = static_cast<const Self &>(rhs_);
    const T & a = data[n];
    const T & b = static_cast<const Self &>(rhs_).data[m];

    return decimalLess<T>(b, a, other.scale, scale) ? 1 : (decimalLess<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <typename T>
StringRef ColumnDecimal<T>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    auto pos = arena.allocContinue(sizeof(T), begin);
    memcpy(pos, &data[n], sizeof(T));
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char * ColumnDecimal<T>::deserializeAndInsertFromArena(const char * pos)
{
    data.push_back(*reinterpret_cast<const T *>(pos));
    return pos + sizeof(T);
}

template <typename T>
void ColumnDecimal<T>::updateHashWithValue(size_t n, SipHash & hash) const
{
    hash.update(data[n]);
}

template <typename T>
void ColumnDecimal<T>::getPermutation(bool reverse, size_t limit, int , IColumn::Permutation & res) const
{
    size_t s = data.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), [](T a, T b) { return a > b; });
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), [](T a, T b) { return a < b; });
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), [](T a, T b) { return a > b; });
        else
            std::sort(res.begin(), res.end(), [](T a, T b) { return a < b; });
    }
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
        memcpy(&new_col.data[0], &data[0], count * sizeof(data[0]));

        if (size > count)
            memset(static_cast<void *>(&new_col.data[count]), static_cast<int>(value_type()), (size - count) * sizeof(value_type));
    }

    return std::move(res);
}

template <typename T>
void ColumnDecimal<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnDecimal & src_vec = static_cast<const ColumnDecimal &>(src);

    if (start + length > src_vec.data.size())
        throw Exception("Parameters start = "
            + toString(start) + ", length = "
            + toString(length) + " are out of bound in ColumnVector<T>::insertRangeFrom method"
            " (data.size() = " + toString(src_vec.data.size()) + ").",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(&data[old_size], &src_vec.data[start], length * sizeof(data[0]));
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

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = &data[0];

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return std::move(res);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(limit, scale);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[perm[i]];

    return std::move(res);
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

    return std::move(res);
}

template <typename T>
void ColumnDecimal<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
void ColumnDecimal<T>::getExtremes(Field & min, Field & max) const
{
    if (data.size() == 0)
    {
        min = typename NearestFieldType<T>::Type(0, scale);
        max = typename NearestFieldType<T>::Type(0, scale);
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

    min = typename NearestFieldType<T>::Type(cur_min, scale);
    max = typename NearestFieldType<T>::Type(cur_max, scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
}
