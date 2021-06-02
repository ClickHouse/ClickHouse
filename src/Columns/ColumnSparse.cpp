#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnTuple.h>
#include <Common/WeakHash.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <DataStreams/ColumnGathererStream.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

ColumnSparse::ColumnSparse(MutableColumnPtr && values_)
    : values(std::move(values_)), _size(0)
{
    if (!values->empty())
        throw Exception("Not empty values passed to ColumnSparse, but no offsets passed", ErrorCodes::LOGICAL_ERROR);

    values->insertDefault();
    offsets = ColumnUInt64::create();
}

ColumnSparse::ColumnSparse(MutableColumnPtr && values_, MutableColumnPtr && offsets_, size_t size_)
    : values(std::move(values_)), offsets(std::move(offsets_)), _size(size_)
{
    const ColumnUInt64 * offsets_concrete = typeid_cast<const ColumnUInt64 *>(offsets.get());

    if (!offsets_concrete)
        throw Exception("offsets_column must be a ColumnUInt64", ErrorCodes::LOGICAL_ERROR);

    /// 'values' should contain one extra element: default value at 0 position.
    if (offsets->size() + 1 != values->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Values size ({}) is inconsistent with offsets size ({})", values->size(), offsets->size());

    if (_size < offsets->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Size of sparse column ({}) cannot be lower than number of non-default values ({})", _size, offsets->size());

#ifndef NDEBUG
    const auto & offsets_data = getOffsetsData();
    const auto * it = std::adjacent_find(offsets_data.begin(), offsets_data.end(), std::greater_equal<UInt64>());
    if (it != offsets_data.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Offsets of ColumnSparse must be strictly sorted");
#endif
}

MutableColumnPtr ColumnSparse::cloneResized(size_t new_size) const
{
    if (new_size == 0)
        return ColumnSparse::create(values->cloneEmpty());

    if (new_size >= _size)
        return ColumnSparse::create(IColumn::mutate(values), IColumn::mutate(offsets), new_size);

    auto res = ColumnSparse::create(values->cloneEmpty());
    res->insertRangeFrom(*this, 0, new_size);
    return res;
}

bool ColumnSparse::isNullAt(size_t n) const
{
    return values->isNullAt(getValueIndex(n));
}

Field ColumnSparse::operator[](size_t n) const
{
    return (*values)[getValueIndex(n)];
}

void ColumnSparse::get(size_t n, Field & res) const
{
    values->get(getValueIndex(n), res);
}

bool ColumnSparse::getBool(size_t n) const
{
    return values->getBool(getValueIndex(n));
}

Float64 ColumnSparse::getFloat64(size_t n) const
{
    return values->getFloat64(getValueIndex(n));
}

Float32 ColumnSparse::getFloat32(size_t n) const
{
    return values->getFloat32(getValueIndex(n));
}

UInt64 ColumnSparse::getUInt(size_t n) const
{
    return values->getUInt(getValueIndex(n));
}

Int64 ColumnSparse::getInt(size_t n) const
{
    return values->getInt(getValueIndex(n));
}

UInt64 ColumnSparse::get64(size_t n) const
{
    return values->get64(getValueIndex(n));
}

StringRef ColumnSparse::getDataAt(size_t n) const
{
    return values->getDataAt(getValueIndex(n));
}

ColumnPtr ColumnSparse::convertToFullColumnIfSparse() const
{
    return values->createWithOffsets(getOffsetsData(), _size, 1);
}

void ColumnSparse::insertData(const char * pos, size_t length)
{
    _size += length;
    return values->insertData(pos, length);
}

/// TODO: maybe need to reimplement it.
StringRef ColumnSparse::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return values->serializeValueIntoArena(getValueIndex(n), arena, begin);
}

const char * ColumnSparse::deserializeAndInsertFromArena(const char * pos)
{
    ++_size;
    return values->deserializeAndInsertFromArena(pos);
}

const char * ColumnSparse::skipSerializedInArena(const char * pos) const
{
    return values->skipSerializedInArena(pos);
}

void ColumnSparse::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    if (start + length > src.size())
        throw Exception("Parameter out of bound in IColumnString::insertRangeFrom method.",
            ErrorCodes::LOGICAL_ERROR);

    auto & offsets_data = getOffsetsData();

    size_t end = start + length;
    if (const auto * src_sparse = typeid_cast<const ColumnSparse *>(&src))
    {
        const auto & src_offsets = src_sparse->getOffsetsData();
        const auto & src_values = src_sparse->getValuesColumn();

        size_t offset_start = std::lower_bound(src_offsets.begin(), src_offsets.end(), start) - src_offsets.begin();
        size_t offset_end = std::lower_bound(src_offsets.begin(), src_offsets.end(), end) - src_offsets.begin();

        if (offset_start != offset_end)
        {
            insertManyDefaults(src_offsets[offset_start] - start);
            offsets_data.push_back(_size);
            ++_size;

            for (size_t i = offset_start + 1; i < offset_end; ++i)
            {
                size_t current_diff = src_offsets[i] - src_offsets[i - 1];
                insertManyDefaults(current_diff - 1);
                offsets_data.push_back(_size);
                ++_size;
            }

            /// 'end' <= 'src_offsets[offsets_end]', but end is excluded, so index is 'offsets_end' - 1.
            /// Since 'end' is excluded need, to subtract one more row from result.
            insertManyDefaults(end - src_offsets[offset_end - 1] - 1);
            values->insertRangeFrom(src_values, offset_start + 1, offset_end - offset_start);
        }
        else
        {
            insertManyDefaults(length);
        }
    }
    else
    {
        for (size_t i = start; i < end; ++i)
        {
            offsets_data.push_back(_size);
            ++_size;
        }

        values->insertRangeFrom(src, start, length);
    }
}

void ColumnSparse::insert(const Field & x)
{
    getOffsetsData().push_back(_size);
    values->insert(x);
    ++_size;
}

void ColumnSparse::insertFrom(const IColumn & src, size_t n)
{

    if (const auto * src_sparse = typeid_cast<const ColumnSparse *>(&src))
    {
        if (size_t value_index = src_sparse->getValueIndex(n))
        {
            getOffsetsData().push_back(_size);
            values->insertFrom(src_sparse->getValuesColumn(), value_index);
        }
    }
    else
    {
        getOffsetsData().push_back(_size);
        values->insertFrom(src, n);
    }

    ++_size;
}

void ColumnSparse::insertDefault()
{
    ++_size;
}

void ColumnSparse::insertManyDefaults(size_t length)
{
    _size += length;
}

void ColumnSparse::popBack(size_t n)
{
    assert(n < _size);

    auto & offsets_data = getOffsetsData();
    size_t new_size = _size - n;

    size_t removed_values = 0;
    while (!offsets_data.empty() && offsets_data.back() >= new_size)
    {
        offsets_data.pop_back();
        ++removed_values;
    }

    if (removed_values)
        values->popBack(removed_values);

    _size = new_size;
}

ColumnPtr ColumnSparse::filter(const Filter & filt, ssize_t) const
{
    if (_size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (offsets->empty())
    {
        auto res = cloneEmpty();
        res->insertManyDefaults(countBytesInFilter(filt));
        return res;
    }

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();

    Filter values_filter;
    values_filter.reserve(values->size());
    values_filter.push_back(1);
    size_t values_result_size_hint = 1;

    size_t res_offset = 0;
    auto offset_it = begin();
    for (size_t i = 0; i < _size; ++i, ++offset_it)
    {
        if (!offset_it.isDefault())
        {
            if (filt[i])
            {
                res_offsets_data.push_back(res_offset);
                values_filter.push_back(1);
                ++res_offset;
                ++values_result_size_hint;
            }
            else
            {
                values_filter.push_back(0);
            }
        }
        else
        {
            res_offset += filt[i] != 0;
        }
    }

    auto res_values = values->filter(values_filter, values_result_size_hint);
    return this->create(std::move(res_values), std::move(res_offsets), res_offset);
}

ColumnPtr ColumnSparse::permute(const Permutation & perm, size_t limit) const
{
    limit = limit ? std::min(limit, _size) : _size;

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (offsets->empty())
    {
        auto res = cloneEmpty();
        res->insertManyDefaults(limit);
        return res;
    }

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();
    auto res_values = values->cloneEmpty();
    res_values->insertDefault();

    /// If we need to permute full column, or if limit is large enough,
    /// it's better to save indexes of values in O(size)
    /// and avoid binary search for obtaining every index.
    /// 3 is just a guess for overhead on copying indexes.
    bool execute_linear =
        limit == _size || limit * std::bit_width(offsets->size()) > _size * 3;

    if (execute_linear)
    {
        PaddedPODArray<UInt64> indexes(_size);
        auto offset_it = begin();
        for (size_t i = 0; i < _size; ++i, ++offset_it)
            indexes[i] = offset_it.getValueIndex();

        for (size_t i = 0; i < limit; ++i)
        {
            size_t index = indexes[perm[i]];
            if (index != 0)
            {
                res_values->insertFrom(*values, index);
                res_offsets_data.push_back(i);
            }
        }
    }
    else
    {
        for (size_t i = 0; i < limit; ++i)
        {
            size_t index = getValueIndex(perm[i]);
            if (index != 0)
            {
                res_values->insertFrom(*values, index);
                res_offsets_data.push_back(i);
            }
        }
    }

    return ColumnSparse::create(std::move(res_values), std::move(res_offsets), limit);
}

ColumnPtr ColumnSparse::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnSparse::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    limit = limit ? std::min(limit, indexes.size()) : indexes.size();

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();
    auto res_values = values->cloneEmpty();
    res_values->insertDefault();

    for (size_t i = 0; i < limit; ++i)
    {
        size_t index = getValueIndex(indexes[i]);
        if (index != 0)
        {
            res_values->insertFrom(*values, index);
            res_offsets_data.push_back(i);
        }
    }

    return ColumnSparse::create(std::move(res_values), std::move(res_offsets), limit);
}

int ColumnSparse::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
{
    if (const auto * rhs_sparse = typeid_cast<const ColumnSparse *>(&rhs_))
        return values->compareAt(getValueIndex(n), rhs_sparse->getValueIndex(m), rhs_sparse->getValuesColumn(), null_direction_hint);

    return values->compareAt(getValueIndex(n), m, rhs_, null_direction_hint);
}

void ColumnSparse::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                    PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                    int direction, int nan_direction_hint) const
{
    if (row_indexes)
    {
        /// TODO: implement without conversion to full column.
        auto this_full = convertToFullColumnIfSparse();
        auto rhs_full = rhs.convertToFullColumnIfSparse();
        this_full->compareColumn(*rhs_full, rhs_row_num, row_indexes, compare_results, direction, nan_direction_hint);
    }
    else
    {
        const auto & rhs_sparse = assert_cast<const ColumnSparse &>(rhs);
        PaddedPODArray<Int8> nested_result;
        values->compareColumn(rhs_sparse.getValuesColumn(), rhs_sparse.getValueIndex(rhs_row_num),
            nullptr, nested_result, direction, nan_direction_hint);

        const auto & offsets_data = getOffsetsData();
        compare_results.resize_fill(_size, nested_result[0]);
        for (size_t i = 0; i < offsets_data.size(); ++i)
            compare_results[offsets_data[i]] = nested_result[i + 1];
    }
}

int ColumnSparse::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator & collator) const
{
    if (const auto * rhs_sparse = typeid_cast<const ColumnSparse *>(&rhs))
        return values->compareAtWithCollation(getValueIndex(n), rhs_sparse->getValueIndex(m), rhs_sparse->getValuesColumn(), null_direction_hint, collator);

    return values->compareAtWithCollation(getValueIndex(n), m, rhs, null_direction_hint, collator);
}

bool ColumnSparse::hasEqualValues() const
{
    return offsets->empty();
}

void ColumnSparse::getPermutationImpl(bool reverse, size_t limit, int null_direction_hint, Permutation & res, const Collator * collator) const
{
    if (_size == 0)
        return;

    res.resize(_size);
    if (offsets->empty())
    {
        for (size_t i = 0; i < _size; ++i)
            res[i] = i;
        return;
    }

    if (limit == 0 || limit > _size)
        limit = _size;

    Permutation perm;
    /// Firstly we sort all values.
    /// limit + 1 for case when there are 0 default values.
    if (collator)
        values->getPermutationWithCollation(*collator, reverse, limit + 1, null_direction_hint, perm);
    else
        values->getPermutation(reverse, limit + 1, null_direction_hint, perm);

    size_t num_of_defaults = getNumberOfDefaults();
    size_t row = 0;

    const auto & offsets_data = getOffsetsData();

    /// Fill the permutation.
    for (size_t i = 0; i < perm.size() && row < limit; ++i)
    {
        if (perm[i] == 0)
        {
            if (!num_of_defaults)
                continue;

            /// Fill the positions of default values in the required quantity.
            auto offset_it = begin();
            while (row < limit)
            {
                while (offset_it.getCurrentRow() < _size && !offset_it.isDefault())
                    ++offset_it;

                if (offset_it.getCurrentRow() == _size)
                    break;

                res[row++] = offset_it.getCurrentRow();
                ++offset_it;
            }
        }
        else
        {
            res[row++] = offsets_data[perm[i] - 1];
        }
    }

    assert(row == limit);
}

void ColumnSparse::getPermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res) const
{
    return getPermutationImpl(reverse, limit, null_direction_hint, res, nullptr);
}

void ColumnSparse::updatePermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_range) const
{
    auto this_full = convertToFullColumnIfSparse();
    this_full->updatePermutation(reverse, limit, null_direction_hint, res, equal_range);
}

void ColumnSparse::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int null_direction_hint, Permutation & res) const
{
    return getPermutationImpl(reverse, limit, null_direction_hint, res, &collator);
}

void ColumnSparse::updatePermutationWithCollation(
    const Collator & collator, bool reverse, size_t limit, int null_direction_hint, Permutation & res, EqualRanges& equal_range) const
{
    auto this_full = convertToFullColumnIfSparse();
    this_full->updatePermutationWithCollation(collator, reverse, limit, null_direction_hint, res, equal_range);
}

size_t ColumnSparse::byteSize() const
{
    return values->byteSize() + offsets->byteSize();
}

size_t ColumnSparse::byteSizeAt(size_t n) const
{
    size_t index = getValueIndex(n);
    size_t res = values->byteSizeAt(index);
    if (index)
        res += sizeof(UInt64);

    return res;
}

size_t ColumnSparse::allocatedBytes() const
{
    return values->allocatedBytes() + offsets->allocatedBytes();
}

void ColumnSparse::protect()
{
    values->protect();
    offsets->protect();
}

ColumnPtr ColumnSparse::replicate(const Offsets & replicate_offsets) const
{
    /// TODO: implement specializations.
    if (_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (_size == 0)
        return ColumnSparse::create(values->cloneEmpty());

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();
    auto res_values = values->cloneEmpty();
    res_values->insertDefault();

    auto offset_it = begin();
    for (size_t i = 0; i < _size; ++i, ++offset_it)
    {
        if (!offset_it.isDefault())
        {
            size_t replicate_size = replicate_offsets[i] - replicate_offsets[i - 1];
            res_offsets_data.reserve(res_offsets_data.size() + replicate_size);
            for (size_t row = replicate_offsets[i - 1]; row < replicate_offsets[i]; ++row)
            {
                res_offsets_data.push_back(row);
                res_values->insertFrom(*values, offset_it.getValueIndex());
            }
        }
    }

    return ColumnSparse::create(std::move(res_values), std::move(res_offsets), replicate_offsets.back());
}

void ColumnSparse::updateHashWithValue(size_t n, SipHash & hash) const
{
    values->updateHashWithValue(getValueIndex(n), hash);
}

void ColumnSparse::updateWeakHash32(WeakHash32 & hash) const
{
    if (hash.getData().size() != _size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of WeakHash32 does not match size of column: "
        "column size is {}, hash size is {}", _size, hash.getData().size());

    auto offset_it = begin();
    auto & hash_data = hash.getData();
    for (size_t i = 0; i < _size; ++i, ++offset_it)
    {
        size_t value_index = offset_it.getValueIndex();
        auto data_ref = values->getDataAt(value_index);
        hash_data[i] = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(data_ref.data), data_ref.size, hash_data[i]);
    }
}

void ColumnSparse::updateHashFast(SipHash & hash) const
{
    values->updateHashFast(hash);
    offsets->updateHashFast(hash);
    hash.update(_size);
}

void ColumnSparse::getExtremes(Field & min, Field & max) const
{
    if (_size == 0)
    {
        values->get(0, min);
        values->get(0, max);
        return;
    }

    if (getNumberOfDefaults() == 0)
    {
        size_t min_idx = 1;
        size_t max_idx = 1;

        for (size_t i = 2; i < values->size(); ++i)
        {
            if (values->compareAt(i, min_idx, *values, 1) < 0)
                min_idx = i;
            else if (values->compareAt(i, max_idx, *values, 1) > 0)
                max_idx = i;
        }

        values->get(min_idx, min);
        values->get(max_idx, max);
        return;
    }

    values->getExtremes(min, max);
}

void ColumnSparse::getIndicesOfNonDefaultValues(IColumn::Offsets & indices, size_t from, size_t limit) const
{
    const auto & offsets_data = getOffsetsData();
    const auto * start = from ? std::lower_bound(offsets_data.begin(), offsets_data.end(), from) : offsets_data.begin();
    const auto * end = limit ? std::lower_bound(offsets_data.begin(), offsets_data.end(), from + limit) : offsets_data.end();

    indices.assign(start, end);
}

size_t ColumnSparse::getNumberOfDefaultRows(size_t step) const
{
    return getNumberOfDefaults() / step;
}

MutableColumns ColumnSparse::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    return scatterImpl<ColumnSparse>(num_columns, selector);
}

void ColumnSparse::gather(ColumnGathererStream & gatherer_stream)
{
    gatherer_stream.gather(*this);
}

ColumnPtr ColumnSparse::compress() const
{
    auto values_compressed = values->compress();
    auto offsets_compressed = offsets->compress();

    size_t byte_size = values_compressed->byteSize() + offsets_compressed->byteSize();

    return ColumnCompressed::create(size(), byte_size,
        [values_compressed = std::move(values_compressed), offsets_compressed = std::move(offsets_compressed), size = size()]
        {
            return ColumnSparse::create(values_compressed->decompress(), offsets_compressed->decompress(), size);
        });
}

bool ColumnSparse::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_sparse = typeid_cast<const ColumnSparse *>(&rhs))
        return values->structureEquals(*rhs_sparse->values);
    return false;
}

void ColumnSparse::forEachSubcolumn(ColumnCallback callback)
{
    callback(values);
    callback(offsets);
}

const IColumn::Offsets & ColumnSparse::getOffsetsData() const
{
    return assert_cast<const ColumnUInt64 &>(*offsets).getData();
}

IColumn::Offsets & ColumnSparse::getOffsetsData()
{
    return assert_cast<ColumnUInt64 &>(*offsets).getData();
}

size_t ColumnSparse::getValueIndex(size_t n) const
{
    assert(n < _size);

    const auto & offsets_data = getOffsetsData();
    const auto * it = std::lower_bound(offsets_data.begin(), offsets_data.end(), n);
    if (it == offsets_data.end() || *it != n)
        return 0;

    return it - offsets_data.begin() + 1;
}

ColumnPtr recursiveRemoveSparse(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        for (auto & element : columns)
            element = recursiveRemoveSparse(element);

        return ColumnTuple::create(columns);
    }

    return column->convertToFullColumnIfSparse();
}

}
