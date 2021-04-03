#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

    if (offsets->size() + 1 != values->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Values size is inconsistent with offsets size. Expected: {}, got {}", offsets->size() + 1, values->size());
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
    values->get(n, res);
}

bool ColumnSparse::getBool(size_t n) const
{
    return values->getBool(getValueIndex(n));
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
    return values->createWithOffsets(getOffsetsData(), _size);
}

void ColumnSparse::insertData(const char * pos, size_t length)
{
    _size += length;
    return values->insertData(pos, length);
}

StringRef ColumnSparse::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return values->serializeValueIntoArena(getValueIndex(n), arena, begin);
}

const char * ColumnSparse::deserializeAndInsertFromArena(const char * pos)
{
    UNUSED(pos);
    throwMustBeDense();
}

const char * ColumnSparse::skipSerializedInArena(const char *) const
{
    throwMustBeDense();
}

void ColumnSparse::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    if (start + length >= src.size())
        throw Exception("Parameter out of bound in IColumnString::insertRangeFrom method.",
            ErrorCodes::LOGICAL_ERROR);

    auto & offsets_data = getOffsetsData();

    size_t end = start + length;
    if (const auto * src_sparse = typeid_cast<const ColumnSparse *>(&src))
    {
        const auto & src_offsets = src_sparse->getOffsetsData();
        const auto & src_values = src_sparse->getValuesColumn();

        if (!src_offsets.empty())
        {
            size_t offset_start = std::lower_bound(src_offsets.begin(), src_offsets.end(), start) - src_offsets.begin();
            size_t offset_end = std::upper_bound(src_offsets.begin(), src_offsets.end(), end) - src_offsets.begin();
            if (offset_end != 0)
                --offset_end;

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

            insertManyDefaults(end - src_offsets[offset_end]);
            values->insertRangeFrom(src_values, offset_start + 1, offset_end - offset_start + 1);
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
    while(!offsets_data.empty() && offsets_data.back() >= new_size)
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

    const auto & offsets_data = getOffsetsData();

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();

    Filter values_filter;
    values_filter.reserve(values->size());
    values_filter.push_back(1);
    size_t values_result_size_hint = 1;

    size_t offset_pos = 0;
    size_t res_offset = 0;

    for (size_t i = 0; i < _size; ++i)
    {
        if (offset_pos < offsets_data.size() && i == offsets_data[offset_pos])
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

            ++offset_pos;
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

    auto res_values = values->cloneEmpty();
    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();
    res_values->insertDefault();

    for (size_t i = 0; i < limit; ++i)
    {
        size_t index = getValueIndex(perm[i]);
        if (index != 0)
        {
            res_values->insertFrom(*values, index);
            res_offsets_data.push_back(i);
        }
    }

    return ColumnSparse::create(std::move(res_values), std::move(res_offsets), limit);
}

ColumnPtr ColumnSparse::index(const IColumn & indexes, size_t limit) const
{
    UNUSED(indexes);
    UNUSED(limit);

    throwMustBeDense();
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
    UNUSED(rhs);
    UNUSED(rhs_row_num);
    UNUSED(row_indexes);
    UNUSED(compare_results);
    UNUSED(direction);
    UNUSED(nan_direction_hint);

    throwMustBeDense();
}

int ColumnSparse::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator &) const
{
    UNUSED(n);
    UNUSED(m);
    UNUSED(rhs);
    UNUSED(null_direction_hint);

    throwMustBeDense();
}

bool ColumnSparse::hasEqualValues() const
{
    return offsets->size() == 0;
}

void ColumnSparse::getPermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res) const
{
    UNUSED(reverse);
    UNUSED(limit);
    UNUSED(null_direction_hint);
    UNUSED(res);

    throwMustBeDense();
}

void ColumnSparse::updatePermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_range) const
{
    UNUSED(reverse);
    UNUSED(null_direction_hint);
    UNUSED(limit);
    UNUSED(res);
    UNUSED(equal_range);

    throwMustBeDense();
}

void ColumnSparse::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int null_direction_hint, Permutation & res) const
{
    UNUSED(collator);
    UNUSED(reverse);
    UNUSED(limit);
    UNUSED(null_direction_hint);
    UNUSED(res);

    throwMustBeDense();
}

void ColumnSparse::updatePermutationWithCollation(
    const Collator & collator, bool reverse, size_t limit, int null_direction_hint, Permutation & res, EqualRanges& equal_range) const
{
    UNUSED(collator);
    UNUSED(reverse);
    UNUSED(limit);
    UNUSED(null_direction_hint);
    UNUSED(res);
    UNUSED(equal_range);

    throwMustBeDense();
}

void ColumnSparse::reserve(size_t)
{
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
    throwMustBeDense();
}

ColumnPtr ColumnSparse::replicate(const Offsets & replicate_offsets) const
{
    UNUSED(replicate_offsets);
    throwMustBeDense();
}

void ColumnSparse::updateHashWithValue(size_t n, SipHash & hash) const
{
    UNUSED(n);
    UNUSED(hash);
    throwMustBeDense();
}

void ColumnSparse::updateWeakHash32(WeakHash32 & hash) const
{
    UNUSED(hash);
    throwMustBeDense();
}

void ColumnSparse::updateHashFast(SipHash & hash) const
{
    UNUSED(hash);
    throwMustBeDense();
}

void ColumnSparse::getExtremes(Field & min, Field & max) const
{
    UNUSED(min);
    UNUSED(max);
    throwMustBeDense();
}

void ColumnSparse::getIndicesOfNonDefaultValues(IColumn::Offsets & indices, size_t from, size_t limit) const
{
    const auto & offsets_data = getOffsetsData();
    auto start = from ? std::lower_bound(offsets_data.begin(), offsets_data.end(), from) : offsets_data.begin();
    auto end = limit ? std::lower_bound(offsets_data.begin(), offsets_data.end(), from + limit) : offsets_data.end();

    indices.assign(start, end);
}

size_t ColumnSparse::getNumberOfDefaultRows(size_t step) const
{
    return (_size - offsets->size()) / step;
}

MutableColumns ColumnSparse::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    UNUSED(num_columns);
    UNUSED(selector);
    throwMustBeDense();
}

void ColumnSparse::gather(ColumnGathererStream & gatherer_stream)
{
    UNUSED(gatherer_stream);
    throwMustBeDense();
}

ColumnPtr ColumnSparse::compress() const
{
    throwMustBeDense();
}

bool ColumnSparse::structureEquals(const IColumn & rhs) const
{
    UNUSED(rhs);
    throwMustBeDense();
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
    auto it = std::lower_bound(offsets_data.begin(), offsets_data.end(), n);
    if (it == offsets_data.end() || *it != n)
        return 0;

    return it - offsets_data.begin() + 1;
}

}
