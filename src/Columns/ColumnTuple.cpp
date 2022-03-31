#include <Columns/ColumnTuple.h>

#include <base/sort.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnCompressed.h>
#include <Core/Field.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
    extern const int LOGICAL_ERROR;
}


std::string ColumnTuple::getName() const
{
    WriteBufferFromOwnString res;
    res << "Tuple(";
    bool is_first = true;
    for (const auto & column : columns)
    {
        if (!is_first)
            res << ", ";
        is_first = false;
        res << column->getName();
    }
    res << ")";
    return res.str();
}

ColumnTuple::ColumnTuple(MutableColumns && mutable_columns)
{
    columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
    {
        if (isColumnConst(*column))
            throw Exception{"ColumnTuple cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

        columns.push_back(std::move(column));
    }
}

ColumnTuple::Ptr ColumnTuple::create(const Columns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{"ColumnTuple cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

    auto column_tuple = ColumnTuple::create(MutableColumns());
    column_tuple->columns.assign(columns.begin(), columns.end());

    return column_tuple;
}

ColumnTuple::Ptr ColumnTuple::create(const TupleColumns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{"ColumnTuple cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

    auto column_tuple = ColumnTuple::create(MutableColumns());
    column_tuple->columns = columns;

    return column_tuple;
}

MutableColumnPtr ColumnTuple::cloneEmpty() const
{
    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->cloneEmpty();

    return ColumnTuple::create(std::move(new_columns));
}

MutableColumnPtr ColumnTuple::cloneResized(size_t new_size) const
{
    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->cloneResized(new_size);

    return ColumnTuple::create(std::move(new_columns));
}

Field ColumnTuple::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnTuple::get(size_t n, Field & res) const
{
    const size_t tuple_size = columns.size();

    res = Tuple();
    Tuple & res_tuple = DB::get<Tuple &>(res);
    res_tuple.reserve(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        res_tuple.push_back((*columns[i])[n]);
}

bool ColumnTuple::isDefaultAt(size_t n) const
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        if (!columns[i]->isDefaultAt(n))
            return false;
    return true;
}

StringRef ColumnTuple::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnTuple::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnTuple::insert(const Field & x)
{
    const auto & tuple = DB::get<const Tuple &>(x);

    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size)
        throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insert(tuple[i]);
}

void ColumnTuple::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnTuple & src = assert_cast<const ColumnTuple &>(src_);

    const size_t tuple_size = columns.size();
    if (src.columns.size() != tuple_size)
        throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insertFrom(*src.columns[i], n);
}

void ColumnTuple::insertDefault()
{
    for (auto & column : columns)
        column->insertDefault();
}

void ColumnTuple::popBack(size_t n)
{
    for (auto & column : columns)
        column->popBack(n);
}

StringRef ColumnTuple::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    StringRef res(begin, 0);
    for (const auto & column : columns)
    {
        auto value_ref = column->serializeValueIntoArena(n, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}

const char * ColumnTuple::deserializeAndInsertFromArena(const char * pos)
{
    for (auto & column : columns)
        pos = column->deserializeAndInsertFromArena(pos);

    return pos;
}

const char * ColumnTuple::skipSerializedInArena(const char * pos) const
{
    for (const auto & column : columns)
        pos = column->skipSerializedInArena(pos);

    return pos;
}

void ColumnTuple::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashWithValue(n, hash);
}

void ColumnTuple::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    for (const auto & column : columns)
        column->updateWeakHash32(hash);
}

void ColumnTuple::updateHashFast(SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashFast(hash);
}

void ColumnTuple::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insertRangeFrom(
            *assert_cast<const ColumnTuple &>(src).columns[i],
            start, length);
}

ColumnPtr ColumnTuple::filter(const Filter & filt, ssize_t result_size_hint) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->filter(filt, result_size_hint);

    return ColumnTuple::create(new_columns);
}

void ColumnTuple::expand(const Filter & mask, bool inverted)
{
    for (auto & column : columns)
        column->expand(mask, inverted);
}

ColumnPtr ColumnTuple::permute(const Permutation & perm, size_t limit) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->permute(perm, limit);

    return ColumnTuple::create(new_columns);
}

ColumnPtr ColumnTuple::index(const IColumn & indexes, size_t limit) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->index(indexes, limit);

    return ColumnTuple::create(new_columns);
}

ColumnPtr ColumnTuple::replicate(const Offsets & offsets) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->replicate(offsets);

    return ColumnTuple::create(new_columns);
}

MutableColumns ColumnTuple::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    const size_t tuple_size = columns.size();
    std::vector<MutableColumns> scattered_tuple_elements(tuple_size);

    for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx)
        scattered_tuple_elements[tuple_element_idx] = columns[tuple_element_idx]->scatter(num_columns, selector);

    MutableColumns res(num_columns);

    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
    {
        MutableColumns new_columns(tuple_size);
        for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx)
            new_columns[tuple_element_idx] = std::move(scattered_tuple_elements[tuple_element_idx][scattered_idx]);
        res[scattered_idx] = ColumnTuple::create(std::move(new_columns));
    }

    return res;
}

int ColumnTuple::compareAtImpl(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator * collator) const
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
    {
        int res;
        if (collator && columns[i]->isCollationSupported())
            res = columns[i]->compareAtWithCollation(n, m, *assert_cast<const ColumnTuple &>(rhs).columns[i], nan_direction_hint, *collator);
        else
            res = columns[i]->compareAt(n, m, *assert_cast<const ColumnTuple &>(rhs).columns[i], nan_direction_hint);
        if (res)
            return res;
    }
    return 0;
}

int ColumnTuple::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint);
}

void ColumnTuple::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnTuple>(assert_cast<const ColumnTuple &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

int ColumnTuple::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint, &collator);
}

bool ColumnTuple::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnTuple>();
}

template <bool positive>
struct ColumnTuple::Less
{
    TupleColumns columns;
    int nan_direction_hint;
    const Collator * collator;

    Less(const TupleColumns & columns_, int nan_direction_hint_, const Collator * collator_=nullptr)
        : columns(columns_), nan_direction_hint(nan_direction_hint_), collator(collator_)
    {
    }

    bool operator() (size_t a, size_t b) const
    {
        for (const auto & column : columns)
        {
            int res;
            if (collator && column->isCollationSupported())
                res = column->compareAtWithCollation(a, b, *column, nan_direction_hint, *collator);
            else
                res = column->compareAt(a, b, *column, nan_direction_hint);
            if (res < 0)
                return positive;
            else if (res > 0)
                return !positive;
        }
        return false;
    }
};

void ColumnTuple::getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator) const
{
    size_t rows = size();
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;

    if (limit >= rows)
        limit = 0;

    EqualRanges ranges;
    ranges.emplace_back(0, rows);
    updatePermutationImpl(direction, stability, limit, nan_direction_hint, res, ranges, collator);
}

void ColumnTuple::updatePermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges, const Collator * collator) const
{
    if (equal_ranges.empty())
        return;

    for (const auto & column : columns)
    {
        while (!equal_ranges.empty() && limit && limit <= equal_ranges.back().first)
            equal_ranges.pop_back();

        if (collator && column->isCollationSupported())
            column->updatePermutationWithCollation(*collator, direction, stability, limit, nan_direction_hint, res, equal_ranges);
        else
            column->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);

        if (equal_ranges.empty())
            break;
    }
}

void ColumnTuple::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, nan_direction_hint, res, nullptr);
}

void ColumnTuple::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    updatePermutationImpl(direction, stability, limit, nan_direction_hint, res, equal_ranges);
}

void ColumnTuple::getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, nan_direction_hint, res, &collator);
}

void ColumnTuple::updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    updatePermutationImpl(direction, stability, limit, nan_direction_hint, res, equal_ranges, &collator);
}

void ColumnTuple::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnTuple::reserve(size_t n)
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        getColumn(i).reserve(n);
}

void ColumnTuple::ensureOwnership()
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        getColumn(i).ensureOwnership();
}

size_t ColumnTuple::byteSize() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSize();
    return res;
}

size_t ColumnTuple::byteSizeAt(size_t n) const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSizeAt(n);
    return res;
}

size_t ColumnTuple::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();
    return res;
}

void ColumnTuple::protect()
{
    for (auto & column : columns)
        column->protect();
}

void ColumnTuple::getExtremes(Field & min, Field & max) const
{
    const size_t tuple_size = columns.size();

    Tuple min_tuple(tuple_size);
    Tuple max_tuple(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->getExtremes(min_tuple[i], max_tuple[i]);

    min = min_tuple;
    max = max_tuple;
}

void ColumnTuple::forEachSubcolumn(ColumnCallback callback)
{
    for (auto & column : columns)
        callback(column);
}

bool ColumnTuple::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_tuple = typeid_cast<const ColumnTuple *>(&rhs))
    {
        const size_t tuple_size = columns.size();
        if (tuple_size != rhs_tuple->columns.size())
            return false;

        for (size_t i = 0; i < tuple_size; ++i)
            if (!columns[i]->structureEquals(*rhs_tuple->columns[i]))
                return false;

        return true;
    }
    else
        return false;
}

bool ColumnTuple::isCollationSupported() const
{
    for (const auto & column : columns)
    {
        if (column->isCollationSupported())
            return true;
    }
    return false;
}


ColumnPtr ColumnTuple::compress() const
{
    size_t byte_size = 0;
    Columns compressed;
    compressed.reserve(columns.size());
    for (const auto & column : columns)
    {
        auto compressed_column = column->compress();
        byte_size += compressed_column->byteSize();
        compressed.emplace_back(std::move(compressed_column));
    }

    return ColumnCompressed::create(size(), byte_size,
        [compressed = std::move(compressed)]() mutable
        {
            for (auto & column : compressed)
                column = column->decompress();
            return ColumnTuple::create(compressed);
        });
}

double ColumnTuple::getRatioOfDefaultRows(double sample_ratio) const
{
    return getRatioOfDefaultRowsImpl<ColumnTuple>(sample_ratio);
}

void ColumnTuple::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    return getIndicesOfNonDefaultRowsImpl<ColumnTuple>(indices, from, limit);
}

SerializationInfoPtr ColumnTuple::getSerializationInfo() const
{
    MutableSerializationInfos infos;
    infos.reserve(columns.size());

    for (const auto & column : columns)
        infos.push_back(const_pointer_cast<SerializationInfo>(column->getSerializationInfo()));

    return std::make_shared<SerializationInfoTuple>(std::move(infos), SerializationInfo::Settings{});
}

}
