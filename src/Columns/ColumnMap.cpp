#include <Columns/ColumnMap.h>
#include <Columns/IColumnImpl.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <ext/map.h>
#include <ext/range.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/WeakHash.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
    extern const int LOGICAL_ERROR;
}


std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    res << "Map(" << columns[0]->getName() << ", " << columns[1]->getName() << ")";
    return res.str();
}

ColumnMap::ColumnMap(MutableColumns && mutable_columns)
{
    columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
    {
        if (isColumnConst(*column))
            throw Exception{"ColumnMap cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

        columns.push_back(std::move(column));
    }
}

ColumnMap::Ptr ColumnMap::create(const Columns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{"ColumnMap cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

    auto column_map = ColumnMap::create(MutableColumns());
    column_map->columns.assign(columns.begin(), columns.end());

    return column_map;
}

ColumnMap::Ptr ColumnMap::create(const MapColumns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{"ColumnMap cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

    auto column_map = ColumnMap::create(MutableColumns());
    column_map->columns = columns;

    return column_map;
}

MutableColumnPtr ColumnMap::cloneEmpty() const
{
    MutableColumns new_columns(2);
    for (size_t i = 0; i < 2; ++i)
        new_columns[i] = columns[i]->cloneEmpty();

    return ColumnMap::create(std::move(new_columns));
}

MutableColumnPtr ColumnMap::cloneResized(size_t new_size) const
{
    MutableColumns new_columns(2);
    for (size_t i = 0; i < 2; ++i)
        new_columns[i] = columns[i]->cloneResized(new_size);

    return ColumnMap::create(std::move(new_columns));
}

Field ColumnMap::operator[](size_t n) const
{
    return ext::map<Map>(columns, [n] (const auto & column) { return (*column)[n]; });
}

void ColumnMap::get(size_t n, Field & res) const
{
    Map map(2);
    columns[0]->get(n, map[0]);
    columns[1]->get(n, map[1]);

    res = map;
}

StringRef ColumnMap::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insert(const Field & x)
{
    const auto & map = DB::get<const Map &>(x);

    if (map.size() != 2)
        throw Exception("Cannot insert value of different size into map", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < 2; ++i)
        columns[i]->insert(map[i]);
}

void ColumnMap::insertDefault()
{
    for (auto & column : columns)
        column->insertDefault();
}
void ColumnMap::popBack(size_t n)
{
    for (auto & column : columns)
        column->popBack(n);
}

StringRef ColumnMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
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

const char * ColumnMap::deserializeAndInsertFromArena(const char * pos)
{
    for (auto & column : columns)
        pos = column->deserializeAndInsertFromArena(pos);

    return pos;
}

void ColumnMap::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashWithValue(n, hash);
}

void ColumnMap::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    for (const auto & column : columns)
        column->updateWeakHash32(hash);
}

void ColumnMap::updateHashFast(SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashFast(hash);
}

void ColumnMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    for (size_t i = 0; i < 2; ++i)
        columns[i]->insertRangeFrom(
            *assert_cast<const ColumnMap &>(src).columns[i],
            start, length);
}

ColumnPtr ColumnMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    Columns new_columns(2);

    for (size_t i = 0; i < 2; ++i)
        new_columns[i] = columns[i]->filter(filt, result_size_hint);

    return ColumnMap::create(new_columns);
}

ColumnPtr ColumnMap::permute(const Permutation & perm, size_t limit) const
{
    Columns new_columns(2);

    for (size_t i = 0; i < 2; ++i)
        new_columns[i] = columns[i]->permute(perm, limit);

    return ColumnMap::create(new_columns);
}

ColumnPtr ColumnMap::index(const IColumn & indexes, size_t limit) const
{
    Columns new_columns(2);

    for (size_t i = 0; i < 2; ++i)
        new_columns[i] = columns[i]->index(indexes, limit);

    return ColumnMap::create(new_columns);
}

ColumnPtr ColumnMap::replicate(const Offsets & offsets) const
{
    Columns new_columns(2);

    for (size_t i = 0; i < 2; ++i)
        new_columns[i] = columns[i]->replicate(offsets);

    return ColumnMap::create(new_columns);
}

MutableColumns ColumnMap::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    std::vector<MutableColumns> scattered_map_elements(2);

    for (size_t map_element_idx = 0; map_element_idx < 2; ++map_element_idx)
        scattered_map_elements[map_element_idx] = columns[map_element_idx]->scatter(num_columns, selector);

    MutableColumns res(num_columns);

    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
    {
        MutableColumns new_columns(2);
        for (size_t map_element_idx = 0; map_element_idx < 2; ++map_element_idx)
            new_columns[map_element_idx] = std::move(scattered_map_elements[map_element_idx][scattered_idx]);
        res[scattered_idx] = ColumnMap::create(std::move(new_columns));
    }

    return res;
}

int ColumnMap::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    for (size_t i = 0; i < 2; ++i)
        if (int res = columns[i]->compareAt(n, m, *assert_cast<const ColumnMap &>(rhs).columns[i], nan_direction_hint))
            return res;

    return 0;
}

void ColumnMap::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnMap>(assert_cast<const ColumnMap &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

template <bool positive>
struct ColumnMap::Less
{
    MapColumns columns;
    int nan_direction_hint;

    Less(const MapColumns & columns_, int nan_direction_hint_)
        : columns(columns_), nan_direction_hint(nan_direction_hint_)
    {
    }

    bool operator() (size_t a, size_t b) const
    {
        for (const auto & column : columns)
        {
            int res = column->compareAt(a, b, *column, nan_direction_hint);
            if (res < 0)
                return positive;
            else if (res > 0)
                return !positive;
        }
        return false;
    }
};

void ColumnMap::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    size_t rows = size();
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;

    if (limit >= rows)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<false>(columns, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<true>(columns, nan_direction_hint));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), Less<false>(columns, nan_direction_hint));
        else
            std::sort(res.begin(), res.end(), Less<true>(columns, nan_direction_hint));
    }
}

void ColumnMap::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_range) const
{
    for (const auto& column : columns)
    {
        column->updatePermutation(reverse, limit, nan_direction_hint, res, equal_range);
        while (limit && !equal_range.empty() && limit <= equal_range.back().first)
            equal_range.pop_back();

        if (equal_range.empty())
            break;
    }
}

void ColumnMap::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnMap::reserve(size_t n)
{
    for (size_t i = 0; i < 2; ++i)
        getColumn(i).reserve(n);
}

size_t ColumnMap::byteSize() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSize();
    return res;
}

size_t ColumnMap::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();
    return res;
}

void ColumnMap::protect()
{
    for (auto & column : columns)
        column->protect();
}

void ColumnMap::getExtremes(Field & min, Field & max) const
{
    Map min_map(2);
    Map max_map(2);

    columns[0]->getExtremes(min_map[0], max_map[0]);
    columns[1]->getExtremes(min_map[1], max_map[1]);

    min = min_map;
    max = max_map;
}

void ColumnMap::forEachSubcolumn(ColumnCallback callback)
{
    for (auto & column : columns)
        callback(column);
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
    {
        if (rhs_map->columns.size() != 2)
            return false;

        for (const auto i : ext::range(0, 2))
            if (!columns[i]->structureEquals(*rhs_map->columns[i]))
                return false;

        return true;
    }
    else
        return false;
}

}
