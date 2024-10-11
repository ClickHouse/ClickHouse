#include <Columns/ColumnTuple.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/IColumnImpl.h>
#include <Core/Field.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Arena.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/iota.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <base/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
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
    if (mutable_columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "This function cannot be used to construct empty tuple. It is a bug");

    columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
    {
        if (isColumnConst(*column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnTuple cannot have ColumnConst as its element");

        columns.push_back(std::move(column));
    }
    column_length = columns[0]->size();
}

ColumnTuple::ColumnTuple(size_t len) : column_length(len) {}

ColumnTuple::Ptr ColumnTuple::create(const Columns & columns)
{
    if (columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "This function cannot be used to construct empty tuple. It is a bug");

    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnTuple cannot have ColumnConst as its element");

    auto column_tuple = ColumnTuple::create(columns[0]->size());
    column_tuple->columns.assign(columns.begin(), columns.end());

    return column_tuple;
}

ColumnTuple::Ptr ColumnTuple::create(const TupleColumns & columns)
{
    if (columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "This function cannot be used to construct empty tuple. It is a bug");

    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnTuple cannot have ColumnConst as its element");

    auto column_tuple = ColumnTuple::create(columns[0]->size());
    column_tuple->columns = columns;

    return column_tuple;
}

MutableColumnPtr ColumnTuple::cloneEmpty() const
{
    if (columns.empty())
        return ColumnTuple::create(0);

    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->cloneEmpty();

    return ColumnTuple::create(std::move(new_columns));
}

MutableColumnPtr ColumnTuple::cloneResized(size_t new_size) const
{
    if (columns.empty())
        return ColumnTuple::create(new_size);

    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->cloneResized(new_size);

    return ColumnTuple::create(std::move(new_columns));
}

size_t ColumnTuple::size() const
{
    if (columns.empty())
        return column_length;

    /// It's difficult to maintain a consistent `column_length` because there
    /// are many places that manipulates sub-columns directly.
    return columns.at(0)->size();
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
    Tuple & res_tuple = res.safeGet<Tuple &>();
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
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {}", getName());
}

void ColumnTuple::insertData(const char *, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertData is not supported for {}", getName());
}

void ColumnTuple::insert(const Field & x)
{
    const auto & tuple = x.safeGet<const Tuple &>();

    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size)
        throw Exception(ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE, "Cannot insert value of different size into tuple");

    ++column_length;
    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insert(tuple[i]);
}

bool ColumnTuple::tryInsert(const Field & x)
{
    if (x.getType() != Field::Types::Which::Tuple)
        return false;

    const auto & tuple = x.safeGet<const Tuple &>();

    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size)
        return false;

    for (size_t i = 0; i < tuple_size; ++i)
    {
        if (!columns[i]->tryInsert(tuple[i]))
        {
            for (size_t j = 0; j != i; ++j)
                columns[j]->popBack(1);

            return false;
        }
    }
    ++column_length;

    return true;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnTuple::insertFrom(const IColumn & src_, size_t n)
#else
void ColumnTuple::doInsertFrom(const IColumn & src_, size_t n)
#endif
{
    const ColumnTuple & src = assert_cast<const ColumnTuple &>(src_);

    const size_t tuple_size = columns.size();
    if (src.columns.size() != tuple_size)
        throw Exception(ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE, "Cannot insert value of different size into tuple");

    ++column_length;
    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insertFrom(*src.columns[i], n);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnTuple::insertManyFrom(const IColumn & src, size_t position, size_t length)
#else
void ColumnTuple::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
#endif
{
    const ColumnTuple & src_tuple = assert_cast<const ColumnTuple &>(src);

    const size_t tuple_size = columns.size();
    if (src_tuple.columns.size() != tuple_size)
        throw Exception(ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE, "Cannot insert value of different size into tuple");

    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insertManyFrom(*src_tuple.columns[i], position, length);
    column_length += length;
}

void ColumnTuple::insertDefault()
{
    ++column_length;
    for (auto & column : columns)
        column->insertDefault();
}

void ColumnTuple::popBack(size_t n)
{
    column_length -= n;
    for (auto & column : columns)
        column->popBack(n);
}

StringRef ColumnTuple::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    if (columns.empty())
    {
        /// Has to put one useless byte into Arena, because serialization into zero number of bytes is ambiguous.
        char * res = arena.allocContinue(1, begin);
        *res = 0;
        return { res, 1 };
    }

    StringRef res(begin, 0);
    for (const auto & column : columns)
    {
        auto value_ref = column->serializeValueIntoArena(n, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}

char * ColumnTuple::serializeValueIntoMemory(size_t n, char * memory) const
{
    for (const auto & column : columns)
        memory = column->serializeValueIntoMemory(n, memory);

    return memory;
}

const char * ColumnTuple::deserializeAndInsertFromArena(const char * pos)
{
    ++column_length;

    if (columns.empty())
        return pos + 1;

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

WeakHash32 ColumnTuple::getWeakHash32() const
{
    auto s = size();
    WeakHash32 hash(s);

    for (const auto & column : columns)
        hash.update(column->getWeakHash32());

    return hash;
}

void ColumnTuple::updateHashFast(SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashFast(hash);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnTuple::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnTuple::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    column_length += length;
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->insertRangeFrom(
            *assert_cast<const ColumnTuple &>(src).columns[i],
            start, length);
}

ColumnPtr ColumnTuple::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (columns.empty())
    {
        size_t bytes = countBytesInFilter(filt);
        return cloneResized(bytes);
    }

    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->filter(filt, result_size_hint);

    return ColumnTuple::create(new_columns);
}

void ColumnTuple::expand(const Filter & mask, bool inverted)
{
    if (columns.empty())
    {
        size_t bytes = countBytesInFilter(mask);
        if (inverted)
            bytes = mask.size() - bytes;
        column_length = bytes;
        return;
    }

    for (auto & column : columns)
        column->expand(mask, inverted);
}

ColumnPtr ColumnTuple::permute(const Permutation & perm, size_t limit) const
{
    if (columns.empty())
    {
        if (column_length != perm.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of permutation doesn't match size of column");

        return cloneResized(limit ? std::min(column_length, limit) : column_length);
    }

    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->permute(perm, limit);

    return ColumnTuple::create(new_columns);
}

ColumnPtr ColumnTuple::index(const IColumn & indexes, size_t limit) const
{
    if (columns.empty())
    {
        if (indexes.size() < limit)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of indexes is less than required");

        return cloneResized(limit ? limit : column_length);
    }

    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->index(indexes, limit);

    return ColumnTuple::create(new_columns);
}

ColumnPtr ColumnTuple::replicate(const Offsets & offsets) const
{
    if (columns.empty())
    {
        if (column_length != offsets.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column");

        return cloneResized(offsets.back());
    }

    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->replicate(offsets);

    return ColumnTuple::create(new_columns);
}

MutableColumns ColumnTuple::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    if (columns.empty())
    {
        if (column_length != selector.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of selector doesn't match size of column");

        std::vector<size_t> counts(num_columns);
        for (auto idx : selector)
            ++counts[idx];

        MutableColumns res(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            res[i] = cloneResized(counts[i]);

        return res;
    }

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

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnTuple::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnTuple::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    return compareAtImpl(n, m, rhs, nan_direction_hint);
}

int ColumnTuple::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint, &collator);
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
            if (res > 0)
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
    iota(res.data(), rows, IColumn::Permutation::value_type(0));

    if (columns.empty())
        return;

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
        while (!equal_ranges.empty() && limit && limit <= equal_ranges.back().from)
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

void ColumnTuple::reserve(size_t n)
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        getColumn(i).reserve(n);
}

size_t ColumnTuple::capacity() const
{
    if (columns.empty())
        return size();

    return getColumn(0).capacity();
}

void ColumnTuple::prepareForSquashing(const Columns & source_columns)
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
    {
        Columns nested_columns;
        nested_columns.reserve(source_columns.size());
        for (const auto & source_column : source_columns)
            nested_columns.push_back(assert_cast<const ColumnTuple &>(*source_column).getColumnPtr(i));
        getColumn(i).prepareForSquashing(nested_columns);
    }
}

void ColumnTuple::shrinkToFit()
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        getColumn(i).shrinkToFit();
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

void ColumnTuple::forEachSubcolumn(MutableColumnCallback callback)
{
    for (auto & column : columns)
        callback(column);
}

void ColumnTuple::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    for (auto & column : columns)
    {
        callback(*column);
        column->forEachSubcolumnRecursively(callback);
    }
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

bool ColumnTuple::hasDynamicStructure() const
{
    for (const auto & column : columns)
    {
        if (column->hasDynamicStructure())
            return true;
    }
    return false;
}

void ColumnTuple::takeDynamicStructureFromSourceColumns(const Columns & source_columns)
{
    std::vector<Columns> nested_source_columns;
    nested_source_columns.resize(columns.size());
    for (size_t i = 0; i != columns.size(); ++i)
        nested_source_columns[i].reserve(source_columns.size());

    for (const auto & source_column : source_columns)
    {
        const auto & nsource_columns = assert_cast<const ColumnTuple &>(*source_column).getColumns();
        for (size_t i = 0; i != nsource_columns.size(); ++i)
            nested_source_columns[i].push_back(nsource_columns[i]);
    }

    for (size_t i = 0; i != columns.size(); ++i)
        columns[i]->takeDynamicStructureFromSourceColumns(nested_source_columns[i]);
}


ColumnPtr ColumnTuple::compress() const
{
    if (columns.empty())
    {
        return ColumnCompressed::create(size(), 0,
            [n = column_length]
            {
                return ColumnTuple::create(n);
            });
    }

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
        [my_compressed = std::move(compressed)]() mutable
        {
            Columns decompressed;
            decompressed.reserve(my_compressed.size());
            for (const auto & column : my_compressed)
                decompressed.push_back(column->decompress());
            return ColumnTuple::create(decompressed);
        });
}

void ColumnTuple::finalize()
{
    for (auto & column : columns)
        column->finalize();
}

bool ColumnTuple::isFinalized() const
{
    return std::all_of(columns.begin(), columns.end(), [](const auto & column) { return column->isFinalized(); });
}

}
