#include <Columns/ColumnMap.h>
#include <Columns/ColumnCompressed.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
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
    extern const int LOGICAL_ERROR;
}


std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    const auto & nested_tuple = getNestedData();
    res << "Map(" << nested_tuple.getColumn(0).getName()
        << ", " << nested_tuple.getColumn(1).getName() << ")";

    return res.str();
}

ColumnMap::ColumnMap(MutableColumnPtr && nested_)
    : nested(std::move(nested_))
{
    const auto * column_array = typeid_cast<const ColumnArray *>(nested.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMap can be created only from array of tuples");

    const auto * column_tuple = typeid_cast<const ColumnTuple *>(column_array->getDataPtr().get());
    if (!column_tuple)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMap can be created only from array of tuples");

    if (column_tuple->getColumns().size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMap should contain only 2 subcolumns: keys and values");

    for (const auto & column : column_tuple->getColumns())
        if (isColumnConst(*column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnMap cannot have ColumnConst as its element");
}

MutableColumnPtr ColumnMap::cloneEmpty() const
{
    return ColumnMap::create(nested->cloneEmpty());
}

MutableColumnPtr ColumnMap::cloneResized(size_t new_size) const
{
    return ColumnMap::create(nested->cloneResized(new_size));
}

Field ColumnMap::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnMap::get(size_t n, Field & res) const
{
    const auto & offsets = getNestedColumn().getOffsets();
    size_t offset = offsets[n - 1];
    size_t size = offsets[n] - offsets[n - 1];

    res = Map();
    auto & map = res.safeGet<Map &>();
    map.reserve(size);

    for (size_t i = 0; i < size; ++i)
        map.push_back(getNestedData()[offset + i]);
}

bool ColumnMap::isDefaultAt(size_t n) const
{
    return nested->isDefaultAt(n);
}

StringRef ColumnMap::getDataAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {}", getName());
}

void ColumnMap::insertData(const char *, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertData is not supported for {}", getName());
}

void ColumnMap::insert(const Field & x)
{
    const auto & map = x.safeGet<const Map &>();
    nested->insert(Array(map.begin(), map.end()));
}

bool ColumnMap::tryInsert(const Field & x)
{
    if (x.getType() != Field::Types::Which::Map)
        return false;

    const auto & map = x.safeGet<const Map &>();
    return nested->tryInsert(Array(map.begin(), map.end()));
}

void ColumnMap::insertDefault()
{
    nested->insertDefault();
}
void ColumnMap::popBack(size_t n)
{
    nested->popBack(n);
}

StringRef ColumnMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return nested->serializeValueIntoArena(n, arena, begin);
}

char * ColumnMap::serializeValueIntoMemory(size_t n, char * memory) const
{
    return nested->serializeValueIntoMemory(n, memory);
}

const char * ColumnMap::deserializeAndInsertFromArena(const char * pos)
{
    return nested->deserializeAndInsertFromArena(pos);
}

const char * ColumnMap::skipSerializedInArena(const char * pos) const
{
    return nested->skipSerializedInArena(pos);
}

void ColumnMap::updateHashWithValue(size_t n, SipHash & hash) const
{
    nested->updateHashWithValue(n, hash);
}

WeakHash32 ColumnMap::getWeakHash32() const
{
    return nested->getWeakHash32();
}

void ColumnMap::updateHashFast(SipHash & hash) const
{
    nested->updateHashFast(hash);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnMap::insertFrom(const IColumn & src, size_t n)
#else
void ColumnMap::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    nested->insertFrom(assert_cast<const ColumnMap &>(src).getNestedColumn(), n);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnMap::insertManyFrom(const IColumn & src, size_t position, size_t length)
#else
void ColumnMap::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
#endif
{
    assert_cast<ColumnArray &>(*nested).insertManyFrom(assert_cast<const ColumnMap &>(src).getNestedColumn(), position, length);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnMap::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    nested->insertRangeFrom(
        assert_cast<const ColumnMap &>(src).getNestedColumn(),
        start, length);
}

ColumnPtr ColumnMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    auto filtered = nested->filter(filt, result_size_hint);
    return ColumnMap::create(filtered);
}

void ColumnMap::expand(const IColumn::Filter & mask, bool inverted)
{
    nested->expand(mask, inverted);
}

ColumnPtr ColumnMap::permute(const Permutation & perm, size_t limit) const
{
    auto permuted = nested->permute(perm, limit);
    return ColumnMap::create(std::move(permuted));
}

ColumnPtr ColumnMap::index(const IColumn & indexes, size_t limit) const
{
    auto res = nested->index(indexes, limit);
    return ColumnMap::create(std::move(res));
}

ColumnPtr ColumnMap::replicate(const Offsets & offsets) const
{
    auto replicated = nested->replicate(offsets);
    return ColumnMap::create(std::move(replicated));
}

MutableColumns ColumnMap::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    auto scattered_columns = nested->scatter(num_columns, selector);
    MutableColumns res;
    res.reserve(num_columns);
    for (auto && scattered : scattered_columns)
        res.push_back(ColumnMap::create(std::move(scattered)));

    return res;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnMap::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnMap::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    const auto & rhs_map = assert_cast<const ColumnMap &>(rhs);
    return nested->compareAt(n, m, rhs_map.getNestedColumn(), nan_direction_hint);
}

void ColumnMap::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                            size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    nested->getPermutation(direction, stability, limit, nan_direction_hint, res);
}

void ColumnMap::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    nested->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
}

void ColumnMap::reserve(size_t n)
{
    nested->reserve(n);
}

size_t ColumnMap::capacity() const
{
    return nested->capacity();
}

void ColumnMap::prepareForSquashing(const Columns & source_columns)
{
    Columns nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        nested_source_columns.push_back(assert_cast<const ColumnMap &>(*source_column).getNestedColumnPtr());
    nested->prepareForSquashing(nested_source_columns);
}

void ColumnMap::shrinkToFit()
{
    nested->shrinkToFit();
}

void ColumnMap::ensureOwnership()
{
    nested->ensureOwnership();
}

size_t ColumnMap::byteSize() const
{
    return nested->byteSize();
}

size_t ColumnMap::byteSizeAt(size_t n) const
{
    return nested->byteSizeAt(n);
}

size_t ColumnMap::allocatedBytes() const
{
    return nested->allocatedBytes();
}

void ColumnMap::protect()
{
    nested->protect();
}

void ColumnMap::getExtremes(Field & min, Field & max) const
{
    Field nested_min;
    Field nested_max;

    nested->getExtremes(nested_min, nested_max);

    /// Convert result Array fields to Map fields because client expect min and max field to have type Map

    Array nested_min_value = nested_min.safeGet<Array>();
    Array nested_max_value = nested_max.safeGet<Array>();

    Map map_min_value(nested_min_value.begin(), nested_min_value.end());
    Map map_max_value(nested_max_value.begin(), nested_max_value.end());

    min = std::move(map_min_value);
    max = std::move(map_max_value);
}

void ColumnMap::forEachSubcolumn(MutableColumnCallback callback)
{
    callback(nested);
}

void ColumnMap::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*nested);
    nested->forEachSubcolumnRecursively(callback);
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
        return nested->structureEquals(*rhs_map->nested);
    return false;
}

ColumnPtr ColumnMap::compress() const
{
    auto compressed = nested->compress();
    const auto byte_size = compressed->byteSize();
    /// The order of evaluation of function arguments is unspecified
    /// and could cause interacting with object in moved-from state
    return ColumnCompressed::create(size(), byte_size, [my_compressed = std::move(compressed)]
    {
        return ColumnMap::create(my_compressed->decompress());
    });
}

void ColumnMap::takeDynamicStructureFromSourceColumns(const Columns & source_columns)
{
    Columns nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        nested_source_columns.push_back(assert_cast<const ColumnMap &>(*source_column).getNestedColumnPtr());
    nested->takeDynamicStructureFromSourceColumns(nested_source_columns);
}

}
