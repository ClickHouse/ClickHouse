#include <Columns/ColumnMap.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/IColumnImpl.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
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
    auto & map = DB::get<Map &>(res);
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
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insert(const Field & x)
{
    const auto & map = DB::get<const Map &>(x);
    nested->insert(Array(map.begin(), map.end()));
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

void ColumnMap::updateWeakHash32(WeakHash32 & hash) const
{
    nested->updateWeakHash32(hash);
}

void ColumnMap::updateHashFast(SipHash & hash) const
{
    nested->updateHashFast(hash);
}

void ColumnMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
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

int ColumnMap::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto & rhs_map = assert_cast<const ColumnMap &>(rhs);
    return nested->compareAt(n, m, rhs_map.getNestedColumn(), nan_direction_hint);
}

void ColumnMap::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnMap>(assert_cast<const ColumnMap &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

bool ColumnMap::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnMap>();
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

void ColumnMap::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnMap::reserve(size_t n)
{
    nested->reserve(n);
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

    Array nested_min_value = nested_min.get<Array>();
    Array nested_max_value = nested_max.get<Array>();

    Map map_min_value(nested_min_value.begin(), nested_min_value.end());
    Map map_max_value(nested_max_value.begin(), nested_max_value.end());

    min = std::move(map_min_value);
    max = std::move(map_max_value);
}

void ColumnMap::forEachSubcolumn(ColumnCallback callback)
{
    callback(nested);
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
        return nested->structureEquals(*rhs_map->nested);
    return false;
}

double ColumnMap::getRatioOfDefaultRows(double sample_ratio) const
{
    return getRatioOfDefaultRowsImpl<ColumnMap>(sample_ratio);
}

void ColumnMap::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    return getIndicesOfNonDefaultRowsImpl<ColumnMap>(indices, from, limit);
}

ColumnPtr ColumnMap::compress() const
{
    auto compressed = nested->compress();
    const auto byte_size = compressed->byteSize();
    /// The order of evaluation of function arguments is unspecified
    /// and could cause interacting with object in moved-from state
    return ColumnCompressed::create(size(), byte_size, [compressed = std::move(compressed)]
    {
        return ColumnMap::create(compressed->decompress());
    });
}

}
