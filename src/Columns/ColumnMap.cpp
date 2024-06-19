#include <algorithm>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnCompressed.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include "Common/Exception.h"
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/WeakHash.h>
#include "Columns/ColumnArray.h"
#include "Columns/ColumnTuple.h"
#include "Columns/IColumn.h"
#include "Functions/GatherUtils/GatherUtils.h"
#include "Functions/GatherUtils/IArraySource.h"
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

ColumnMap::Ptr ColumnMap::create(Columns && columns)
{
    MutableColumns mutable_columns;
    mutable_columns.reserve(columns.size());

    for (const auto & column : columns)
        mutable_columns.emplace_back(column->assumeMutable());

    return create(std::move(mutable_columns));
}

ColumnMap::Ptr ColumnMap::create(const ColumnPtr & keys, const ColumnPtr & values, const ColumnPtr & offsets)
{
    auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{keys, values}), offsets);
    return ColumnMap::create(std::move(nested_column));
}

ColumnMap::MutablePtr ColumnMap::create(MutableColumnPtr && column)
{
    MutableColumns columns;
    columns.emplace_back(std::move(column));
    return create(std::move(columns));
}

std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    const auto & nested_tuple = getNestedData();
    res << "Map(" << nested_tuple.getColumn(0).getName()
        << ", " << nested_tuple.getColumn(1).getName() << ")";

    return res.str();
}

ColumnMap::ColumnMap(MutableColumns && shards_)
{
    if (shards_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Map must have at least one shard");

    for (auto & shard : shards_)
    {
        const auto * column_array = typeid_cast<const ColumnArray *>(shard.get());
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

        shards.emplace_back(std::move(shard));
    }
}

template <typename F>
MutableColumns ColumnMap::applyForShards(F && f) const
{
    MutableColumns new_shards;
    new_shards.reserve(shards.size());
    for (const auto & shard : shards)
        new_shards.push_back(f(shard)->assumeMutable());
    return new_shards;
}

std::vector<ColumnMap::WrappedPtr> ColumnMap::cloneEmptyShards() const
{
    std::vector<WrappedPtr> new_shards;
    new_shards.reserve(shards.size());

    for (const auto & shard : shards)
        new_shards.push_back(shard->cloneEmpty());

    return new_shards;
}

void ColumnMap::concatToOneShard(std::vector<WrappedPtr> && shard_sources, IColumn & res)
{
    size_t num_shards = shard_sources.size();
    std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources(num_shards);

    for (size_t i = 0; i < num_shards; ++i)
    {
        const auto & shard_array = assert_cast<const ColumnArray &>(*shard_sources[i]);
        sources[i] = GatherUtils::createArraySource(shard_array, false, shard_array.size());
    }

    auto & res_array = assert_cast<ColumnArray &>(res);
    GatherUtils::concatInplace(sources, res_array);
}

template <bool one_value, typename Inserter>
void ColumnMap::insertRangeImpl(const ColumnMap & src, Inserter && inserter)
{
    size_t num_shards = getNumShards();
    size_t num_src_shards = src.getNumShards();

    if (num_shards == num_src_shards)
    {
        for (size_t i = 0; i < num_shards; ++i)
            inserter(*shards[i], *src.shards[i]);
    }
    else if (num_shards == 1)
    {
        if constexpr (one_value)
        {
            for (size_t i = 0; i < num_src_shards; ++i)
                inserter(*shards[0], *src.shards[i]);
        }
        else
        {
            auto tmp_shards = src.cloneEmptyShards();
            for (size_t i = 0; i < num_src_shards; ++i)
                inserter(*tmp_shards[i], *src.shards[i]);

            concatToOneShard(std::move(tmp_shards), *shards[0]);
        }
    }
    else
    {
        /// TODO: fix
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implememnted");
    }
}

MutableColumnPtr ColumnMap::cloneEmpty() const
{
    return ColumnMap::create(applyForShards([](const auto & shard)
    {
        return shard->cloneEmpty();
    }));
}

MutableColumnPtr ColumnMap::cloneResized(size_t new_size) const
{
    return ColumnMap::create(applyForShards([new_size](const auto & shard)
    {
        return shard->cloneResized(new_size);
    }));
}

Field ColumnMap::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnMap::get(size_t n, Field & res) const
{
    res = Map();
    auto & map = res.get<Map &>();

    for (const auto & shard : shards)
    {
        const auto & offsets = assert_cast<const ColumnArray &>(*shard).getOffsets();

        size_t offset = offsets[n - 1];
        size_t size = offsets[n] - offsets[n - 1];

        for (size_t i = 0; i < size; ++i)
            map.push_back(getNestedData()[offset + i]);
    }
}

bool ColumnMap::isDefaultAt(size_t n) const
{
    return std::ranges::all_of(shards, [n](const auto & shard) { return shard->isDefaultAt(n); });
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
    const auto & map = x.get<const Map &>();

    if (shards.size() == 1)
    {
        shards[0]->insert(Array(map.begin(), map.end()));
        return;
    }

    auto tmp_column = ColumnMap::create(shards[0]->cloneEmpty());
    tmp_column->insert(x);
    insertFrom(*tmp_column, 0);
}

bool ColumnMap::tryInsert(const Field & x)
{
    if (x.getType() != Field::Types::Which::Map)
        return false;

    insert(x);
    return true;
}

void ColumnMap::insertDefault()
{
    for (auto & shard : shards)
        shard->insertDefault();
}
void ColumnMap::popBack(size_t n)
{
    for (auto & shard : shards)
        shard->popBack(n);
}

StringRef ColumnMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    /// TODO: fix
    return shards[0]->serializeValueIntoArena(n, arena, begin);
}

char * ColumnMap::serializeValueIntoMemory(size_t n, char * memory) const
{
    /// TODO: fix
    return shards[0]->serializeValueIntoMemory(n, memory);
}

const char * ColumnMap::deserializeAndInsertFromArena(const char * pos)
{
    /// TODO: fix
    return shards[0]->deserializeAndInsertFromArena(pos);
}

const char * ColumnMap::skipSerializedInArena(const char * pos) const
{
    /// TODO: fix
    return shards[0]->skipSerializedInArena(pos);
}

void ColumnMap::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (const auto & shard : shards)
        shard->updateHashWithValue(n, hash);
}

void ColumnMap::updateWeakHash32(WeakHash32 & hash) const
{
    for (const auto & shard : shards)
        shard->updateWeakHash32(hash);
}

void ColumnMap::updateHashFast(SipHash & hash) const
{
    for (const auto & shard : shards)
        shard->updateHashFast(hash);
}

void ColumnMap::insertFrom(const IColumn & src, size_t n)
{
    insertRangeImpl<true>(assert_cast<const ColumnMap &>(src), [&](IColumn & dest, const IColumn & source)
    {
        dest.insertFrom(source, n);
    });
}

void ColumnMap::insertManyFrom(const IColumn & src, size_t position, size_t length)
{
    insertRangeImpl<true>(assert_cast<const ColumnMap &>(src), [&](IColumn & dest, const IColumn & source)
    {
        dest.insertManyFrom(source, position, length);
    });
}

void ColumnMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    return insertRangeImpl<false>(assert_cast<const ColumnMap &>(src), [&](IColumn & dest, const IColumn & source)
    {
        dest.insertRangeFrom(source, start, length);
    });
}

ColumnPtr ColumnMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    return ColumnMap::create(applyForShards([&](const auto & shard)
    {
        return shard->filter(filt, result_size_hint);
    }));
}

void ColumnMap::expand(const IColumn::Filter & mask, bool inverted)
{
    for (auto & shard : shards)
        shard->expand(mask, inverted);
}

ColumnPtr ColumnMap::permute(const Permutation & perm, size_t limit) const
{
    return ColumnMap::create(applyForShards([&](const auto & shard)
    {
        return shard->permute(perm, limit);
    }));
}

ColumnPtr ColumnMap::index(const IColumn & indexes, size_t limit) const
{
    return ColumnMap::create(applyForShards([&](const auto & shard)
    {
        return shard->index(indexes, limit);
    }));
}

ColumnPtr ColumnMap::replicate(const Offsets & offsets) const
{
    return ColumnMap::create(applyForShards([&](const auto & shard)
    {
        return shard->replicate(offsets);
    }));
}

MutableColumns ColumnMap::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    std::vector<MutableColumns> new_shards(num_columns);

    for (const auto & shard : shards)
    {
        auto scattered_shards = shard->scatter(num_columns, selector);

        for (size_t i = 0; i < num_columns; ++i)
            new_shards[i].push_back(std::move(scattered_shards[i]));
    }

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        res[i] = ColumnMap::create(std::move(new_shards[i]));
    return res;
}

int ColumnMap::compareAt(size_t, size_t, const IColumn &, int) const
{
    return 0;
}

void ColumnMap::getPermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability, size_t, int, IColumn::Permutation & res) const
{
    size_t s = shards[0]->size();
    res.resize_exact(s);
    iota(res.data(), s, IColumn::Permutation::value_type(0));
}

void ColumnMap::updatePermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability, size_t, int, IColumn::Permutation &, EqualRanges &) const
{
}

void ColumnMap::reserve(size_t n)
{
    for (auto & shard : shards)
        shard->reserve(n);
}

void ColumnMap::shrinkToFit()
{
    for (auto & shard : shards)
        shard->shrinkToFit();
}

void ColumnMap::ensureOwnership()
{
    for (auto & shard : shards)
        shard->ensureOwnership();
}

size_t ColumnMap::byteSize() const
{
    size_t res = 0;
    for (const auto & shard : shards)
        res += shard->byteSize();
    return res;
}

size_t ColumnMap::byteSizeAt(size_t n) const
{
    size_t res = 0;
    for (const auto & shard : shards)
        res += shard->byteSizeAt(n);
    return res;
}

size_t ColumnMap::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & shard : shards)
        res += shard->allocatedBytes();
    return res;
}

void ColumnMap::protect()
{
    for (auto & shard : shards)
        shard->protect();
}

void ColumnMap::getExtremes(Field & min, Field & max) const
{
    Field nested_min;
    Field nested_max;

    for (const auto & shard : shards)
        shard->getExtremes(nested_min, nested_max);

    /// Convert result Array fields to Map fields because client expect min and max field to have type Map

    Array nested_min_value = nested_min.get<Array>();
    Array nested_max_value = nested_max.get<Array>();

    Map map_min_value(nested_min_value.begin(), nested_min_value.end());
    Map map_max_value(nested_max_value.begin(), nested_max_value.end());

    min = std::move(map_min_value);
    max = std::move(map_max_value);
}

void ColumnMap::forEachSubcolumn(MutableColumnCallback callback)
{
    for (auto & shard : shards)
        callback(shard);
}

void ColumnMap::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    for (auto & shard : shards)
    {
        callback(*shard);
        shard->forEachSubcolumnRecursively(callback);
    }
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
        return shards[0]->structureEquals(*rhs_map->shards[0]);
    return false;
}

ColumnPtr ColumnMap::finalize() const
{
    if (isFinalized())
        return getPtr();

    std::vector<WrappedPtr> finalized_shards;
    finalized_shards.reserve(shards.size());

    for (const auto & shard : shards)
        finalized_shards.push_back(shard->finalize());

    auto res = shards[0]->cloneEmpty();
    concatToOneShard(std::move(finalized_shards), *res);
    return ColumnMap::create(std::move(res));
}

bool ColumnMap::isFinalized() const
{
    return shards.size() == 1 && shards[0]->isFinalized();
}

ColumnPtr ColumnMap::compress() const
{
    std::vector<ColumnPtr> compressed;
    size_t byte_size = 0;

    for (const auto & shard : shards)
    {
        compressed.emplace_back(shard->compress());
        byte_size += compressed.back()->byteSize();
    }

    /// The order of evaluation of function arguments is unspecified
    /// and could cause interacting with object in moved-from state
    return ColumnCompressed::create(size(), byte_size, [my_compressed = std::move(compressed)]
    {
        MutableColumns decompressed;
        for (const auto & shard : my_compressed)
            decompressed.emplace_back(shard->decompress()->assumeMutable());
        return ColumnMap::create(std::move(decompressed));
    });
}

void ColumnMap::takeDynamicStructureFromSourceColumns(const Columns & source_columns)
{
    /// TODO: fix
    Columns nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        nested_source_columns.push_back(assert_cast<const ColumnMap &>(*source_column).getNestedColumnPtr());
    shards[0]->takeDynamicStructureFromSourceColumns(nested_source_columns);
}

}
