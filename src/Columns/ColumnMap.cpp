#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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

#include <Common/HashTable/HashMap.h>
#include <Common/UInt128.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

//class ColumnMap::IIndex
//{
//public:
//    virtual ~IIndex() {}

//    virtual IColumn::Ptr findAll(const IColumn & needles, size_t rows_count) const = 0;

//    // updates the index with N last rows of the key/value columns.
//    virtual void afterAppend(size_t N) = 0;
//    virtual void beforeRemove(size_t N) = 0;

//    // Completely rebuilds an index.
//    virtual void reset() = 0;
//};

struct MapKey
{
    size_t row = 0;
    StringRef data = {};

    inline bool operator==(const MapKey & other) const
    {
        return row == other.row && data == other.data;
    }
};

struct MapKeyHash
{
    inline size_t operator()(const MapKey & map_key) const
    {
//        return StringRefHash()(map_key.data);
        return UInt128Hash()(UInt128{map_key.row, StringRefHash()(map_key.data)});
    }
};

template <typename T>
inline bool areEqual(const T & left, const T & right)
{
    return left == right;
}

template <typename Key, typename TMapped, typename Hash, typename TState = HashTableNoState, bool (*KeyEqualityOp)(const Key &, const Key &) = &areEqual>
struct HashMapCellWithKeyEquality : public HashMapCell<Key, TMapped, Hash, TState>
{
    using Base = HashMapCell<Key, TMapped, Hash, TState>;
    using Base::Base;

    bool keyEquals(const Key & key_) const { return KeyEqualityOp(this->value.first, key_); }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return KeyEqualityOp(this->value.first, key_); }
    bool keyEquals(const Key & key_, size_t hash_, const typename Base::State &) const { return keyEquals(key_, hash_); }
};

template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
using HashMap2 = HashMapTable<Key, HashMapCellWithKeyEquality<Key, Mapped, Hash>, Hash, Grower, Allocator>;

}


namespace ZeroTraits
{

template <>
inline bool check<DB::MapKey>(const DB::MapKey x)
{
    return x.data.data == nullptr;
//    return x.row == 0 && x.data.data == nullptr;

//    static const DB::MapKey zero_key;
//    return memcmp(&x, &zero_key, sizeof(x)) == 0;
}
template <>
inline void set<DB::MapKey>(DB::MapKey & x) { memset(&x, 0, sizeof(x)); }

}

namespace
{
using namespace DB;

//class ColumnMapIndexBase : public DB::ColumnMap::IIndex
//{
//protected:
//    const ColumnArray & keys_column;
//    const ColumnArray & values_column;

//    ColumnMapIndexBase(const ColumnArray & keys_column_, const ColumnArray & values_column_)
//        : keys_column(keys_column_),
//          values_column(values_column_)
//    {}


//    void validateNeedlesType(const IColumn & needles) const
//    {
//    }
//};

//template <typename ColumnVectorType, typename KeyStorageType>
//class ColumnVectorIndex : public DB::ColumnMap::IIndex
//{
//public:
//    // Assert cast keys are of given ColumnVector<T> specialization, also make sure that T size is <= KeyStorageSize
//    ColumnVectorIndex(const IColumn & keys, const IColumn & values)
//    {

//    }

//    // updates the index with N last rows of the key/value columns.
//    void afterAppend(size_t N) override;
//    void beforeRemove(size_t N) override;

//    // Completely rebuilds an index.
//    void reset() override;
//};
}

namespace DB
{

class ColumnMap::Index
{
    using MapIndex = HashMap2<MapKey, UInt64, MapKeyHash>;
    const ColumnArray & keys_column;
    const ColumnArray & values_column;
    MapIndex index;

public:
    Index(const ColumnArray & keys_column_, const ColumnArray & values_column_)
        : keys_column(keys_column_),
          values_column(values_column_)
    {
        const auto & map_values_offsets = values_column.getOffsets();
        const auto & map_keys_offsets = keys_column.getOffsets();
        const auto & map_keys_data = keys_column.getData();

        index.reserve(map_values_offsets.size());

        // Build an index, store global key offset as mapped values since that greatly simplifies value extraction.
        size_t starting_offset = 0;
        for (size_t row = 0; row < keys_column.size(); ++row)
        {
            if (map_keys_offsets[row] != map_values_offsets[row])
                throw Exception(
                        fmt::format("Different number of elements in key ({}) and value ({}) arrays on row {}.",
                                map_keys_offsets[row], map_values_offsets[row], row),
                        ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

            const size_t final_offset = map_keys_offsets[row];
            for (size_t i = starting_offset; i < final_offset; ++i)
            {
                MapIndex::LookupResult it;
                bool inserted;
                const auto key_data = map_keys_data.getDataAt(i);

                index.emplace(MapKey{row, key_data}, it, inserted);
                if (inserted)
                    new (&it->getMapped()) UInt64(i);
            }
            starting_offset = final_offset;
        }
    }

    inline IColumn::Ptr findAll(const IColumn & needles, size_t rows_count) const
    {
        {
            const IColumn & needle_type_column = isColumnConst(needles)
                    ? typeid_cast<const ColumnConst&>(needles).getDataColumn() : needles;

            if (!keys_column.getData().structureEquals(needle_type_column))
                throw Exception("Incompatitable needle column type " + needle_type_column.getName() + " expected " + keys_column.getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        // TODO: make sure to convert needles-value to values-type to avoid
        // false negative when searching for UInt64(1) while Map is keyed in UInt32.

        const auto & nested_values = values_column.getData();
        auto result = nested_values.cloneEmpty();
        result->reserve(needles.size());

        for (size_t row = 0; row < rows_count; ++row)
        {
            const auto & key = MapKey{row, needles.getDataAt(row)};
            const auto hash = MapKeyHash{}(key);
            if (auto res = index.find(key, hash))
                result->insertFrom(nested_values, res->getMapped());
            else
                result->insertDefault();
        }

        return result;
    }
};


std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    res << "Map(" << columns[0]->getName() << ", " << columns[1]->getName() << ")";
    return res.str();
}

ColumnMap::ColumnMap(MutableColumns && mutable_columns)
    : key_index(std::make_shared<Index>(
        assert_cast<const ColumnArray &>(*mutable_columns[0]),
        assert_cast<const ColumnArray &>(*mutable_columns[1])))
{
    columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
    {
        if (isColumnConst(*column))
            throw Exception{"ColumnMap cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

        columns.push_back(std::move(column));
    }
}

ColumnMap::ColumnMap(const ColumnMap & other)
    : columns(other.columns),
      key_index(std::make_shared<Index>(
            assert_cast<const ColumnArray &>(*columns[0]),
            assert_cast<const ColumnArray &>(*columns[1])))
{}

ColumnMap::~ColumnMap()
{}

ColumnMap::Ptr ColumnMap::create(const Columns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{"ColumnMap cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

    MutableColumns map_columns(2);
    for (size_t i = 0; i < 2; ++i)
        map_columns[i] = columns[i]->assumeMutable();

    return Base::create(std::move(map_columns));
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


ColumnPtr ColumnMap::findAll(const IColumn & keys, size_t rows_count) const
{
    if (unlikely(rows_count > columns[0]->size() || rows_count > keys.size()))
        throw Exception("Too many rows for ColumnMap", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (key_index == nullptr)
    {
//        std::lock_guard<std::mutex> lock(key_index_mutex);
        if (key_index == nullptr)
        {
            const ColumnArray & keys_array = assert_cast<const ColumnArray &>(*columns[0]);
            const ColumnArray & values_array = assert_cast<const ColumnArray &>(*columns[1]);
            std::make_shared<Index>(keys_array, values_array).swap(key_index);
        }
    }

    return key_index->findAll(keys, rows_count);
}

}
