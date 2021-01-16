#include <Columns/ColumnMap.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumnImpl.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <ext/map.h>
#include <ext/range.h>

#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

#include <boost/smart_ptr/make_shared.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

class ColumnMap::IIndex
{
public:
    virtual ~IIndex() = default;

    virtual IColumn::Ptr findAll(const IColumn & needles, size_t rows_count) const = 0;

    // updates the index with N last rows of the key/value columns.
    virtual void afterAppend(size_t N) = 0;
    virtual void beforeRemove(size_t N) = 0;

    // Completely rebuilds an index.
    virtual void rebuild() = 0;
};

}

namespace
{
using namespace DB;

const auto * logger()
{
    static const auto & logger = Poco::Logger::get("ColumnMap");
    return &logger;
}

template <typename KeyColumnType, typename KeyStorageType>
class ColumnMapIndex : public ColumnMap::IIndex
{
    // Columns are max 0xFFFF items, so array on each row is max 0xFFFF items long, which is more than enough.
    using MappedType = UInt32;

    template <typename KeyType>
    using HashMapTypeSelector = std::conditional_t<std::is_same_v<KeyType, StringRef>,
        StringHashMap<MappedType>,
        HashMap<KeyStorageType, MappedType, DefaultHash<KeyStorageType>, HashTableGrower<>>
    >;

    using HashMapType = HashMapTypeSelector<typename KeyColumnType::ValueType>;

    const KeyColumnType & keys_column;
    const IColumn & values_column;
    const ColumnArray::Offsets & offsets;

    // One hashtables per row is much more performant (less collisions)
    // than one huge HT and much more convenient in terms of building keys - no need to mess with
    // compound (row+key_value) keys and inventing hash functions.
    std::vector<HashMapType> hash_tables;

    mutable std::atomic<size_t> find_queries = 0;
    mutable std::atomic<size_t> lookup_hits = 0;
    mutable std::atomic<size_t> lookup_misses = 0;
    mutable std::atomic<size_t> find_total_microseconds = 0;
    mutable std::atomic<size_t> index_rebuilds = 0;
    mutable std::atomic<size_t> index_rebuilds_microseconds = 0;
    mutable std::atomic<size_t> items_added = 0;
    mutable std::atomic<size_t> items_removed = 0;

public:

    ColumnMapIndex(const IColumn & keys_column_, const IColumn & values_column_, const ColumnArray::Offsets & offsets_)
        : ColumnMapIndex(*assert_cast<const KeyColumnType*>(&keys_column_), values_column_, offsets_)
    {}

    ColumnMapIndex(const KeyColumnType & keys_column_, const IColumn & values_column_, const ColumnArray::Offsets & offsets_)
        : keys_column(keys_column_)
        , values_column(values_column_)
        , offsets(offsets_)
    {
        addRowsToIndex(0, offsets.size());
    }

    ~ColumnMapIndex() override
    {
        if (hash_tables.size() > 0 || find_queries > 0)
        {
            LOG_DEBUG(logger(), "Index({} => {}) 0x{} Destroying index, final stats"
                    "\n\thashtables                  : {}"
                    "\n\ttotal keys                  : {}"
                    "\n\ttotal find queries          : {}"
                    "\n\t   key lookup hits          : {}"
                    "\n\t   key lookup misses        : {}"
                    "\n\ttotal find time             : {}us"
                    "\n\tavg find time               : {}us per find query"
                    "\n\tavg lookup time             : {}us per lookup"
                    "\n\tavg lookups                 : {} per find query"
                    "\n\ttotal index full rebuilds   : {}"
                    "\n\tavg index full rebuild time : {}us per full rebuild"
                    "\n\ttotal items added           : {}"
                    "\n\ttotal items removed         : {}"
                    , keys_column.getName(), values_column.getName(), static_cast<const void*>(this)
                    , hash_tables.size()
                    , keys_column.size()
                    , find_queries
                    , lookup_hits
                    , lookup_misses
                    , find_total_microseconds
                    , static_cast<double>(find_total_microseconds) / find_queries
                    , static_cast<double>(find_total_microseconds) / (lookup_hits + lookup_misses)
                    , static_cast<double>(lookup_hits + lookup_misses) / find_queries
                    , index_rebuilds
                    , static_cast<double>(index_rebuilds_microseconds) / index_rebuilds
                    , items_added
                    , items_removed);
        }
    }

    // updates the index with N last rows of the key/value columns.
    void afterAppend(size_t number_of_items) override
    {
        addRowsToIndex(offsets.size() - number_of_items, offsets.size());
        items_added += number_of_items;
    }

    void beforeRemove(size_t number_of_items) override
    {
        hash_tables.erase(hash_tables.end() - number_of_items, hash_tables.end());
        items_removed += number_of_items;
    }

    // Completely rebuilds an index.
    void rebuild() override
    {
        Stopwatch stopwatch;
        stopwatch.start();

        addRowsToIndex(0, offsets.size());

        index_rebuilds_microseconds += stopwatch.elapsedMicroseconds();
        ++index_rebuilds;
    }

    void addRowsToIndex(size_t start_row, size_t end_row)
    {
//        LOG_DEBUG(logger(), "Index({} => {}) 0x{} Adding items to index: {} to {} on column with {}:{}:{} items"
//                  "\n\tkeys col: {}, vals col: {}, offsets: {}"
//                  , keys_column.getName(), values_column.getName(), static_cast<const void*>(this)
//                  , start_row, end_row
//                  , keys_column.size(), values_column.size(), offsets.size()
//                  , reinterpret_cast<const void*>(&keys_column)
//                  , reinterpret_cast<const void*>(&values_column)
//                  , reinterpret_cast<const void*>(&offsets));

        hash_tables.reserve(hash_tables.size() + end_row - start_row);

        // Build an index, store global key offset as mapped values since that greatly simplifies value extraction.
        size_t starting_offset = start_row == 0 ? 0 : offsets[start_row - 1];
        size_t total_items = 0;

        for (size_t row = start_row; row < end_row; ++row)
        {
            const size_t final_offset = offsets[row];

            HashMapType map;

            if constexpr (!std::is_same_v<KeyStorageType, StringRef>)
            {
                map.reserve(final_offset - starting_offset);
            }

            for (size_t i = starting_offset; i < final_offset; ++i)
            {
                typename HashMapType::LookupResult it;
                bool inserted;
                // Casting allows narrower types like UInt8 and UInt16 to be matched against UInt64,
                // i.e. UInt64(1) and UInt8(1) would have same hash.
                map.emplace(static_cast<KeyStorageType>(keys_column.getElement(i)), it, inserted);
                if (inserted)
                    it->getMapped() = static_cast<MappedType>(i);
            }
            total_items += final_offset - starting_offset;
            starting_offset = final_offset;

            hash_tables.emplace_back(std::move(map));
        }
    }

    template <typename NeedleColumnType>
    struct VectorColumnValueExtractor
    {
        const NeedleColumnType & needle_col;

        VectorColumnValueExtractor(const NeedleColumnType & needle_col_)
            : needle_col(needle_col_)
        {}

        inline KeyStorageType getElement(size_t row) const
        {
            return needle_col.getElement(row);
        }
    };

    template <typename NeedleColumnType>
    struct ConstColumnValueExtractor
    {
        const KeyStorageType value;
        ConstColumnValueExtractor(const NeedleColumnType & needle_col)
            : value(static_cast<KeyStorageType>(needle_col.getElement(0)))
        {}

        inline KeyStorageType getElement(size_t /*row*/) const
        {
            return value;
        }
    };

    IColumn::Ptr findAll(const IColumn & needles, size_t rows_count) const override
    {
        if (needles.size() > hash_tables.size())
            throw Exception(fmt::format("There are more needles ({}) than rows in index ({})", needles.size(), hash_tables.size()),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (const auto * needles_column = checkAndGetColumn<ColumnConst>(needles))
        {
            auto find_const = [this](const auto & typed_needles_column, size_t rows_count_) -> auto
            {
                return this->findAllTyped<ConstColumnValueExtractor>(typed_needles_column, rows_count_);
            };
            return findDispatch(find_const, needles_column->getDataColumn(), rows_count);
        }
        else
        {
            auto find_vector = [this](const auto & typed_needles_column, size_t rows_count_) -> auto
            {
                return this->findAllTyped<VectorColumnValueExtractor>(typed_needles_column, rows_count_);
            };
            return findDispatch(find_vector, needles, rows_count);
        }
    }

    template <typename FindFunction>
    IColumn::Ptr findDispatch(FindFunction find_function, const IColumn & needles, size_t rows_count) const
    {
        if constexpr (std::is_same_v<KeyStorageType, StringRef>)
        {
            if (const auto * needles_column = checkAndGetColumn<ColumnString>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnFixedString>(needles))
                return find_function(*needles_column, rows_count);
        }
        else //if constexpr (std::is_unsigned_v<KeyStorageType>)
        {
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<UInt8>>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<UInt16>>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<UInt32>>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<UInt64>>(needles))
                return find_function(*needles_column, rows_count);
//        }
//        else if constexpr (std::is_signed_v<KeyStorageType>)
//        {
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<Int8>>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<Int16>>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<Int32>>(needles))
                return find_function(*needles_column, rows_count);
            if (const auto * needles_column = checkAndGetColumn<ColumnVector<Int64>>(needles))
                return find_function(*needles_column, rows_count);
        }

        throw Exception(fmt::format("Unsupported needle type for Map: {} expected {}",
                needles.getName(), keys_column.getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <template <typename> typename ValueExtractor, typename NeedleColumnType>
    IColumn::Ptr findAllTyped(const NeedleColumnType & needles, size_t rows_count) const
    {
        const ValueExtractor<NeedleColumnType> values(needles);

        ++find_queries;

        size_t misses = 0;
        size_t hits = 0;

        auto result = values_column.cloneEmpty();
        result->reserve(needles.size());

        Stopwatch stopwatch;
        stopwatch.start();

        for (size_t row = 0; row < rows_count; ++row)
        {
            auto res = hash_tables[row].find(static_cast<KeyStorageType>(values.getElement(row)));
            if (res)
            {
                result->insertFrom(values_column, res->getMapped());
                ++hits;
            }
            else
            {
                result->insertDefault();
                ++misses;
            }
        }

        stopwatch.stop();
        find_total_microseconds += stopwatch.elapsedMicroseconds();
        lookup_hits += hits;
        lookup_misses += misses;
        return result;
    }
};

boost::shared_ptr<ColumnMap::IIndex> makeIndex(const IColumn & keys, const IColumn & values_column, const ColumnArray::Offsets & offsets)
{
    const IColumn * keys_column = &keys;
    if (const auto * nullable_keys = checkAndGetColumn<ColumnNullable>(keys))
    {
        keys_column = &nullable_keys->getNestedColumn();
    }

    switch (keys_column->getDataType())
    {
        case TypeIndex::UInt8:
            return boost::make_shared<ColumnMapIndex<ColumnVector<UInt8>, UInt64>>(*keys_column, values_column, offsets);
        case TypeIndex::UInt16:
            return boost::make_shared<ColumnMapIndex<ColumnVector<UInt16>, UInt64>>(*keys_column, values_column, offsets);
        case TypeIndex::UInt32:
            return boost::make_shared<ColumnMapIndex<ColumnVector<UInt32>, UInt64>>(*keys_column, values_column, offsets);
        case TypeIndex::UInt64:
            return boost::make_shared<ColumnMapIndex<ColumnVector<UInt64>, UInt64>>(*keys_column, values_column, offsets);
////        case TypeIndex::UInt128:
////            return boost::make_shared<ColumnMapIndex<ColumnVector<UInt128>, UInt128>>(*keys_column, values_column, offsets);
////        case TypeIndex::UInt256:
        case TypeIndex::Int8:
            return boost::make_shared<ColumnMapIndex<ColumnVector<Int8>, Int64>>(*keys_column, values_column, offsets);
        case TypeIndex::Int16:
            return boost::make_shared<ColumnMapIndex<ColumnVector<Int16>, Int64>>(*keys_column, values_column, offsets);
        case TypeIndex::Int32:
            return boost::make_shared<ColumnMapIndex<ColumnVector<Int32>, Int64>>(*keys_column, values_column, offsets);
        case TypeIndex::Int64:
            return boost::make_shared<ColumnMapIndex<ColumnVector<Int64>, Int64>>(*keys_column, values_column, offsets);
////        case TypeIndex::Int128:
////        case TypeIndex::Int256:
// TODO: since most of Floats are not going to be binary equal, it might not make any sense to store those in a hashmap
//        case TypeIndex::Float32:
//            return boost::make_shared<ColumnMapIndex<ColumnVector<Float32>, Float64>>(*keys_column, values_column, offsets);
//        case TypeIndex::Float64:
//            return boost::make_shared<ColumnMapIndex<ColumnVector<Float64>, Float64>>(*keys_column, values_column, offsets);
////        case TypeIndex::Date:
////        case TypeIndex::DateTime:
////        case TypeIndex::DateTime64:
        case TypeIndex::String:
            return boost::make_shared<ColumnMapIndex<ColumnString, StringRef>>(*keys_column, values_column, offsets);
        case TypeIndex::FixedString:
            return boost::make_shared<ColumnMapIndex<ColumnFixedString, StringRef>>(*keys_column, values_column, offsets);
        case TypeIndex::Enum8:
            return boost::make_shared<ColumnMapIndex<ColumnVector<Int8>, Int64>>(*keys_column, values_column, offsets);
        case TypeIndex::Enum16:
            return boost::make_shared<ColumnMapIndex<ColumnVector<Int16>, Int64>>(*keys_column, values_column, offsets);
////        case TypeIndex::Decimal32:
////        case TypeIndex::Decimal64:
////        case TypeIndex::Decimal128:
////        case TypeIndex::Decimal256:
////        case TypeIndex::UUID:
    default:
        throw Exception("Unsupported KEY column of Map " + keys_column->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

}

namespace DB
{

std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    const auto & nested_tuple = getNestedData();
    res << "Map(" << nested_tuple.getColumn(0).getName()
        << ", " << nested_tuple.getColumn(1).getName() << ")";

    return res.str();
}

ColumnMap::ColumnMap(MutableColumnPtr && nested_)
    : nested(std::move(nested_)),
      key_index(nullptr)
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

ColumnMap::ColumnMap(const ColumnMap & other)
    : nested(other.nested),
      key_index(nullptr)
{}

ColumnMap::~ColumnMap() = default;

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
    auto array = DB::get<Array>((*nested)[n]);
    return Map(std::make_move_iterator(array.begin()), std::make_move_iterator(array.end()));
}

void ColumnMap::get(size_t n, Field & res) const
{
    const auto & offsets = getNestedColumn().getOffsets();
    size_t offset = offsets[n - 1];
    size_t size = offsets[n] - offsets[n - 1];

    res = Map(size);
    auto & map = DB::get<Map &>(res);

    for (size_t i = 0; i < size; ++i)
        getNestedData().get(offset + i, map[i]);
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

    if (auto index = key_index.load())
        index->afterAppend(1);
}

void ColumnMap::insertDefault()
{
    nested->insertDefault();

    if (auto index = key_index.load())
        index->afterAppend(1);
}

void ColumnMap::popBack(size_t n)
{
    LOG_DEBUG(logger(), "{} : {}", reinterpret_cast<const void*>(this), __FUNCTION__);
    if (auto index = key_index.load())
        index->beforeRemove(n);

    nested->popBack(n);
}

StringRef ColumnMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return nested->serializeValueIntoArena(n, arena, begin);
}

const char * ColumnMap::deserializeAndInsertFromArena(const char * pos)
{
    const char * result = nested->deserializeAndInsertFromArena(pos);
    key_index.store(nullptr);

    return result;
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

    if (auto index = key_index.load())
        index->afterAppend(length);
}

ColumnPtr ColumnMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    auto filtered = nested->filter(filt, result_size_hint);
    return ColumnMap::create(filtered);
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

void ColumnMap::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    nested->getPermutation(reverse, limit, nan_direction_hint, res);
}

void ColumnMap::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_range) const
{
    nested->updatePermutation(reverse, limit, nan_direction_hint, res, equal_range);
}

void ColumnMap::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
    key_index.store(nullptr);
}

void ColumnMap::reserve(size_t n)
{
    nested->reserve(n);
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
    nested->getExtremes(min, max);
}

void ColumnMap::forEachSubcolumn(ColumnCallback callback)
{
    nested->forEachSubcolumn(callback);

    key_index.store(nullptr);
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
        return nested->structureEquals(*rhs_map->nested);
    return false;
}

ColumnArray & ColumnMap::getNestedColumn()
{
    key_index.store(nullptr);
    return assert_cast<ColumnArray &>(*nested);
}

ColumnTuple & ColumnMap::getNestedData()
{
    key_index.store(nullptr);
    return assert_cast<ColumnTuple &>(getNestedColumn().getData());
}


ColumnPtr ColumnMap::findAll(const IColumn & keys, size_t rows_count) const
{
    if (unlikely(rows_count > nested->size() || rows_count > keys.size()))
        throw Exception("Too many rows for ColumnMap", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (key_index.load() == nullptr)
    {
        std::lock_guard lock(key_index_mutex);
        if (key_index.load() == nullptr)
        {
            key_index.store(makeIndex(getNestedData().getColumn(0), getNestedData().getColumn(1), getNestedColumn().getOffsets()));
        }
    }

    return key_index.load()->findAll(keys, rows_count);
}

}
