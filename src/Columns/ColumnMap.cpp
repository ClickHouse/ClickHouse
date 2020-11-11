#include <Columns/ColumnMap.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
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

class ColumnMap::IIndex
{
public:
    virtual ~IIndex() {}

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

struct SharedArenaWithFreeListsAllocator
{
    ArenaWithFreeLists * shared_allocator = nullptr;

    inline void * alloc(size_t size, size_t /*alignment*/ = 0)
    {
        return shared_allocator->alloc(size);
    }

    /// Free memory range.
    inline void free(void * buf, size_t size)
    {
        shared_allocator->free(reinterpret_cast<char *>(buf), size);
    }

    inline void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        return shared_allocator->realloc(buf, old_size, new_size, alignment);
    }
};

template <typename KeyColumnType, typename KeyStorageType>
class ColumnMapIndex : public ColumnMap::IIndex
{
    // Columns are max 0xFFFF items, so array on each row is max 0xFFFF items long, which is more than enough.
    using MappedType = UInt32;

    template <typename KeyType, typename AllocatorType>
    using HashMapTypeSelector = std::conditional_t<std::is_same_v<KeyType, StringRef>,
        StringHashMap<MappedType, AllocatorType>,
        HashMap<KeyStorageType, MappedType, DefaultHash<KeyStorageType>, HashTableGrower<>, AllocatorType>
    >;

    using HashMapType = HashMapTypeSelector<typename KeyColumnType::ValueType, SharedArenaWithFreeListsAllocator>;

    const ColumnArray & keys_column;
    const ColumnArray & values_column;
    ArenaWithFreeLists arena;
    std::vector<HashMapType> index;

    mutable std::atomic<size_t> find_queries = 0;
    mutable std::atomic<size_t> find_hits = 0;
    mutable std::atomic<size_t> find_misses = 0;
    mutable std::atomic<size_t> index_lookups = 0;
    mutable std::atomic<size_t> find_total_nanoseconds = 0;
    size_t index_rebuilds = 0;

public:

    ColumnMapIndex(const ColumnArray & keys_column_, const ColumnArray & values_column_)
        : keys_column(keys_column_),
          values_column(values_column_),
          arena((keys_column.getData().size() * sizeof(typename HashMapType::cell_type)) * 1.5)
    {
        std::cerr << " !!! " << static_cast<const void*>(this) << " Arena initial size " << arena.size() << std::endl;
        rebuild();
    }

    ~ColumnMapIndex() override
    {
        if (index.size() > 0 || find_queries > 0)
        {
            std::cerr << " !!! " << static_cast<const void*>(this) << " Destroying the ColumnMapIndex, final stats:"
                      << "\n\tindex rows     : " << index.size()
                      << "\n\tsearches : " << find_queries
                      << "\n\t  hits   : " << find_hits
                      << "\n\t  misses : " << find_misses
                      << "\n\ttotal find time : " << find_total_nanoseconds << "ns"
                      << "\n\tavg find (per row) time: " << static_cast<double>(find_total_nanoseconds)/index_lookups << "ns/index lookup"
                      << "\n\tavg lookups per search : " << static_cast<double>(index_lookups)/find_queries
                      << "\n\tindex rebuilds : " << index_rebuilds
                      << std::endl;
        }
    }

    // updates the index with N last rows of the key/value columns.
    void afterAppend(size_t N) override
    {
        rebuild(keys_column.size() - N, keys_column.size());
    }

    void beforeRemove(size_t N) override
    {
        index.erase(index.end() - N, index.end());
        ++index_rebuilds;
    }

    // Completely rebuilds an index.
    void rebuild() override
    {
        rebuild(0, keys_column.size());
    }

    void rebuild(size_t start_row, size_t end_row)
    {
        const auto & keys_data = assert_cast<const KeyColumnType &>(keys_column.getData());
        const auto & keys_offsets = keys_column.getOffsets();
        const auto & values_offsets = values_column.getOffsets();

        const auto started_at = std::chrono::system_clock::now();
        std::cerr << " !!! " << static_cast<const void*>(this)
                  << " Rebuilding ColumnMapIndex of "
                  << keys_data.getName() << " => " << values_column.getData().getName()
                  << " : " << start_row << " to " << end_row << std::endl;

        index.reserve(index.size() + end_row - start_row);

        // Build an index, store global key offset as mapped values since that greatly simplifies value extraction.
        size_t starting_offset = start_row == 0 ? 0 : keys_offsets[start_row - 1];
        size_t total_items = 0;
        for (size_t row = start_row; row < end_row; ++row)
        {
            if (keys_offsets[row] != values_offsets[row])
                throw Exception(
                        fmt::format("Different number of elements in key ({}) and value ({}) arrays on row {}.",
                                keys_offsets[row], values_offsets[row], row),
                        ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

            const size_t final_offset = keys_offsets[row];

            HashMapType map(SharedArenaWithFreeListsAllocator{&arena});

            if constexpr (!std::is_same_v<KeyStorageType, StringRef>)
            {
                map.reserve(final_offset - starting_offset);
            }

            for (size_t i = starting_offset; i < final_offset; ++i)
            {
                typename HashMapType::LookupResult it;
                bool inserted;
                // Casting to allow narrower types like UInt8 and UInt16 to be matched against UInt64,
                // i.e. UInt64(1) and UInt8(1) would have same hash.
                map.emplace(static_cast<KeyStorageType>(keys_data.getElement(i)), it, inserted);
                if (inserted)
                    it->getMapped() = static_cast<MappedType>(i);
            }
            total_items += final_offset - starting_offset;
            starting_offset = final_offset;

            index.emplace_back(std::move(map));
        }

        std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - started_at;
        std::cerr << " !!! " << static_cast<const void*>(this)
                  << " Rebuilt ColumnMapIndex with total " << total_items
                  << " hash cell items, building index took " << elapsed_seconds.count() << "s" << std::endl;
        ++index_rebuilds;
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

//        ConstColumnValueExtractor(const ColumnConst & const_needle_col)
//            : ConstColumnValueExtractor(typeid_cast<const NeedleColumnType>(const_needle_col))
//        {}

        inline KeyStorageType getElement(size_t /*row*/) const
        {
            return value;
        }
    };

    IColumn::Ptr findAll(const IColumn & needles, size_t rows_count) const override
    {
        if (index.size() < needles.size())
            throw Exception(fmt::format("There are more needles ({}) that rows in index ({})", needles.size(), index.size()),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (const auto * needles_column = checkAndGetColumn<ColumnConst>(needles))
        {
            auto findConst = [this](const auto & typed_needles_column, size_t rows_count_ ) -> auto
            {
                return this->findAllTyped<ConstColumnValueExtractor>(typed_needles_column, rows_count_);
            };
            return findDispatch(findConst, needles_column->getDataColumn(), rows_count);
        }
        else
        {
            auto findVector = [this](const auto & typed_needles_column, size_t rows_count_ ) -> auto
            {
                return this->findAllTyped<VectorColumnValueExtractor>(typed_needles_column, rows_count_);
            };
            return findDispatch(findVector, needles, rows_count);
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
                needles.getName(), keys_column.getData().getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <template <typename> typename ValueExtractor, typename NeedleColumnType>
    IColumn::Ptr findAllTyped(const NeedleColumnType & needles, size_t rows_count) const
    {
        const ValueExtractor<NeedleColumnType> values(needles);

        ++find_queries;

        size_t misses = 0;
        size_t hits = 0;

        const auto & nested_values = values_column.getData();
        auto result = nested_values.cloneEmpty();
        result->reserve(needles.size());

        Stopwatch stopwatch;
        stopwatch.start();

        for (size_t row = 0; row < rows_count; ++row)
        {
            auto res = index[row].find(static_cast<KeyStorageType>(values.getElement(row)));
            if (res)
            {
                result->insertFrom(nested_values, res->getMapped());
                ++hits;
            }
            else
            {
                result->insertDefault();
                ++misses;
            }
        }

        stopwatch.stop();
        find_total_nanoseconds += stopwatch.elapsed();
        index_lookups += rows_count;
        find_hits += hits;
        find_misses += misses;
        return result;
    }
};

std::shared_ptr<ColumnMap::IIndex> makeIndex(const ColumnArray & keys_column, const ColumnArray & values_column)
{
    switch (keys_column.getData().getDataType())
    {
        case TypeIndex::UInt8:
            return std::make_shared<ColumnMapIndex<ColumnVector<UInt8>, UInt64>>(keys_column, values_column);
        case TypeIndex::UInt16:
            return std::make_shared<ColumnMapIndex<ColumnVector<UInt16>, UInt64>>(keys_column, values_column);
        case TypeIndex::UInt32:
            return std::make_shared<ColumnMapIndex<ColumnVector<UInt32>, UInt64>>(keys_column, values_column);
        case TypeIndex::UInt64:
            return std::make_shared<ColumnMapIndex<ColumnVector<UInt64>, UInt64>>(keys_column, values_column);
////        case TypeIndex::UInt128:
////            return std::make_shared<ColumnMapIndex<ColumnVector<UInt128>, UInt128>>(keys_column, values_column);
////        case TypeIndex::UInt256:
        case TypeIndex::Int8:
            return std::make_shared<ColumnMapIndex<ColumnVector<Int8>, Int64>>(keys_column, values_column);
        case TypeIndex::Int16:
            return std::make_shared<ColumnMapIndex<ColumnVector<Int16>, Int64>>(keys_column, values_column);
        case TypeIndex::Int32:
            return std::make_shared<ColumnMapIndex<ColumnVector<Int32>, Int64>>(keys_column, values_column);
        case TypeIndex::Int64:
            return std::make_shared<ColumnMapIndex<ColumnVector<Int64>, Int64>>(keys_column, values_column);
////        case TypeIndex::Int128:
////        case TypeIndex::Int256:
// TODO: since most of Floats are not going to be binary equal, it might not make any sense to store those in a hashmap
//        case TypeIndex::Float32:
//            return std::make_shared<ColumnMapIndex<ColumnVector<Float32>, Float64>>(keys_column, values_column);
//        case TypeIndex::Float64:
//            return std::make_shared<ColumnMapIndex<ColumnVector<Float64>, Float64>>(keys_column, values_column);
////        case TypeIndex::Date:
////        case TypeIndex::DateTime:
////        case TypeIndex::DateTime64:
        case TypeIndex::String:
            return std::make_shared<ColumnMapIndex<ColumnString, StringRef>>(keys_column, values_column);
        case TypeIndex::FixedString:
            return std::make_shared<ColumnMapIndex<ColumnFixedString, StringRef>>(keys_column, values_column);
        case TypeIndex::Enum8:
            return std::make_shared<ColumnMapIndex<ColumnVector<Int8>, Int64>>(keys_column, values_column);
        case TypeIndex::Enum16:
            return std::make_shared<ColumnMapIndex<ColumnVector<Int16>, Int64>>(keys_column, values_column);
////        case TypeIndex::Decimal32:
////        case TypeIndex::Decimal64:
////        case TypeIndex::Decimal128:
////        case TypeIndex::Decimal256:
////        case TypeIndex::UUID:
    default:
        throw Exception("Unsuported KEY column of Map " + keys_column.getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

}

namespace DB
{

std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    res << "Map(" << columns[0]->getName() << ", " << columns[1]->getName() << ")";
    return res.str();
}

ColumnMap::ColumnMap(MutableColumns && mutable_columns)
    : key_index(makeIndex(
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
      key_index(makeIndex(
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

    key_index->afterAppend(1);
}
void ColumnMap::popBack(size_t n)
{
    key_index->beforeRemove(n);

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

    std::cerr << "!!!! rebuilding index" << __PRETTY_FUNCTION__ << std::endl;
    key_index->rebuild();
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

    std::cerr << "!!!! rebuilding index" << __PRETTY_FUNCTION__ << std::endl;
    key_index->rebuild();
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

//    if (key_index == nullptr)
//    {
////        std::lock_guard<std::mutex> lock(key_index_mutex);
//        if (key_index == nullptr)
//        {
//            const ColumnArray & keys_array = assert_cast<const ColumnArray &>(*columns[0]);
//            const ColumnArray & values_array = assert_cast<const ColumnArray &>(*columns[1]);
//            std::make_shared<Index>(keys_array, values_array).swap(key_index);
//        }
//    }

    return key_index->findAll(keys, rows_count);
}

}
