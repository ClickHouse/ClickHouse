#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Common/Arena.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>

#include <Common/CacheBase.h>
#include <Common/SLRUCachePolicy.h>

#include <condition_variable>
#include <mutex>


namespace DB
{

namespace Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
}


namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

namespace
{

inline UInt64 hashAt(const IColumn & column, size_t row_id)
{
    SipHash h;
    column.updateHashWithValue(row_id, h);
    return h.get64();
}

inline bool equalAt(const IColumn & left_column, size_t left_row_id, const IColumn & right_column, size_t right_row_id)
{
    if (const auto * left_nullable = checkAndGetColumn<ColumnNullable>(&left_column))
    {
        if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(&right_column))
        {
            const bool left_is_null = left_nullable->isNullAt(left_row_id);
            const bool right_is_null = right_nullable->isNullAt(right_row_id);
            if (left_is_null || right_is_null)
                return left_is_null && right_is_null;

            /// Both not null
            return left_nullable->getNestedColumn().compareAt(
                       left_row_id, right_row_id, right_nullable->getNestedColumn(), /*nan_direction_hint*/ 1)
                == 0;
        }

        if (left_nullable->isNullAt(left_row_id))
            return false;

        /// Right is not nullable
        return left_nullable->getNestedColumn().compareAt(left_row_id, right_row_id, right_column, /*nan_direction_hint*/ 1) == 0;
    }

    if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(&right_column))
    {
        if (right_nullable->isNullAt(right_row_id))
            return false;

        return left_column.compareAt(left_row_id, right_row_id, right_nullable->getNestedColumn(), /*nan_direction_hint*/ 1) == 0;
    }

    return left_column.compareAt(left_row_id, right_row_id, right_column, /*nan_direction_hint*/ 1) == 0;
}

struct ValueBytes
{
    std::string bytes;
    UInt64 sip = 0;

    /// TODO: Maybe we do not need to store the null value? maybe its already encoded?
    void setAndHash(const char * data, size_t size, bool is_null)
    {
        bytes.resize(size + 1);
        bytes[0] = static_cast<char>(!is_null);
        if (size)
            memcpy(bytes.data() + 1, data, size);

        SipHash h;
        h.update(bytes.data(), bytes.size());
        sip = h.get64();
    }
};

/// Cache key: (dictionary name, epoch, attribute, canonical bytes)
/// TODO: Use 128 bit sip which should be strong enough to avoid collisions here and then skip storing full bytes?
struct Key
{
    std::string dict_name;
    UInt64 epoch = 0;
    std::string attr_name;
    ValueBytes value;

    bool operator==(const Key & rhs) const
    {
        return epoch == rhs.epoch && value.sip == rhs.value.sip && dict_name == rhs.dict_name && attr_name == rhs.attr_name
            && value.bytes == rhs.value.bytes;
    }
};

struct KeyHash
{
    size_t operator()(const Key & key) const noexcept
    {
        SipHash h;
        h.update(key.dict_name.data(), key.dict_name.size());
        h.update(key.attr_name.data(), key.attr_name.size());
        h.update(key.epoch);
        h.update(key.value.sip);
        return static_cast<size_t>(h.get64());
    }
};

/// Single-key dicts: ColumnArray(inner_key_column, offsets=[...])
/// Multi-key dicts: ColumnArray(ColumnTuple(key_cols...), offsets=[...])
struct Mapped
{
    ColumnPtr array;
    size_t bytes = 0;
};


/// Weight function for the SLRU: count allocated bytes of cached array
/// TODO: add any other overheads from key?
struct MappedWeight
{
    size_t operator()(const Mapped & mapped) const
    {
        return (mapped.array ? mapped.array->allocatedBytes() : 0) + sizeof(Mapped) + sizeof(Key);
    }
};

using DictGetKeysCache = CacheBase<Key, Mapped, KeyHash, MappedWeight>;


struct DomainKey
{
    std::string dict_name;
    std::string attr_name;

    bool operator==(const DomainKey & other) const { return dict_name == other.dict_name && attr_name == other.attr_name; }
};

struct DomainKeyHash
{
    size_t operator()(const DomainKey & key) const noexcept
    {
        SipHash h;
        h.update(key.dict_name.data(), key.dict_name.size());
        h.update(key.attr_name.data(), key.attr_name.size());
        return static_cast<size_t>(h.get64());
    }
};

struct Domain
{
    std::mutex m;
    std::condition_variable cv;
    bool building = false; // a thread is scanning dict->read() for this (dict, attr)
    UInt64 build_epoch_seen = 0; // to wake waiters
};


class SharedCache
{
public:
    static SharedCache & instance()
    {
        static SharedCache inst;
        return inst;
    }

    /// Called fast-path on every executeImpl to compute dictionary epoch.
    /// If the source is reported modified, bump epoch and opportunistically delete old entries for this dict.
    UInt64 getEpochAndMaybeBump(const std::string & dict_name, const auto & dict)
    {
        bool modified = dict->isModified();

        std::lock_guard lk(epoch_mutex);
        auto & rec = epochs[dict_name];
        if (modified)
        {
            ++rec.epoch;

            // Proactively evict old-epoch keys for this dictionary
            cache.remove([&](const Key & key, const DictGetKeysCache::MappedPtr &)
                         { return key.dict_name == dict_name && key.epoch < rec.epoch; });
        }
        return rec.epoch;
    }

    Domain & getDomain(const std::string & dict_name, const std::string & attr)
    {
        std::lock_guard lk(domain_mutex);
        return domains[DomainKey{dict_name, attr}];
    }

    DictGetKeysCache & getCache() { return cache; }

private:
    SharedCache()
        : cache(
              "SLRU",
              CurrentMetrics::end(),
              CurrentMetrics::end(),
              defaultMaxBytes(),
              DictGetKeysCache::NO_MAX_COUNT,
              DictGetKeysCache::DEFAULT_SIZE_RATIO)
    {
    }

    /// TODO: Change to be configurable via settings
    static size_t defaultMaxBytes() { return 100ULL << 20; }

    struct EpochRec
    {
        UInt64 epoch = 0;
    };

    std::mutex epoch_mutex;
    std::unordered_map<std::string, EpochRec> epochs;

    std::mutex domain_mutex;
    std::unordered_map<DomainKey, Domain, DomainKeyHash> domains;

    DictGetKeysCache cache;
};

inline ValueBytes serializeValueWithNullTag(const IColumn & col, size_t row_id)
{
    ValueBytes out;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&col))
    {
        if (nullable->isNullAt(row_id))
        {
            out.setAndHash(nullptr, 0, /*is_null=*/true);
            return out;
        }
        const IColumn & nested = nullable->getNestedColumn();
        if (auto sz = nested.getSerializedValueSize(row_id))
        {
            out.bytes.resize(1 + *sz);
            out.bytes[0] = 1;
            char * dst = out.bytes.data() + 1;
            nested.serializeValueIntoMemory(row_id, dst);
            SipHash h;
            h.update(out.bytes.data(), out.bytes.size());
            out.sip = h.get64();
            return out;
        }

        // Fallback to arena
        Arena scratch;
        const char * begin = nullptr;
        StringRef ref = nested.serializeValueIntoArena(row_id, scratch, begin);
        out.setAndHash(ref.data, ref.size, /*is_null=*/false);
        return out;
    }

    // Non-nullable case
    if (auto sz = col.getSerializedValueSize(row_id))
    {
        out.bytes.resize(1 + *sz);
        out.bytes[0] = 1;
        char * dst = out.bytes.data() + 1;
        col.serializeValueIntoMemory(row_id, dst);
        SipHash h;
        h.update(out.bytes.data(), out.bytes.size());
        out.sip = h.get64();
        return out;
    }

    // Arena fallback
    {
        Arena scratch;
        const char * begin = nullptr;
        StringRef ref = col.serializeValueIntoArena(row_id, scratch, begin);
        out.setAndHash(ref.data, ref.size, /*is_null=*/false);
        return out;
    }
}

/// Single key: Array(KeyType)
/// Multi key:  Array(Tuple(key1,...,keyN))
/// TODO: This is slow
Mapped makeCachedArrayFromLinkedList(
    const std::vector<MutableColumnPtr> & payload_key_cols,
    const std::vector<size_t> & next_key_pos,
    size_t last_key_pos_for_bucket,
    const DataTypes & key_types)
{
    std::vector<MutableColumnPtr> key_cols;
    key_cols.reserve(key_types.size());
    for (const auto & t : key_types)
        key_cols.emplace_back(t->createColumn());

    size_t cur = last_key_pos_for_bucket;
    size_t len = 0;
    while (cur != std::numeric_limits<size_t>::max())
    {
        for (size_t key_id = 0; key_id < key_cols.size(); ++key_id)
            key_cols[key_id]->insertFrom(*payload_key_cols[key_id], cur);
        cur = next_key_pos[cur];
        ++len;
    }

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(len);

    if (key_cols.size() == 1)
    {
        auto array = ColumnArray::create(std::move(key_cols[0]), std::move(offsets));
        Mapped mapped;
        mapped.array = std::move(array);
        mapped.bytes = mapped.array->allocatedBytes();
        return mapped;
    }
    auto tuple_column = ColumnTuple::create(std::move(key_cols));
    auto array = ColumnArray::create(std::move(tuple_column), std::move(offsets));
    Mapped mapped;
    mapped.array = std::move(array);
    mapped.bytes = mapped.array->allocatedBytes();
    return mapped;
}

Mapped makeEmptyCachedArray(const DataTypes & key_types)
{
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(0);

    if (key_types.size() == 1)
    {
        auto array = ColumnArray::create(key_types[0]->createColumn(), std::move(offsets));
        Mapped mapped;
        mapped.array = std::move(array);
        mapped.bytes = mapped.array->allocatedBytes();
        return mapped;
    }

    std::vector<MutableColumnPtr> key_cols;
    key_cols.reserve(key_types.size());
    for (const auto & t : key_types)
        key_cols.emplace_back(t->createColumn());
    auto tuple_column = ColumnTuple::create(std::move(key_cols));
    auto array = ColumnArray::create(std::move(tuple_column), std::move(offsets));
    Mapped mapped;
    mapped.array = std::move(array);
    mapped.bytes = mapped.array->allocatedBytes();
    return mapped;
}

}

class FunctionDictGetKeys final : public IFunction
{
public:
    static constexpr auto name = "dictGetKeys";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDictGetKeys>(context); }

    explicit FunctionDictGetKeys(ContextPtr context_)
        : helper(context_)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isVariadic() const override { return false; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const final { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * dict_name_const_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!dict_name_const_col)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String.",
                arguments[0].type->getName(),
                getName());

        const String dictionary_name = dict_name_const_col->getValue<String>();

        const auto * attr_name_const_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!attr_name_const_col)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String.",
                arguments[1].type->getName(),
                getName());

        const String attribute_column_name = attr_name_const_col->getValue<String>();

        auto dict_struct = helper.getDictionaryStructure(dictionary_name);
        if (!dict_struct.hasAttribute(attribute_column_name))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dictionary has no attribute '{}'", attribute_column_name);

        const auto key_types = dict_struct.getKeyTypes();
        if (key_types.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary has no keys");

        if (key_types.size() == 1)
            return std::make_shared<DataTypeArray>(key_types[0]);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(key_types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        const String dict_name = checkAndGetColumnConst<ColumnString>(arguments[0].column.get())->getValue<String>();
        const String attr_name = checkAndGetColumnConst<ColumnString>(arguments[1].column.get())->getValue<String>();

        auto dict = helper.getDictionary(arguments[0].column);
        const auto & structure = dict->getStructure();
        const auto key_types = structure.getKeyTypes();
        const size_t keys_cnt = key_types.size();

        const auto & attribute_column_type = structure.getAttribute(attr_name).type;


        /// In the event the dictionary has been reloaded and modified, we invalidate the previous cache by bumping the epoch that is
        /// uniquely associated with the dictionary state.
        UInt64 epoch = SharedCache::instance().getEpochAndMaybeBump(dict_name, dict);

        const bool is_values_const = isColumnConst(*arguments[2].column);

        ColumnWithTypeAndName values_column_raw{arguments[2].column->convertToFullColumnIfConst(), arguments[2].type, arguments[2].name};
        ColumnPtr values_full = castColumnAccurate(values_column_raw, attribute_column_type)->convertToFullColumnIfLowCardinality();

        if (is_values_const)
            return executeConstPath(dict_name, epoch, attr_name, *values_full, key_types, keys_cnt, input_rows_count, dict);

        return executeVectorPath(dict_name, epoch, attr_name, *values_full, key_types, keys_cnt, input_rows_count, dict);
    }

private:
    mutable FunctionDictHelper helper;

    ColumnPtr executeConstPath(
        const String & dict_name,
        UInt64 epoch,
        const String & attr_name,
        const IColumn & values_col,
        const DataTypes & key_types,
        size_t keys_cnt,
        size_t input_rows_count,
        const auto & dict) const
    {
        auto value_bytes = serializeValueWithNullTag(values_col, 0);
        Key key{dict_name, epoch, attr_name, std::move(value_bytes)};

        auto & cache = SharedCache::instance().getCache();

        auto [mapped_ptr, was_built] = cache.getOrSet(
            key,
            [&]() -> DictGetKeysCache::MappedPtr
            {
                auto results = computeForMissesSingleScan(
                    attr_name,
                    dict,
                    key_types,
                    keys_cnt,
                    [&](auto & target_map, auto & bucket_representatives, auto & interested_buckets)
                    {
                        UInt64 hash = hashAt(values_col, 0);
                        target_map[hash].push_back(0);
                        bucket_representatives.push_back(0);
                        interested_buckets.push_back(0);
                    },
                    values_col,
                    1);
                return std::make_shared<Mapped>(std::move(results[0]));
            });

        ColumnPtr array = mapped_ptr->array;
        return ColumnConst::create(array, input_rows_count);
    }

    ColumnPtr executeVectorPath(
        const String & dict_name,
        UInt64 epoch,
        const String & attr_name,
        const IColumn & values_column,
        const DataTypes & key_types,
        size_t keys_cnt,
        size_t input_rows_count,
        const auto & dict) const
    {
        using BucketIdList = PODArray<UInt64, 2 * sizeof(UInt64)>;
        using Map = HashMap<UInt64, BucketIdList, HashCRC32<UInt64>>;
        Map hash_to_bucket_ids;
        hash_to_bucket_ids.reserve(input_rows_count);

        std::vector<size_t> row_id_to_bucket_id(input_rows_count);
        std::vector<size_t> bucket_id_to_representative_row_id;
        bucket_id_to_representative_row_id.reserve(input_rows_count);

        for (size_t cur_row_id = 0; cur_row_id < input_rows_count; ++cur_row_id)
        {
            const UInt64 hash = hashAt(values_column, cur_row_id);
            auto & potential_bucket_ids = hash_to_bucket_ids[hash];

            bool previously_seen = false;
            for (size_t bucket_id : potential_bucket_ids)
            {
                size_t bucket_representative_row_id = bucket_id_to_representative_row_id[bucket_id];
                if (equalAt(values_column, cur_row_id, values_column, bucket_representative_row_id))
                {
                    previously_seen = true;
                    row_id_to_bucket_id[cur_row_id] = bucket_id;
                    break;
                }
            }

            /// New unique value, create a new bucket
            if (!previously_seen)
            {
                const size_t new_bucket_id = bucket_id_to_representative_row_id.size();
                bucket_id_to_representative_row_id.push_back(cur_row_id);
                potential_bucket_ids.push_back(new_bucket_id);
                row_id_to_bucket_id[cur_row_id] = new_bucket_id;
            }
        }

        const size_t num_buckets = bucket_id_to_representative_row_id.size();

        std::vector<ValueBytes> bucket_bytes;
        bucket_bytes.reserve(num_buckets);
        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
            bucket_bytes.emplace_back(serializeValueWithNullTag(values_column, bucket_id_to_representative_row_id[bucket_id]));

        auto & cache = SharedCache::instance().getCache();
        std::vector<DictGetKeysCache::MappedPtr> bucket_cached(num_buckets); // may contain nullptr for misses
        std::vector<size_t> miss_bucket_ids;
        miss_bucket_ids.reserve(num_buckets);

        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
        {
            Key key{dict_name, epoch, attr_name, bucket_bytes[bucket_id]};
            if (auto hit = cache.get(key))
                bucket_cached[bucket_id] = hit;
            else
                miss_bucket_ids.push_back(bucket_id);
        }

        if (!miss_bucket_ids.empty())
        {
            Domain & dom = SharedCache::instance().getDomain(dict_name, attr_name);
            std::unique_lock lk(dom.m);

            // If someone else is currently building for this (dict, attr), wait until their scan completes, then recheck cache.
            if (dom.building)
            {
                UInt64 seen = dom.build_epoch_seen;
                dom.cv.wait(lk, [&] { return dom.build_epoch_seen != seen && !dom.building; });

                // Recheck cache for our misses to pick up entries filled by the other builder
                std::vector<size_t> remaining;
                remaining.reserve(miss_bucket_ids.size());
                for (size_t bucket_id : miss_bucket_ids)
                {
                    Key key{dict_name, epoch, attr_name, bucket_bytes[bucket_id]};
                    if (auto hit = cache.get(key))
                        bucket_cached[bucket_id] = hit;
                    else
                        remaining.push_back(bucket_id);
                }
                miss_bucket_ids.swap(remaining);
            }

            // If still have misses, we do the single scan; others will wait.
            if (!miss_bucket_ids.empty())
            {
                dom.building = true;
                lk.unlock();

                auto results = computeForMissesSingleScan(
                    attr_name,
                    dict,
                    key_types,
                    keys_cnt,
                    [&](auto & target_map, auto & bucket_representatives, auto & interested_buckets)
                    {
                        bucket_representatives = bucket_id_to_representative_row_id; // use same representatives
                        interested_buckets = miss_bucket_ids;

                        for (size_t bucket_id : miss_bucket_ids)
                        {
                            UInt64 hash = hashAt(values_column, bucket_representatives[bucket_id]);
                            target_map[hash].push_back(bucket_id);
                        }
                    },
                    values_column,
                    input_rows_count);

                // Insert computed results into cache and fill our local bucket_cached
                for (size_t bucket_id : miss_bucket_ids)
                {
                    Key key{dict_name, epoch, attr_name, bucket_bytes[bucket_id]};
                    auto mapped_ptr = std::make_shared<Mapped>(std::move(results[bucket_id]));
                    cache.set(key, mapped_ptr);
                    bucket_cached[bucket_id] = std::move(mapped_ptr);
                }

                /// Wake waiters
                lk.lock();
                dom.building = false;
                ++dom.build_epoch_seen;
                lk.unlock();
                dom.cv.notify_all();
            }
        }

        std::vector<size_t> bucket_len(num_buckets);
        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
        {
            const auto & array = static_cast<const ColumnArray &>(*bucket_cached[bucket_id]->array);
            bucket_len[bucket_id] = array.getOffsets()[0];
        }

        if (num_buckets == 1)
            return ColumnConst::create(bucket_cached[0]->array, input_rows_count);

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_col->getData();
        offsets.resize(input_rows_count);

        std::vector<size_t> bucket_use_count(num_buckets, 0);
        for (size_t row_id = 0; row_id < input_rows_count; ++row_id)
            ++bucket_use_count[row_id_to_bucket_id[row_id]];

        size_t total_output = 0;
        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
            total_output += bucket_len[bucket_id] * bucket_use_count[bucket_id];

        if (keys_cnt == 1)
        {
            std::vector<const IColumn *> src(num_buckets);
            for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
                src[bucket_id] = &static_cast<const ColumnArray &>(*bucket_cached[bucket_id]->array).getData();

            auto data_col = key_types[0]->createColumn();
            data_col->reserve(total_output);

            size_t pos = 0;
            for (size_t row_id = 0; row_id < input_rows_count;)
            {
                const size_t bucket_id = row_id_to_bucket_id[row_id];
                const size_t len = bucket_len[bucket_id];
                size_t j = row_id + 1;
                while (j < input_rows_count && row_id_to_bucket_id[j] == bucket_id)
                    ++j;

                data_col->insertRangeFrom(*src[bucket_id], 0, len);

                for (; row_id < j; ++row_id)
                {
                    pos += len;
                    offsets[row_id] = pos;
                }
            }

            return ColumnArray::create(std::move(data_col), std::move(offsets_col));
        }

        std::vector<const IColumn *> src(num_buckets * keys_cnt);
        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
        {
            const auto & arr = static_cast<const ColumnArray &>(*bucket_cached[bucket_id]->array);
            const auto & tup = static_cast<const ColumnTuple &>(arr.getData());
            const size_t base = bucket_id * keys_cnt;
            for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                src[base + key_id] = &tup.getColumn(key_id);
        }

        MutableColumns result_cols;
        result_cols.reserve(keys_cnt);
        for (const auto & t : key_types)
        {
            auto col = t->createColumn();
            col->reserve(total_output);
            result_cols.emplace_back(std::move(col));
        }

        size_t pos = 0;
        for (size_t row_id = 0; row_id < input_rows_count;)
        {
            const size_t bucket_id = row_id_to_bucket_id[row_id];
            const size_t len = bucket_len[bucket_id];
            const size_t base = bucket_id * keys_cnt;
            size_t j = row_id + 1;
            while (j < input_rows_count && row_id_to_bucket_id[j] == bucket_id)
                ++j;

            for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                result_cols[key_id]->insertRangeFrom(*src[base + key_id], 0, len);

            for (; row_id < j; ++row_id)
            {
                pos += len;
                offsets[row_id] = pos;
            }
        }

        return ColumnArray::create(ColumnTuple::create(std::move(result_cols)), std::move(offsets_col));
    }

    template <class SetupTargets>
    std::vector<Mapped> computeForMissesSingleScan(
        const String & attr_name,
        const auto & dict,
        const DataTypes & key_types,
        size_t keys_cnt,
        SetupTargets && setup_targets,
        const IColumn & values_column,
        size_t input_rows_count) const
    {
        using BucketIdList = PODArray<UInt64, 2 * sizeof(UInt64)>;
        using Map = HashMap<UInt64, BucketIdList, HashCRC32<UInt64>>;

        Map target;
        std::vector<size_t> bucket_representatives;
        std::vector<size_t> interested_buckets;
        setup_targets(target, bucket_representatives, interested_buckets);

        const size_t num_buckets_total = bucket_representatives.size();
        std::vector<size_t> last_key_pos_for_bucket(num_buckets_total, std::numeric_limits<size_t>::max());
        std::vector<size_t> counts(num_buckets_total, 0);
        std::vector<size_t> next_key_pos;
        next_key_pos.reserve(input_rows_count);

        std::vector<MutableColumnPtr> payload_key_cols;
        payload_key_cols.reserve(keys_cnt);
        for (const auto & t : key_types)
            payload_key_cols.emplace_back(t->createColumn());

        Names column_names = dict->getStructure().getKeysNames();
        column_names.push_back(attr_name);

        auto pipe = dict->read(column_names, helper.getContext()->getSettingsRef()[Setting::max_block_size], 1);
        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
        {
            ColumnPtr attr_col = block.getByPosition(keys_cnt).column->convertToFullColumnIfLowCardinality();

            std::vector<ColumnPtr> key_src(keys_cnt);
            for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                key_src[key_id] = block.getByPosition(key_id).column->convertToFullColumnIfLowCardinality();

            const size_t rows = attr_col->size();
            for (size_t i = 0; i < rows; ++i)
            {
                const UInt64 hash = hashAt(*attr_col, i);

                auto * it = target.find(hash);
                if (it == target.end())
                    continue;

                const auto & candidate_buckets = it->getMapped();
                for (size_t bucket_id : candidate_buckets)
                {
                    size_t bucket_representative_row_id = bucket_representatives[bucket_id];
                    if (!equalAt(*attr_col, i, values_column, bucket_representative_row_id))
                        continue;

                    const size_t cur_pos = payload_key_cols[0]->size();
                    for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                        payload_key_cols[key_id]->insertFrom(*key_src[key_id], i);

                    next_key_pos.push_back(last_key_pos_for_bucket[bucket_id]);
                    last_key_pos_for_bucket[bucket_id] = cur_pos;
                    ++counts[bucket_id];

                    break;
                }
            }
        }

        std::vector<Mapped> out(num_buckets_total);

        for (size_t bucket_id : interested_buckets)
        {
            if (counts[bucket_id] == 0)
                out[bucket_id] = makeEmptyCachedArray(key_types);
            else
                out[bucket_id]
                    = makeCachedArrayFromLinkedList(payload_key_cols, next_key_pos, last_key_pos_for_bucket[bucket_id], key_types);
        }

        return out;
    }
};


REGISTER_FUNCTION(DictGetKeys)
{
    FunctionDocumentation::Description description = "Inverse dictionary lookup: return keys where attribute equals the given value.";
    FunctionDocumentation::Syntax syntax = "dictGetKeys('dict_name', 'attr_name', value_expr)";
    FunctionDocumentation::Arguments arguments
        = {{"dict_name", "Name of the dictionary.", {"String"}},
           {"attr_name", "Attribute to match.", {"String"}},
           {"value_expr", "Value to match against the attribute.", {"Expression"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"For single key dictionaries: an array of keys whose attribute equals `value_expr`. For multi key dictionaries: an array of "
           "tuples of keys whose attribute equals `value_expr`. If there is no attribute corresponding to `value_expr` in the dictionary, "
           "then an empty array is returned. ClickHouse throws an exception if it cannot parse the value of the attribute or the value "
           "cannot be converted to the attribute data type.",
           {}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Dictionary;
    FunctionDocumentation docs{description, syntax, arguments, returned_value, {}, introduced_in, category};

    factory.registerFunction<FunctionDictGetKeys>(docs);
}
}
