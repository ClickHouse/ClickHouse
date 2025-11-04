#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Core/Settings.h>
#include <Core/Types.h>
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
#include <unordered_set>


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

inline UInt128 hashAt(const IColumn & column, size_t row_id)
{
    SipHash h;
    column.updateHashWithValue(row_id, h);
    return h.get128();
}


struct Key
{
    UInt128 domain_hash;
    UInt128 hash;

    bool operator==(const Key & rhs) const { return domain_hash == rhs.domain_hash && hash == rhs.hash; }
};

struct KeyHash
{
    size_t operator()(const Key & key) const noexcept
    {
        SipHash sip;
        sip.update(key.domain_hash);
        sip.update(key.hash);
        return static_cast<size_t>(sip.get64());
    }
};

using Mapped = std::vector<std::byte>;

using MappedPtr = std::shared_ptr<Mapped>;

struct MappedWeight
{
    size_t operator()(const Mapped & mapped) const { return mapped.capacity() + sizeof(Mapped); }
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
    bool building = false;
    UInt64 build_epoch_seen = 0;
};


class SharedCache
{
public:
    static SharedCache & instance()
    {
        static SharedCache inst;
        return inst;
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

    std::mutex domain_mutex;
    std::unordered_map<DomainKey, Domain, DomainKeyHash> domains;

    DictGetKeysCache cache;
};

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

        const auto & attribute_column_type = structure.getAttribute(attr_name).type;


        ColumnWithTypeAndName values_column_raw{arguments[2].column->convertToFullColumnIfConst(), arguments[2].type, arguments[2].name};
        ColumnPtr values_full = castColumnAccurate(values_column_raw, attribute_column_type)->convertToFullColumnIfLowCardinality();

        return executeVectorPath(dict_name, attr_name, *values_full, key_types, input_rows_count, dict);
    }

private:
    mutable FunctionDictHelper helper;

    using Map = HashMap<UInt128, size_t, HashCRC32<UInt128>>;

    ColumnPtr executeVectorPath(
        const String & dict_name,
        const String & attr_name,
        const IColumn & values_column,
        const DataTypes & key_types,
        size_t input_rows_count,
        const auto & dict) const
    {
        Map hash_to_bucket_id;
        hash_to_bucket_id.reserve(input_rows_count);

        SipHash sip;
        sip.update(dict_name.data(), dict_name.size());
        sip.update(attr_name.data(), attr_name.size());
        const UInt128 domain_hash = sip.get128();

        std::vector<size_t> row_id_to_bucket_id(input_rows_count);

        size_t num_buckets = 0;
        std::vector<UInt128> bucket_hashes;
        bucket_hashes.reserve(input_rows_count);

        for (size_t cur_row_id = 0; cur_row_id < input_rows_count; ++cur_row_id)
        {
            const UInt128 hash = hashAt(values_column, cur_row_id);

            auto * it = hash_to_bucket_id.find(hash);
            if (it)
            {
                row_id_to_bucket_id[cur_row_id] = it->getMapped();
            }
            else
            {
                const size_t new_bucket_id = num_buckets++;
                hash_to_bucket_id[hash] = new_bucket_id;
                row_id_to_bucket_id[cur_row_id] = new_bucket_id;
                bucket_hashes.push_back(hash);
            }
        }

        auto & cache = SharedCache::instance().getCache();
        std::vector<MappedPtr> bucket_cached(num_buckets); // may contain nullptr for misses
        std::vector<size_t> missing_bucket_ids;
        missing_bucket_ids.reserve(num_buckets);

        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
        {
            Key key{domain_hash, bucket_hashes[bucket_id]};
            if (auto hit = cache.get(key))
                bucket_cached[bucket_id] = hit;
            else
                missing_bucket_ids.push_back(bucket_id);
        }

        if (!missing_bucket_ids.empty())
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
                remaining.reserve(missing_bucket_ids.size());
                for (size_t bucket_id : missing_bucket_ids)
                {
                    Key key{domain_hash, bucket_hashes[bucket_id]};
                    if (auto hit = cache.get(key))
                        bucket_cached[bucket_id] = hit;
                    else
                        remaining.push_back(bucket_id);
                }
                missing_bucket_ids.swap(remaining);
            }

            // If still have misses, we do the single scan; others will wait.
            if (!missing_bucket_ids.empty())
            {
                dom.building = true;
                lk.unlock();

                getMissingBucketKeys(dict, attr_name, key_types, bucket_cached, missing_bucket_ids, hash_to_bucket_id);

                for (size_t bucket_id : missing_bucket_ids)
                {
                    if (!bucket_cached[bucket_id])
                        bucket_cached[bucket_id] = std::make_shared<Mapped>();
                    Key key{domain_hash, bucket_hashes[bucket_id]};
                    cache.set(key, bucket_cached[bucket_id]);
                }

                /// Wake waiters
                lk.lock();
                dom.building = false;
                ++dom.build_epoch_seen;
                lk.unlock();
                dom.cv.notify_all();
            }
        }

        const size_t keys_cnt = key_types.size();
        MutableColumns result_cols;
        result_cols.reserve(keys_cnt);
        for (const auto & key_type : key_types)
        {
            auto col = key_type->createColumn();
            col->reserve(input_rows_count);
            result_cols.emplace_back(std::move(col));
        }

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_col->getData();
        offsets.resize(input_rows_count);

        std::vector<size_t> bucket_first_offset(num_buckets, std::numeric_limits<size_t>::max());
        std::vector<size_t> bucket_len(num_buckets, 0);

        size_t current_offset = 0;
        for (size_t row_id = 0; row_id < input_rows_count; ++row_id)
        {
            const size_t bucket_id = row_id_to_bucket_id[row_id];

            // empty hit
            if (!bucket_cached[bucket_id])
            {
                offsets[row_id] = current_offset;
                continue;
            }

            size_t start = bucket_first_offset[bucket_id];
            size_t len = bucket_len[bucket_id];

            if (start != std::numeric_limits<size_t>::max())
            {
                // fast path: reuse already materialized slice
                if (len)
                {
                    for (size_t key_pos = 0; key_pos < keys_cnt; ++key_pos)
                        result_cols[key_pos]->insertRangeFrom(*result_cols[key_pos], start, len);
                    current_offset += len;
                }
                offsets[row_id] = current_offset;
                continue;
            }

            // first time this bucket is seen: decode once from cached_bytes
            const auto & cached_bytes = *bucket_cached[bucket_id];
            if (cached_bytes.empty())
            {
                bucket_first_offset[bucket_id] = current_offset;
                bucket_len[bucket_id] = 0;
                offsets[row_id] = current_offset;
                continue;
            }

            const size_t before = current_offset;
            const char * pos = reinterpret_cast<const char *>(cached_bytes.data());
            const char * end = pos + cached_bytes.size();

            while (pos < end)
            {
                for (size_t key_pos = 0; key_pos < keys_cnt; ++key_pos)
                    pos = result_cols[key_pos]->deserializeAndInsertFromArena(pos);
                ++current_offset;
            }

            bucket_first_offset[bucket_id] = before;
            bucket_len[bucket_id] = current_offset - before;
            offsets[row_id] = current_offset;
        }

        if (keys_cnt == 1)
        {
            return ColumnArray::create(std::move(result_cols[0]), std::move(offsets_col));
        }

        return ColumnArray::create(ColumnTuple::create(std::move(result_cols)), std::move(offsets_col));
    }

    template <class DictionaryPtr>
    void getMissingBucketKeys(
        const DictionaryPtr & dict,
        const String & attr_name,
        const DataTypes & key_types,
        std::vector<MappedPtr> & out,
        const std::vector<size_t> & missing_bucket_ids,
        const Map & hash_to_bucket_id) const
    {
        std::vector<UInt8> is_missing(out.size(), 0);
        for (size_t id : missing_bucket_ids)
            is_missing[id] = 1;

        const size_t keys_cnt = key_types.size();

        Names column_names = dict->getStructure().getKeysNames();
        column_names.push_back(attr_name);

        auto pipe = dict->read(column_names, helper.getContext()->getSettingsRef()[Setting::max_block_size], 1);
        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        /// The arena will not own any thing, just used for temporary allocations during serialization
        /// of keys. Then rollback after use.
        Arena arena;
        Block block;
        while (executor.pull(block))
        {
            ColumnPtr attr_col = block.getByPosition(keys_cnt).column->convertToFullIfNeeded();

            std::vector<ColumnPtr> key_columns(keys_cnt);
            for (size_t key_pos = 0; key_pos < keys_cnt; ++key_pos)
                key_columns[key_pos] = block.getByPosition(key_pos).column->convertToFullIfNeeded();

            const size_t rows_in_block = attr_col->size();
            for (size_t row_id = 0; row_id < rows_in_block; ++row_id)
            {
                const UInt128 hash = hashAt(*attr_col, row_id);

                const auto * it = hash_to_bucket_id.find(hash);
                if (it == hash_to_bucket_id.end())
                    continue;

                const size_t bucket_id = it->getMapped();
                if (!is_missing[bucket_id])
                    continue;

                auto & mapped = out[bucket_id];
                if (!mapped)
                    mapped = std::make_shared<Mapped>();

                for (size_t key_pos = 0; key_pos < keys_cnt; ++key_pos)
                {
                    const auto & key_col = key_columns[key_pos];
                    const char * begin = nullptr;
                    StringRef ref = key_col->serializeValueIntoArena(row_id, arena, begin);

                    const size_t old_size = mapped->size();
                    const size_t need = old_size + ref.size;

                    if (need > mapped->capacity())
                    {
                        size_t cap = mapped->capacity();
                        if (cap == 0)
                            cap = 64;
                        while (cap < need)
                            cap *= 2;
                        mapped->reserve(cap);
                    }

                    mapped->resize(need);
                    std::memcpy(mapped->data() + old_size, ref.data, ref.size);

                    const size_t alloc = static_cast<size_t>((ref.data - begin) + ref.size);
                    arena.rollback(alloc);
                }
            }
        }
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
