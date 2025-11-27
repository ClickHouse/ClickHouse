#include <Columns/ColumnSparse.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Core/Types.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Functions/IFunction.h>

#include <Interpreters/Cache/ReverseLookupCache.h>
#include <Interpreters/Context.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>

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

inline UInt128 sipHash128AtRow(const IColumn & column, size_t row_id)
{
    SipHash h;
    column.updateHashWithValue(row_id, h);
    return h.get128();
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

    bool useDefaultImplementationForConstants() const final { return false; }

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
        static_assert(sizeof(SerializedKeys::value_type) == 1, "SerializedKeys must store raw bytes");

        if (input_rows_count == 0)
            return result_type->createColumn();

        const auto * dict_name_const_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        const auto * attr_name_const_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

        chassert(dict_name_const_col);
        chassert(attr_name_const_col);

        const String dict_name = dict_name_const_col->getValue<String>();
        const String attr_name = attr_name_const_col->getValue<String>();

        if (isColumnConst(*arguments[2].column))
        {
            return executeConstPath(dict_name, attr_name, arguments[2], input_rows_count);
        }

        return executeVectorPath(dict_name, attr_name, arguments[2], input_rows_count);
    }

private:
    mutable FunctionDictHelper helper;

    using HashToBucket = HashMap<UInt128, size_t, HashCRC32<UInt128>>;

    /// For constant path, it's simple algorithm:
    ///  Step 1. Compute the hash of the const value column.
    ///  Step 2. Scan the dictionary and store the matching rows keys directly into the result column.
    ///  Step 3. Format the result column into appropriate format: tuple for multi-key dictionary or single value otherwise.
    ColumnPtr executeConstPath(
        const String & dict_name,
        const String & attr_name,
        const ColumnWithTypeAndName & argument_values_column,
        size_t input_rows_count) const
    {
        auto dict = helper.getDictionary(dict_name);
        chassert(dict != nullptr);

        const auto & structure = dict->getStructure();
        const auto & attribute_column_type = structure.getAttribute(attr_name).type;
        ColumnPtr values_column = castColumnAccurate(argument_values_column, attribute_column_type);

        chassert(values_column != nullptr);
        chassert(!values_column->empty());

        /// Step 1
        const UInt128 values_column_value_hash = sipHash128AtRow(*values_column, 0);

        /// Step 2
        MutableColumns result_cols;
        const auto key_types = structure.getKeyTypes();
        chassert(!key_types.empty());

        for (const auto & key_type : key_types)
        {
            auto col = key_type->createColumn();
            result_cols.emplace_back(std::move(col));
        }

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_col->getData();
        offsets.resize(1);

        const size_t num_keys = key_types.size();

        Names column_names = structure.getKeysNames();
        column_names.push_back(attr_name);

        auto pipe = dict->read(column_names, helper.getContext()->getSettingsRef()[Setting::max_block_size], 1);
        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        Block block;
        size_t out_offset = 0;
        while (executor.pull(block))
        {
            ColumnPtr attr_col = removeSpecialRepresentations(block.getByPosition(num_keys).column);

            std::vector<ColumnPtr> key_columns(num_keys);
            for (size_t key_pos = 0; key_pos < num_keys; ++key_pos)
                key_columns[key_pos] = removeSpecialRepresentations(block.getByPosition(key_pos).column);

            const size_t rows_in_block = attr_col->size();
            for (size_t row_id = 0; row_id < rows_in_block; ++row_id)
            {
                const UInt128 value_hash = sipHash128AtRow(*attr_col, row_id);

                /// Probability of hash collision of Sip128 is extremely astronomically low. As a result, for the sake of simplicity and efficiency,
                /// let's assume it never happens
                if (value_hash != values_column_value_hash)
                    continue;

                for (size_t key_pos = 0; key_pos < num_keys; ++key_pos)
                {
                    result_cols[key_pos]->insertFrom(*key_columns[key_pos], row_id);
                }
                ++out_offset;
            }
        }
        offsets[0] = out_offset;

        /// Step 3
        if (num_keys == 1)
        {
            auto array_column = ColumnArray::create(std::move(result_cols[0]), std::move(offsets_col));
            return ColumnConst::create(std::move(array_column), input_rows_count);
        }

        auto array_column = ColumnArray::create(ColumnTuple::create(std::move(result_cols)), std::move(offsets_col));
        return ColumnConst::create(std::move(array_column), input_rows_count);
    }

    /// Here's the algorithm:
    ///   Step 1. Assign each unique element of the `values_column` to a unique `bucket`. If two elements belong to the same bucket,
    ///           it implies they are the same (to be precise, their hash are the same).
    ///   Step 2. Check which bucket results can already be found in the shared Cache and store their result locally in `bucket_cached_bytes`. Create an array
    ///           bucket_ids which are not available in the Cache.
    ///   Step 3. Scan the dictionary to get the result for the missing buckets, update the Cache and also update the local `bucket_cached_bytes`.
    ///   Step 4. Unpack the `bucket_cached_bytes` to IColumn format column `results_cols`. Storing IColumn format per key in the Cache is
    ///           is very expensive; so, we only store the raw bytes in the form of `SerializedKeysPtr`.
    ///   Step 5. Format the result column into appropriate format: tuple for multi-key dictionary or single value otherwise.
    ColumnPtr executeVectorPath(
        const String & dict_name,
        const String & attr_name,
        const ColumnWithTypeAndName & argument_values_column,
        size_t input_rows_count) const
    {
        auto dict = helper.getDictionary(dict_name);
        chassert(dict != nullptr);

        const auto & structure = dict->getStructure();
        const auto & attribute_column_type = structure.getAttribute(attr_name).type;
        ColumnPtr values_column = castColumnAccurate(argument_values_column, attribute_column_type)->convertToFullIfNeeded();

        chassert(values_column != nullptr);
        chassert(values_column->size() == input_rows_count);

        /// Step 1
        HashToBucket value_hash_to_bucket_id;
        value_hash_to_bucket_id.reserve(input_rows_count);

        SipHash sip;
        sip.update(dict_name.data(), dict_name.size());
        sip.update(attr_name.data(), attr_name.size());
        const UInt128 domain_id = sip.get128();

        std::vector<size_t> row_id_to_bucket_id(input_rows_count);

        size_t num_buckets = 0;
        std::vector<UInt128> bucket_value_hashes;
        bucket_value_hashes.reserve(input_rows_count);

        for (size_t cur_row_id = 0; cur_row_id < input_rows_count; ++cur_row_id)
        {
            const UInt128 value_hash = sipHash128AtRow(*values_column, cur_row_id);

            auto * it = value_hash_to_bucket_id.find(value_hash);
            if (it)
            {
                row_id_to_bucket_id[cur_row_id] = it->getMapped();
            }
            else
            {
                const size_t new_bucket_id = num_buckets++;
                value_hash_to_bucket_id[value_hash] = new_bucket_id;
                row_id_to_bucket_id[cur_row_id] = new_bucket_id;
                bucket_value_hashes.push_back(value_hash);
            }
        }

        /// Step 2
        auto & cache = helper.getContext()->getQueryContext()->getReverseLookupCache();
        std::vector<SerializedKeysPtr> bucket_cached_bytes(num_buckets);
        std::vector<size_t> missing_bucket_ids;
        missing_bucket_ids.reserve(num_buckets);

        chassert(bucket_value_hashes.size() == num_buckets);

        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
        {
            chassert(bucket_id < bucket_value_hashes.size());
            CacheKey key{domain_id, bucket_value_hashes[bucket_id]};
            if (auto hit = cache.get(key))
                bucket_cached_bytes[bucket_id] = hit;
            else
                missing_bucket_ids.push_back(bucket_id);
        }

        /// Step 3
        const auto key_types = structure.getKeyTypes();
        chassert(!key_types.empty());

        if (!missing_bucket_ids.empty())
        {
            fillMissingBucketsFromDict(dict, attr_name, key_types, bucket_cached_bytes, missing_bucket_ids, value_hash_to_bucket_id);

            for (size_t bucket_id : missing_bucket_ids)
            {
                if (!bucket_cached_bytes[bucket_id])
                    bucket_cached_bytes[bucket_id] = std::make_shared<SerializedKeys>();
                CacheKey key{domain_id, bucket_value_hashes[bucket_id]};
                if (!cache.contains(key))
                {
                    cache.set(key, bucket_cached_bytes[bucket_id]);
                }
            }
        }


        /// Step 4
        const size_t num_keys = key_types.size();
        MutableColumns result_cols;
        result_cols.reserve(num_keys);
        for (const auto & key_type : key_types)
        {
            auto col = key_type->createColumn();
            col->reserve(input_rows_count);
            result_cols.emplace_back(std::move(col));
        }

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_col->getData();
        offsets.resize(input_rows_count);

        /// For each bucket, it's very expensive to repeatedly deserialize from cached_bytes and construct IColumn elements.
        /// So, for each bucket, we only deserialize once and store the position of the deserialized slice in `result_cols`.
        /// Then, for the next time this bucket is seen, we can directly copy from `result_cols` which is very efficient.
        std::vector<size_t> bucket_start_offset(num_buckets, std::numeric_limits<size_t>::max());
        std::vector<size_t> bucket_row_count(num_buckets, 0);

        size_t out_offset = 0;
        for (size_t row_id = 0; row_id < input_rows_count; ++row_id)
        {
            const size_t bucket_id = row_id_to_bucket_id[row_id];
            chassert(bucket_id < num_buckets);

            /// No matching rows in the dictionary for this bucket
            if (!bucket_cached_bytes[bucket_id])
            {
                offsets[row_id] = out_offset;
                continue;
            }

            size_t start = bucket_start_offset[bucket_id];
            size_t len = bucket_row_count[bucket_id];

            /// This means we have already decoded this bucket before. We can directly copy from result_cols (faster
            /// than deserializing again).
            if (start != std::numeric_limits<size_t>::max())
            {
                if (len)
                {
                    for (size_t key_pos = 0; key_pos < num_keys; ++key_pos)
                        result_cols[key_pos]->insertRangeFrom(*result_cols[key_pos], start, len);
                    out_offset += len;
                }
                offsets[row_id] = out_offset;
                continue;
            }

            /// Need to decode from cached bytes. This is slow but happens only once per bucket.
            const auto & cached_bytes_ptr = bucket_cached_bytes[bucket_id];
            chassert(cached_bytes_ptr != nullptr);

            const auto & cached_bytes = *cached_bytes_ptr;
            if (cached_bytes.empty())
            {
                bucket_start_offset[bucket_id] = out_offset;
                bucket_row_count[bucket_id] = 0;
                offsets[row_id] = out_offset;
                continue;
            }

            const size_t before = out_offset;
            DB::ReadBufferFromMemory in(reinterpret_cast<const char *>(cached_bytes.data()), cached_bytes.size());
            while (!in.eof())
            {
                for (size_t key_pos = 0; key_pos < num_keys; ++key_pos)
                    result_cols[key_pos]->deserializeAndInsertFromArena(in);

                ++out_offset;
            }

            chassert(in.count() == cached_bytes.size());

            bucket_start_offset[bucket_id] = before;
            bucket_row_count[bucket_id] = out_offset - before;
            offsets[row_id] = out_offset;
        }

        /// Step 5
        if (num_keys == 1)
        {
            return ColumnArray::create(std::move(result_cols[0]), std::move(offsets_col));
        }

        return ColumnArray::create(ColumnTuple::create(std::move(result_cols)), std::move(offsets_col));
    }

    /// This is similar to `executeConstPath`. If the dictionary row matches and is needed, then store its value.
    template <class DictionaryPtr>
    void fillMissingBucketsFromDict(
        const DictionaryPtr & dict,
        const String & attr_name,
        const DataTypes & key_types,
        std::vector<SerializedKeysPtr> & out,
        const std::vector<size_t> & missing_bucket_ids,
        const HashToBucket & value_hash_to_bucket_id) const
    {
        std::vector<UInt8> is_missing(out.size(), 0);
        for (size_t id : missing_bucket_ids)
        {
            chassert(id < out.size());
            is_missing[id] = 1;
        }

        const size_t num_keys = key_types.size();

        Names column_names = dict->getStructure().getKeysNames();
        chassert(column_names.size() == num_keys);
        column_names.push_back(attr_name);

        auto pipe = dict->read(column_names, helper.getContext()->getSettingsRef()[Setting::max_block_size], 1);
        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        /// The arena will not own anything, just used for temporary allocations during serialization
        /// of keys. Then rollback after use to free memory for next use.
        Arena arena;
        Block block;
        while (executor.pull(block))
        {
            chassert(block.columns() >= num_keys + 1);

            ColumnPtr attr_col = removeSpecialRepresentations(block.getByPosition(num_keys).column);
            const size_t rows_in_block = attr_col->size();

            std::vector<ColumnPtr> key_columns(num_keys);
            for (size_t key_pos = 0; key_pos < num_keys; ++key_pos)
            {
                key_columns[key_pos] = removeSpecialRepresentations(block.getByPosition(key_pos).column);
                chassert(key_columns[key_pos]->size() == rows_in_block);
            }

            for (size_t row_id = 0; row_id < rows_in_block; ++row_id)
            {
                const UInt128 value_hash = sipHash128AtRow(*attr_col, row_id);

                /// Not in user given `values_column`
                const auto * it = value_hash_to_bucket_id.find(value_hash);
                if (it == value_hash_to_bucket_id.end())
                    continue;

                const size_t bucket_id = it->getMapped();

                chassert(bucket_id < out.size());

                /// In user given `values_column` but not needed
                if (!is_missing[bucket_id])
                    continue;

                auto & mapped = out[bucket_id];
                if (!mapped)
                    mapped = std::make_shared<SerializedKeys>();

                for (size_t key_pos = 0; key_pos < num_keys; ++key_pos)
                {
                    const auto & key_col = key_columns[key_pos];
                    const char * begin = nullptr;
                    auto ref = key_col->serializeValueIntoArena(row_id, arena, begin);

                    chassert(begin != nullptr);
                    chassert(ref.data() >= begin);

                    const size_t old_size = mapped->size();
                    const size_t need = old_size + ref.size();

                    /// PODArray has geometric growth with reserve. This is important.
                    /// Otherwise, each repeated incremental `resize()` will cause
                    /// repeated reallocations and copy which is very inefficient.
                    mapped->reserve(need);
                    mapped->resize_assume_reserved(need);

                    std::memcpy(mapped->data() + old_size, ref.data(), ref.size());

                    const size_t alloc = static_cast<size_t>((ref.data() - begin) + ref.size());

                    /// This is important to rollback otherwise we will have double memory consumption.
                    /// Additionally, just used memory is now hot in CPU cache which speeds up next serialization.
                    [[maybe_unused]] void * rollback_ptr = arena.rollback(alloc);
                    chassert(rollback_ptr == static_cast<const void *>(begin));
                }
            }
        }
        /// Ideally, we should be `shrink_to_fit` each `mapped` in `out` here to save memory.
        /// However, since saved memory is typically small, we skip it for performance consideration.
    }
};


REGISTER_FUNCTION(DictGetKeys)
{
    FunctionDocumentation::Description description = R"(
Returns the dictionary key(s) whose attribute equals the specified value. This is the inverse of the function `dictGet` on a single attribute.

Use setting `max_reverse_dictionary_lookup_cache_size_bytes` to cap the size of the per-query reverse-lookup cache used by `dictGetKeys`.
The cache stores serialized key tuples for each attribute value to avoid re-scanning the dictionary within the same query.
The cache is not persistent across queries. When the limit is reached, entries are evicted with LRU.
This is most effective with large dictionaries when the input has low cardinality and the working set fits in the cache. Set to `0` to disable caching.
    )";
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
    FunctionDocumentation::Examples examples
        = {{"Sample usage",
            R"(
SELECT dictGetKeys('task_id_to_priority_dictionary', 'priority_level', 'high') AS ids;
    )",
            R"(
┌─-ids──┐
│ [4,2] │
└───────┘
    )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Dictionary;
    FunctionDocumentation docs{description, syntax, arguments, returned_value, {}, introduced_in, category};

    factory.registerFunction<FunctionDictGetKeys>(docs);
}
}
