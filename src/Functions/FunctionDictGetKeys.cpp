#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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
#include <Common/HashTable/HashMap.h>

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

static inline UInt64 hashAt(const IColumn & column, size_t row)
{
    SipHash h;
    column.updateHashWithValue(row, h);
    return h.get64();
}

static inline bool equalAt(const IColumn & left_column, size_t left_row_id, const IColumn & right_column, size_t right_row_id)
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

        const String attribute_column_name = checkAndGetColumnConst<ColumnString>(arguments[1].column.get())->getValue<String>();
        auto dict = helper.getDictionary(arguments[0].column);
        const auto & structure = dict->getStructure();
        const auto & attribute_column_type = structure.getAttribute(attribute_column_name).type;
        const auto key_types = structure.getKeyTypes();
        const size_t keys_cnt = key_types.size();

        struct Bucket
        {
            std::vector<MutableColumnPtr> key_cols;
        };

        const bool is_values_column_const = isColumnConst(*arguments[2].column);

        if (is_values_column_const)
        {
            ColumnWithTypeAndName values_column_raw{
                arguments[2].column->cloneResized(1)->convertToFullColumnIfConst(), arguments[2].type, arguments[2].name};
            ColumnPtr values_column = castColumnAccurate(values_column_raw, attribute_column_type)->convertToFullColumnIfLowCardinality();
            const UInt64 const_value_hash = hashAt(*values_column, 0);

            std::vector<Bucket> buckets(1);
            buckets[0].key_cols.reserve(keys_cnt);
            for (const auto & key_type : key_types)
                buckets[0].key_cols.emplace_back(key_type->createColumn());

            Names column_names = structure.getKeysNames();
            column_names.push_back(attribute_column_name);

            auto pipe = dict->read(column_names, helper.getContext()->getSettingsRef()[Setting::max_block_size], 1);
            QueryPipeline pipeline(std::move(pipe));
            PullingPipelineExecutor executor(pipeline);

            Block block;
            while (executor.pull(block))
            {
                ColumnPtr attribute_column = block.getByPosition(keys_cnt).column->convertToFullColumnIfLowCardinality();

                std::vector<ColumnPtr> key_source(keys_cnt);
                for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                    key_source[key_id] = block.getByPosition(key_id).column->convertToFullColumnIfLowCardinality();

                const size_t num_rows_in_block = attribute_column->size();
                for (size_t cur_row_id = 0; cur_row_id < num_rows_in_block; ++cur_row_id)
                {
                    if (hashAt(*attribute_column, cur_row_id) != const_value_hash)
                        continue;

                    /// Different unique attribute values can have same hash, so we need to compare actual values too
                    if (!equalAt(*attribute_column, cur_row_id, *values_column, 0))
                        continue;

                    for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                        buckets[0].key_cols[key_id]->insertFrom(*key_source[key_id], cur_row_id);
                }
            }

            auto offsets_column = ColumnArray::ColumnOffsets::create();
            offsets_column->getData().push_back(buckets[0].key_cols[0]->size());

            if (keys_cnt == 1)
            {
                auto array_column = ColumnArray::create(std::move(buckets[0].key_cols[0]), std::move(offsets_column));
                return ColumnConst::create(std::move(array_column), input_rows_count);
            }

            MutableColumns result_columns;
            result_columns.reserve(keys_cnt);
            for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                result_columns.emplace_back(std::move(buckets[0].key_cols[key_id]));

            auto array_column = ColumnArray::create(ColumnTuple::create(std::move(result_columns)), std::move(offsets_column));
            return ColumnConst::create(std::move(array_column), input_rows_count);
        }

        ColumnWithTypeAndName values_column_raw{arguments[2].column->convertToFullColumnIfConst(), arguments[2].type, arguments[2].name};
        ColumnPtr values_column = castColumnAccurate(values_column_raw, attribute_column_type)->convertToFullColumnIfLowCardinality();

        if (values_column->size() != input_rows_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "After converting to full column, size mismatch in function {}: {} vs {}",
                getName(),
                values_column->size(),
                input_rows_count);

        /// Each unique value of `values_column` (3rd argument) will have a unique bucket.
        /// For each input row, we need to find the bucket id corresponding to its attribute value which is tracked via `row_id_to_bucket_id`.
        /// For example, if input rows 0, 3, 5 of the `values_column` have same value, they will map to same bucket id say 2.

        /// Different values can map to the same hash. As a result, comparing the hash value alone is not
        /// sufficient to confirm if two values are the same. So we maintain the buckets ids of different values that map to the same hash in `hash_to_bucket_ids`.
        /// Then, we iterate over the bucket ids, and get representative value for each bucket and compare with the current value to
        /// to check if the current row's value previousely seen or not.
        /// If yes, we get the bucket id from `row_id_to_bucket_id`. If not, we create a new bucket.

        using BucketIdList = PODArray<UInt64, 2 * sizeof(UInt64)>;
        using Map = HashMap<UInt64, BucketIdList, HashCRC32<UInt64>>;
        Map hash_to_bucket_ids;
        hash_to_bucket_ids.reserve(input_rows_count);

        std::vector<size_t> row_id_to_bucket_id(input_rows_count);

        std::vector<size_t> bucket_id_to_representative_row_id;
        bucket_id_to_representative_row_id.reserve(input_rows_count);
        for (size_t cur_row_id = 0; cur_row_id < input_rows_count; ++cur_row_id)
        {
            const UInt64 hash = hashAt(*values_column, cur_row_id);
            auto & potential_bucket_ids = hash_to_bucket_ids[hash];
            bool previously_seen = false;
            for (size_t bucket_id : potential_bucket_ids)
            {
                const size_t bucket_representative_row_id = bucket_id_to_representative_row_id[bucket_id];
                if (equalAt(*values_column, cur_row_id, *values_column, bucket_representative_row_id))
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


        /// Prepare storage for keys corresponding to each bucket
        const size_t num_buckets = bucket_id_to_representative_row_id.size();

        std::vector<Bucket> buckets(num_buckets);
        for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id)
        {
            buckets[bucket_id].key_cols.reserve(keys_cnt);
            for (const auto & key_type : key_types)
                buckets[bucket_id].key_cols.emplace_back(key_type->createColumn());
        }

        /// Stream dictionary: keys... column + attribute column
        Names column_names = structure.getKeysNames();
        column_names.push_back(attribute_column_name);

        auto pipe = dict->read(column_names, helper.getContext()->getSettingsRef()[Setting::max_block_size], 1);
        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);


        /// For each row in the dictionary, find which bucket it belongs to (based on attribute value) if any,
        /// and insert the corresponding keys into that bucket
        Block block;
        while (executor.pull(block))
        {
            /// Currently, dictionary attribute column cannot be LowCardinality. However, in future if it changes, we should
            /// be able to handle that too.
            ColumnPtr attribute_column = block.getByPosition(keys_cnt).column->convertToFullColumnIfLowCardinality();

            std::vector<ColumnPtr> key_source(keys_cnt);
            for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                key_source[key_id] = block.getByPosition(key_id).column->convertToFullColumnIfLowCardinality();

            const size_t num_rows_in_block = attribute_column->size();
            for (size_t cur_row_id = 0; cur_row_id < num_rows_in_block; ++cur_row_id)
            {
                const UInt64 hash = hashAt(*attribute_column, cur_row_id);

                auto * it = hash_to_bucket_ids.find(hash);
                if (it == hash_to_bucket_ids.end()) /// Not in the `values_column`
                    continue;

                /// We cannot be sure yet that the attribute is part of `values_column`, because multiple unique attribute values can hash to same value
                const auto & potential_bucket_ids = it->getMapped();
                for (size_t bucket_id : potential_bucket_ids)
                {
                    const size_t bucket_representative_row_id = bucket_id_to_representative_row_id[bucket_id];
                    if (!equalAt(*attribute_column, cur_row_id, *values_column, bucket_representative_row_id))
                        continue;

                    for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                        buckets[bucket_id].key_cols[key_id]->insertFrom(*key_source[key_id], cur_row_id);

                    /// Only one bucket can match. We break here and save expensive operations at `equalAt`
                    break;
                }
            }
        }

        MutableColumns result_columns;
        result_columns.reserve(keys_cnt);
        for (const auto & key_type : key_types)
            result_columns.emplace_back(key_type->createColumn());

        auto offsets_column = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_column->getData();
        offsets.resize(input_rows_count);

        size_t position = 0;
        for (size_t row_id = 0; row_id < input_rows_count; ++row_id)
        {
            const size_t bucket_id = row_id_to_bucket_id[row_id];
            const size_t num_matched_keys = buckets[bucket_id].key_cols[0]->size();
            for (size_t key_id = 0; key_id < keys_cnt; ++key_id)
                result_columns[key_id]->insertRangeFrom(*buckets[bucket_id].key_cols[key_id], 0, num_matched_keys);
            position += num_matched_keys;
            offsets[row_id] = position;
        }

        if (keys_cnt == 1)
        {
            return ColumnArray::create(std::move(result_columns[0]), std::move(offsets_column));
        }

        return ColumnArray::create(ColumnTuple::create(std::move(result_columns)), std::move(offsets_column));
    }

private:
    mutable FunctionDictHelper helper;
};


REGISTER_FUNCTION(DictGetKeys)
{
    FunctionDocumentation::Description description = "Inverse dictionary lookup: return keys where attribute equals the given value.";
    FunctionDocumentation::Syntax syntax = "dictGetKeys('dictionary_name', 'attribute_column_name', value_expr)";
    FunctionDocumentation::Arguments arguments
        = {{"dictionary_name", "Name of the dictionary.", {"String"}},
           {"attribute_column_name", "Attribute to match.", {"String"}},
           {"value_expr", "Value to match. Vector or constant. Casts to attribute type.", {}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Array of keys. Element is UInt64 for simple key or Tuple(...) for complex key.", {}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Dictionary;
    FunctionDocumentation docs{description, syntax, arguments, returned_value, {}, introduced_in, category};

    factory.registerFunction<FunctionDictGetKeys>(docs);
}

}
