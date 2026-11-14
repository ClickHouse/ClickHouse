#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Common/isValidUTF8.h>

namespace DB
{

#if USE_AVRO

namespace Setting
{
    extern const SettingsUInt64 iceberg_insert_max_partitions;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

ChunkPartitioner::ChunkPartitioner(
    Poco::JSON::Array::Ptr partition_specification, Poco::JSON::Array::Ptr schema_fields, ContextPtr context, SharedHeader sample_block_)
    : sample_block(sample_block_)
    , max_partitions_count(context->getSettingsRef()[Setting::iceberg_insert_max_partitions])
{
    std::unordered_map<Int32, String> id_to_column;
    {
        for (size_t i = 0; i < schema_fields->size(); ++i)
        {
            auto field = schema_fields->getObject(static_cast<UInt32>(i));
            id_to_column[field->getValue<Int32>(Iceberg::f_id)] = field->getValue<String>(Iceberg::f_name);
        }
    }
    for (size_t i = 0; i != partition_specification->size(); ++i)
    {
        auto partition_specification_field = partition_specification->getObject(static_cast<UInt32>(i));

        auto transform_name = partition_specification_field->getValue<String>("transform");
        transform_name = Poco::toLower(transform_name);

        FunctionOverloadResolverPtr transform;

        auto source_id = partition_specification_field->getValue<Int32>(Iceberg::f_source_id);
        auto column_name = id_to_column[source_id];

        auto & factory = FunctionFactory::instance();

        auto transform_and_argument = Iceberg::parseTransformAndArgument(transform_name);
        if (!transform_and_argument)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown transform {}", transform_name);

        auto function = factory.get(transform_and_argument->transform_name, context);

        ColumnsWithTypeAndName columns_for_function;
        if (transform_and_argument->argument)
            columns_for_function.push_back(ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeUInt64>(), ""));
        columns_for_function.push_back(sample_block_->getByName(column_name));

        result_data_types.push_back(function->getReturnType(columns_for_function));
        functions.push_back(function);
        function_params.push_back(transform_and_argument->argument);
        columns_to_apply.push_back(column_name);
    }
}

size_t ChunkPartitioner::PartitionKeyHasher::operator()(const PartitionKey & key) const
{
    size_t result = 0;
    for (const auto & part_key : key)
        result ^= hasher(part_key.dump());
    return result;
}

std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>>
ChunkPartitioner::partitionChunk(const Chunk & chunk)
{
    /// Not `chunk.empty()`: a chunk whose rows were all position-deleted has columns but no rows,
    /// and there is nothing to scatter in it either.
    if (!chunk.hasRows())
        return {};

    std::unordered_map<String, ColumnWithTypeAndName> name_to_column;
    for (size_t i = 0; i < sample_block->columns(); ++i)
    {
        auto column_ptr = chunk.getColumns()[i];
        auto column_name = sample_block->getNames()[i];
        name_to_column[column_name] = ColumnWithTypeAndName(column_ptr, sample_block->getDataTypes()[i], column_name);
    }

    std::vector<ChunkPartitioner::PartitionKey> transform_results(chunk.getNumRows());
    ColumnRawPtrs raw_columns;
    Columns functions_columns;
    for (size_t transform_ind = 0; transform_ind < functions.size(); ++transform_ind)
    {
        ColumnsWithTypeAndName arguments;
        if (function_params[transform_ind].has_value())
        {
            auto type = std::make_shared<DataTypeUInt64>();
            auto column_value = ColumnUInt64::create();
            column_value->insert(*function_params[transform_ind]);
            auto const_column = ColumnConst::create(std::move(column_value), chunk.getNumRows());
            arguments.push_back(ColumnWithTypeAndName(const_column->clone(), type, "#"));
        }
        arguments.push_back(name_to_column[columns_to_apply[transform_ind]]);
        auto result
            = functions[transform_ind]->build(arguments)->execute(arguments, result_data_types[transform_ind], chunk.getNumRows(), false);
        functions_columns.push_back(result);
        raw_columns.push_back(result.get());

        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Field field;
            result->get(i, field);
            transform_results[i].push_back(field);
        }
    }

    auto get_partition = [&](size_t row_num)
    {
        return transform_results[row_num];
    };

    std::vector<std::pair<ChunkPartitioner::PartitionKey, Chunk>> result;
    PODArray<size_t> partition_num_to_first_row;
    MergeTreeDataWriter::PartitionSelector selector;
    /// With 16-bit ids, more than 65536 partitions leave the selector empty.
    /// That is more than `max_partitions_count`, so the check below throws.
    if (max_partitions_count <= 65536)
        selector.emplace<IColumn::SmallSelector>();
    std::visit([&](auto & typed_selector)
    {
        buildScatterSelector(raw_columns, partition_num_to_first_row, typed_selector, 0, Context::getGlobalContextInstance());
    }, selector);

    size_t partitions_count = partition_num_to_first_row.size();
    if (partitions_count > max_partitions_count)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Too many partitions per insert operation: {}. The maximum allowed number of partitions is {}. To change this, use the `iceberg_insert_max_partitions` setting",
            partitions_count,
            max_partitions_count);
    chassert(partitions_count > 0);
    result.reserve(partitions_count);

    std::vector<ChunkPartitioner::PartitionKey> partition_keys;
    partition_keys.reserve(partitions_count);
    for (size_t i = 0; i < partitions_count; ++i)
    {
        auto partition_key = get_partition(partition_num_to_first_row[i]);
        for (size_t transform_ind = 0; transform_ind < partition_key.size(); ++transform_ind)
        {
            /// String partition values are stored as Avro strings in the manifest, which must be UTF-8.
            if (partition_key[transform_ind].getType() != Field::Types::String)
                continue;
            const auto & value = partition_key[transform_ind].safeGet<String>();
            if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Cannot write a partition value that is not valid UTF-8 into Iceberg partition column '{}'",
                    columns_to_apply[transform_ind]);
        }
        partition_keys.push_back(std::move(partition_key));
    }

    if (partitions_count == 1)
    {
        MutableColumns columns;
        columns.reserve(chunk.getNumColumns());
        for (const auto & column : chunk.getColumns())
            columns.push_back(column->cloneFinalized());
        result.push_back({std::move(partition_keys[0]), Chunk(std::move(columns), chunk.getNumRows())});
        return result;
    }

    auto scattered = std::visit(
        [&](const auto & typed_selector) { return scatterColumns(chunk.getColumns(), partitions_count, typed_selector); }, selector);
    for (size_t i = 0; i < partitions_count; ++i)
        result.push_back({std::move(partition_keys[i]), Chunk(std::move(scattered.pieces[i]), scattered.rows[i])});
    return result;
}

void IcebergPartitionCalculator::transform(Chunk & chunk)
{
    /// Position deletes upstream may filter a chunk down to zero rows (`skip_empty_chunks` only
    /// suppresses empty output, it does not shield `transform` from empty input). Skip it: the
    /// partition value is latched from the first non-empty chunk instead.
    if (!chunk.hasRows())
        return;

    if (!partition_value.empty())
        return;

    auto result = partitioner.partitionChunk(chunk);
    if (result.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not calculate partition when chunk have {} partitions", result.size());

    for (const auto & [partition_key, _] : result)
        partition_value = partition_key;
}

#endif

}
