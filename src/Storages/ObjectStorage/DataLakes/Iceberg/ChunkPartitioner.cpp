#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

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
}

ChunkPartitioner::ChunkPartitioner(
    Poco::JSON::Array::Ptr partition_specification, Poco::JSON::Object::Ptr schema, ContextPtr context, SharedHeader sample_block_)
    : sample_block(sample_block_)
    , max_partitions_count(context->getSettingsRef()[Setting::iceberg_insert_max_partitions])
{
    std::unordered_map<Int32, String> id_to_column;
    {
        auto schema_fields = schema->getArray(Iceberg::f_fields);
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
            = functions[transform_ind]->build(arguments)->execute(arguments, std::make_shared<DataTypeString>(), chunk.getNumRows(), false);
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
    IColumn::Selector selector;

    buildScatterSelector(raw_columns, partition_num_to_first_row, selector, 0, Context::getGlobalContextInstance());


    size_t partitions_count = partition_num_to_first_row.size();
    if (partitions_count > max_partitions_count)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Too many partitions per insert operation: {}. The maximum allowed number of partitions is {}. To change this, use the `iceberg_insert_max_partitions` setting",
            partitions_count,
            max_partitions_count);
    chassert(partitions_count > 0);
    std::vector<std::pair<ChunkPartitioner::PartitionKey, MutableColumns>> result_columns;
    result_columns.reserve(partitions_count);

    for (size_t i = 0; i < partitions_count; ++i)
        result_columns.push_back({get_partition(partition_num_to_first_row[i]), chunk.cloneEmptyColumns()});

    for (size_t col = 0; col < chunk.getNumColumns(); ++col)
    {
        if (partitions_count > 1)
        {
            MutableColumns scattered = chunk.getColumns()[col]->scatter(partitions_count, selector);
            for (size_t i = 0; i < partitions_count; ++i)
                result_columns[i].second[col] = std::move(scattered[i]);
        }
        else
        {
            result_columns[0].second[col] = chunk.getColumns()[col]->cloneFinalized();
        }
    }
    for (auto && [key, partition_columns] : result_columns)
    {
        size_t column_size = partition_columns[0]->size();
        result.push_back({key, Chunk(std::move(partition_columns), column_size)});
    }
    return result;
}

#endif

}
