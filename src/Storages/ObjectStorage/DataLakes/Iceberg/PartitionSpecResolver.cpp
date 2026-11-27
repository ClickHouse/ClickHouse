#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionSpecResolver.h>

#if USE_AVRO

#include <Core/Block.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Common/Exception.h>

#include <Poco/JSON/Array.h>

namespace DB::ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace DB::Iceberg
{

PartitionSpecResolver::PartitionSpecResolver(
    Poco::JSON::Object::Ptr metadata_object_, IcebergSchemaProcessorPtr schema_processor_, ContextPtr context_)
    : metadata_object(metadata_object_)
    , schema_processor(std::move(schema_processor_))
    , context(std::move(context_))
{
}

const PartitionSpecResolver::ResolvedPartitionSpec & PartitionSpecResolver::resolve(Int32 spec_id, Int32 files_schema_id)
{
    if (auto it = resolved_specs.find({spec_id, files_schema_id}); it != resolved_specs.end())
        return it->second;

    const auto partitions_specs = metadata_object->getArray(f_partition_specs);
    const auto schemas = metadata_object->getArray(f_schemas);
    const auto current_schema_id = metadata_object->getValue<Int32>(f_current_schema_id);

    Poco::JSON::Object::Ptr spec;
    for (UInt32 i = 0; i < partitions_specs->size(); ++i)
    {
        auto candidate = partitions_specs->getObject(i);
        if (candidate->getValue<Int64>(f_spec_id) == spec_id)
        {
            spec = candidate;
            break;
        }
    }
    if (!spec)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Iceberg metadata does not contain a partition spec entry matching spec-id {}",
            spec_id);

    ResolvedPartitionSpec resolved;
    resolved.spec = spec;
    auto spec_fields = spec->getArray(f_fields);

    /// Partition field names and the schema source-ids they transform.
    std::vector<Int32> source_ids;
    for (UInt32 i = 0; i < spec_fields->size(); ++i)
    {
        auto spec_field = spec_fields->getObject(i);
        resolved.partition_columns.push_back(spec_field->getValue<String>(f_name));
        source_ids.push_back(spec_field->getValue<Int32>(f_source_id));
    }

    /// Derive partition value types from a schema that defines every source column the spec references, preferring the schema
    /// the source files were written with, then the current one, then any historical one; register all schemas first so they can be queried by id.
    for (UInt32 i = 0; i < schemas->size(); ++i)
        schema_processor->addIcebergTableSchema(schemas->getObject(i));

    auto build_sample_block = [&](Int32 schema_id) -> std::optional<Block>
    {
        auto fields_characteristics = schema_processor->tryGetFieldsCharacteristics(schema_id, source_ids);
        /// A short result means this schema does not define every partition source column.
        if (fields_characteristics.size() != source_ids.size())
            return std::nullopt;
        Block block;
        for (const auto & name_and_type : fields_characteristics)
            block.insert(ColumnWithTypeAndName(name_and_type.type, name_and_type.name));
        return block;
    };

    Int32 schema_id_for_spec = files_schema_id;
    std::optional<Block> spec_sample_block = build_sample_block(schema_id_for_spec);
    if (!spec_sample_block && files_schema_id != current_schema_id)
    {
        schema_id_for_spec = current_schema_id;
        spec_sample_block = build_sample_block(schema_id_for_spec);
    }
    if (!spec_sample_block)
    {
        for (UInt32 i = 0; i < schemas->size(); ++i)
        {
            Int32 candidate_id = schemas->getObject(i)->getValue<Int32>(f_schema_id);
            if (candidate_id == files_schema_id || candidate_id == current_schema_id)
                continue;
            spec_sample_block = build_sample_block(candidate_id);
            if (spec_sample_block)
            {
                schema_id_for_spec = candidate_id;
                break;
            }
        }
    }
    if (!spec_sample_block)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "No Iceberg schema defines all source columns referenced by partition spec {}",
            spec_id);

    auto schema_for_spec = schema_processor->getIcebergTableSchemaById(schema_id_for_spec);
    auto shared_sample_block = std::make_shared<const Block>(std::move(*spec_sample_block));
    resolved.partition_types
        = ChunkPartitioner(spec_fields, schema_for_spec->getArray(f_fields), context, shared_sample_block).getResultTypes();

    return resolved_specs.emplace(std::make_pair(spec_id, files_schema_id), std::move(resolved)).first->second;
}

}

#endif
