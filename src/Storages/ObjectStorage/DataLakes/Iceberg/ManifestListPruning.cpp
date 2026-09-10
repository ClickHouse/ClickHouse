#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestListPruning.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>

#include <Common/FieldAccurateComparison.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeNullable.h>

using namespace DB;

namespace DB::Iceberg
{

namespace
{

std::optional<Int64> decodeSignedInteger(const String & bytes)
{
    if (bytes.empty() || bytes.size() > sizeof(Int64))
        return {};

    UInt64 value = (bytes.back() & 0x80) ? ~UInt64(0) : UInt64(0);
    for (size_t i = bytes.size(); i > 0; --i)
        value = (value << 8) | static_cast<UInt8>(bytes[i - 1]);

    return static_cast<Int64>(value);
}

std::optional<Field> deserializeUnsignedBound(const String & bytes, const IDataType & type)
{
    const auto value = decodeSignedInteger(bytes);
    if (!value.has_value() || *value < 0)
        return {};

    const size_t type_bits = 8 * type.getSizeOfValueInMemory();
    if (type_bits < 8 * sizeof(UInt64) && (static_cast<UInt64>(*value) >> type_bits) != 0)
        return {};

    return Field(static_cast<UInt64>(*value));
}

std::optional<std::pair<Field, Field>> boundsOfPartitionFieldSummary(
    const PartitionFieldSummary & summary, const DataTypePtr & type, Int32 partition_spec_id, size_t field_index)
{
    if (summary.contains_null || summary.contains_nan || !summary.lower_bound.has_value() || summary.lower_bound->empty()
        || !summary.upper_bound.has_value() || summary.upper_bound->empty())
        return {};

    const auto non_nullable_type = removeNullable(type);

    std::optional<Field> lower;
    std::optional<Field> upper;
    if (WhichDataType(non_nullable_type).isUInt())
    {
        lower = deserializeUnsignedBound(*summary.lower_bound, *non_nullable_type);
        upper = deserializeUnsignedBound(*summary.upper_bound, *non_nullable_type);
    }
    else
    {
        lower = deserializeFieldFromBinaryRepr(*summary.lower_bound, type, true);
        upper = deserializeFieldFromBinaryRepr(*summary.upper_bound, type, false);
    }

    if (!lower.has_value() || !upper.has_value())
        return {};

    if (accurateLess(*upper, *lower))
    {
        LOG_WARNING(
            getLogger("ManifestListPruner"),
            "Manifest list declares a lower bound above the upper bound for field {} of partition spec {}; skipping "
            "partition pruning for this field",
            field_index,
            partition_spec_id);
        return {};
    }

    return std::pair{std::move(*lower), std::move(*upper)};
}

}

ManifestListPruner::ManifestListPruner(
    const IcebergSchemaProcessor & schema_processor_,
    Int32 current_schema_id_,
    Int32 partition_schema_id_,
    const Poco::JSON::Array::Ptr & partition_specs,
    const DB::ActionsDAG * filter_dag,
    DB::ContextPtr context)
{
    if (filter_dag == nullptr || !partition_specs || partition_specs->size() == 0)
        return;

    std::vector<Int32> used_columns_in_filter;
    std::unordered_map<Int32, DB::NameAndTypePair> row_lineage_columns_in_filter;
    auto transformed_dag = renameFilterDagColumnsToFieldIds(
        schema_processor_, current_schema_id_, partition_schema_id_, filter_dag, used_columns_in_filter, row_lineage_columns_in_filter);

    for (UInt32 i = 0; i < partition_specs->size(); ++i)
    {
        auto spec = partition_specs->getObject(i);
        if (!spec->has(f_spec_id) || !spec->has(f_fields))
            continue;

        auto fields = spec->get(f_fields).extract<Poco::JSON::Array::Ptr>();
        if (!fields || fields->size() == 0)
            continue;

        auto partition_key = buildPartitionKeyFromSpec(fields, partition_schema_id_, schema_processor_, context);
        if (!partition_key.key_description.has_value() || partition_key.key_description->data_types.size() != fields->size())
            continue;

        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context, /* boolean_context */ true);
        DB::KeyCondition condition(
            inverted_dag, context, partition_key.key_description->column_names, partition_key.key_description->expression);
        conditions_by_spec_id.emplace(
            spec->getValue<Int32>(f_spec_id), SpecCondition{*partition_key.key_description, std::move(condition)});
    }
}

bool ManifestListPruner::canBePruned(Int32 partition_spec_id, const PartitionFieldSummaries & partition_summaries) const
{
    if (partition_summaries.empty())
        return false;

    auto condition_it = conditions_by_spec_id.find(partition_spec_id);
    if (condition_it == conditions_by_spec_id.end())
        return false;

    const auto & partition_key = condition_it->second.partition_key;
    if (partition_key.data_types.size() != partition_summaries.size())
        return false;

    std::vector<FieldRef> left_keys(partition_summaries.size());
    std::vector<FieldRef> right_keys(partition_summaries.size());
    for (size_t i = 0; i < partition_summaries.size(); ++i)
    {
        auto bounds
            = boundsOfPartitionFieldSummary(partition_summaries[i], partition_key.data_types.at(i), partition_spec_id, i);

        left_keys[i] = bounds.has_value() ? FieldRef(bounds->first) : FieldRef(NEGATIVE_INFINITY);
        right_keys[i] = bounds.has_value() ? FieldRef(bounds->second) : FieldRef(POSITIVE_INFINITY);
    }

    return !condition_it->second.condition.mayBeTrueInRange(
        partition_summaries.size(), left_keys.data(), right_keys.data(), partition_key.data_types);
}

}

#endif
