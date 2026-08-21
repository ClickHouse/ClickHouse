#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestListPruning.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>

using namespace DB;

namespace DB::Iceberg
{

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
    auto transformed_dag = renameFilterDagColumnsToFieldIds(
        schema_processor_, current_schema_id_, partition_schema_id_, filter_dag, used_columns_in_filter);

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
            spec->getValue<Int32>(f_spec_id), SpecCondition{std::move(*partition_key.key_description), std::move(condition)});
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
        const auto & summary = partition_summaries[i];
        const auto & type = partition_key.data_types.at(i);

        std::optional<Field> lower;
        std::optional<Field> upper;
        if (!summary.contains_nan && summary.lower_bound.has_value() && !summary.lower_bound->empty()
            && summary.upper_bound.has_value() && !summary.upper_bound->empty())
        {
            lower = deserializeFieldFromBinaryRepr(*summary.lower_bound, type, true);
            upper = deserializeFieldFromBinaryRepr(*summary.upper_bound, type, false);
        }

        left_keys[i] = lower.has_value() ? FieldRef(*lower) : FieldRef(NEGATIVE_INFINITY);
        right_keys[i] = (upper.has_value() && !summary.contains_null) ? FieldRef(*upper) : FieldRef(POSITIVE_INFINITY);
    }

    return !condition_it->second.condition.mayBeTrueInRange(
        partition_summaries.size(), left_keys.data(), right_keys.data(), partition_key.data_types);
}

}

#endif
