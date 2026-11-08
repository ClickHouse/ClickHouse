#pragma once
#include "config.h"

#if USE_AVRO

#include <Interpreters/ActionsDAG.h>
#include <Poco/JSON/Array.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

class ManifestListPruner
{
public:
    /// `name_mapping` is the table property `schema.name-mapping.default`, nullptr when absent.
    /// `require_ready_sets` makes `IN` conditions whose sets are not built yet unknown instead of building them.
    ManifestListPruner(
        const IcebergSchemaProcessor & schema_processor_,
        Int32 current_schema_id_,
        Int32 partition_schema_id_,
        const Poco::JSON::Array::Ptr & partition_specs,
        const DB::ActionsDAG * filter_dag,
        const DB::IcebergNameMapping * name_mapping,
        DB::ContextPtr context,
        bool require_ready_sets = false);

    bool canBePruned(Int32 partition_spec_id, const PartitionFieldSummaries & partition_summaries) const;

private:
    struct SpecCondition
    {
        DB::KeyDescription partition_key;
        DB::KeyCondition condition;
        /// Index-aligned with the spec fields: the source column is one the name mapping leaves unbound.
        std::vector<bool> unbound_fields;
    };

    std::unordered_map<Int32, SpecCondition> conditions_by_spec_id;
};

}

#endif
