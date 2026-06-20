#pragma once
#include "config.h"


#include <Parsers/IAST_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>


namespace DB::Iceberg
{

enum class PruningReturnStatus
{
    NOT_PRUNED,
    PARTITION_PRUNED,
    MIN_MAX_INDEX_PRUNED
};

}

#if USE_AVRO

namespace DB::Iceberg
{

struct ProcessedManifestFileEntry;
class ManifestFileIterator;

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name);

/// Prune specific data files based on manifest content
class ManifestFilesPruner
{
private:
    const IcebergSchemaProcessor & schema_processor;
    Int32 current_schema_id;
    Int32 initial_schema_id;
    const DB::KeyDescription * partition_key;
    std::optional<DB::KeyCondition> partition_key_condition;

    std::unordered_map<Int32, DB::KeyCondition> min_max_key_conditions;
    /// NOTE: tricky part to support RENAME column.
    /// Takes ActionDAG representation of user's WHERE expression and
    /// rename columns to the their origina numeric ID's in iceberg
    std::unique_ptr<DB::ActionsDAG> transformFilterDagForManifest(const DB::ActionsDAG * source_dag, std::vector<Int32> & used_columns_in_filter) const;

public:
    ManifestFilesPruner(
        const IcebergSchemaProcessor & schema_processor_,
        Int32 current_schema_id_,
        Int32 initial_schema_id_,
        const DB::ActionsDAG * filter_dag,
        const ManifestFileIterator & manifest_file,
        DB::ContextPtr context);

    PruningReturnStatus canBePruned(const ProcessedManifestFileEntryPtr & entry, const std::unordered_map<Int32, DB::Range> & entry_hyperrectangles) const;
};

}

#endif
