#pragma once
#include "config.h"

#if USE_AVRO

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>


namespace Iceberg
{

struct ManifestFileEntry;
class ManifestFileContent;

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name);

/// Prune specific data files based on manifest content
class ManifestFilesPruner
{
private:
    const DB::IcebergSchemaProcessor & schema_processor;
    Int32 current_schema_id;
    Int32 manifest_schema_id;
    const DB::KeyDescription * partition_key;
    std::optional<DB::KeyCondition> partition_key_condition;

    std::unordered_map<Int32, DB::KeyCondition> min_max_key_conditions;
    /// NOTE: tricky part to support RENAME column.
    /// Takes ActionDAG representation of user's WHERE expression and
    /// rename columns to the their origina numeric ID's in iceberg
    std::unique_ptr<DB::ActionsDAG> transformFilterDagForManifest(const DB::ActionsDAG * source_dag, std::vector<Int32> & used_columns_in_filter) const;
public:

    ManifestFilesPruner(
        const DB::IcebergSchemaProcessor & schema_processor_,
        Int32 current_schema_id_,
        const DB::ActionsDAG * filter_dag,
        const ManifestFileContent & manifest_file,
        DB::ContextPtr context);

    bool canBePruned(const ManifestFileEntry & entry) const;

};

}

#endif
