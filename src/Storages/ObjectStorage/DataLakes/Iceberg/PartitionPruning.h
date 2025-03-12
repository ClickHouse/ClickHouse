#pragma once

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

DB::ASTPtr getASTFromTransform(const String & transform_name, const String & column_name);

/// Prune specific data files based on manifest content
class PartitionPruner
{
private:
    const DB::IcebergSchemaProcessor * schema_processor;
    Int32 current_schema_id;
    DB::KeyDescription partition_key;

    std::optional<DB::KeyCondition> key_condition;
    /// NOTE: tricky part to support RENAME column in partition key.
    /// Takes ActionDAG representation of user's WHERE expression and
    /// rename columns to the names which were present in current manifest file
    std::unique_ptr<DB::ActionsDAG> transformFilterDagForManifest(const DB::ActionsDAG * source_dag, Int32 manifest_schema_id, const std::vector<Int32> & partition_column_ids) const;
public:
    PartitionPruner(
        const DB::IcebergSchemaProcessor * schema_processor_,
        Int32 current_schema_id_,
        const DB::ActionsDAG * filter_dag,
        const ManifestFileContent & manifest_file,
        DB::ContextPtr context);

    bool canBePruned(const ManifestFileEntry & entry) const;

};

}
