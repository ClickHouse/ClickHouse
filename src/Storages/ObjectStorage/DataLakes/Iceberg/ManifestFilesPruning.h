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

struct PartitionKeyFromSpec
{
    PartitionSpecification partition_specification;
    std::optional<DB::KeyDescription> key_description;
};

PartitionKeyFromSpec buildPartitionKeyFromSpec(
    const Poco::JSON::Array::Ptr & partition_specification_json,
    Int32 schema_id,
    const IcebergSchemaProcessor & schema_processor,
    DB::ContextPtr context);

/// ClickHouse before 26.9 wrote `truncate[W]` of a negative integer as a value that is not a multiple of `W`;
/// the rows of such a partition have the spec values on both sides of it.
struct IntegerTruncateField
{
    size_t tuple_index;
    Int64 width;
};

std::optional<Int64> getIntegerTruncateWidth(const PartitionSpecsEntry & partition_field, const DB::DataTypePtr & key_type);

std::vector<IntegerTruncateField> getIntegerTruncateFields(
    const PartitionSpecification & partition_specification, const DB::DataTypes & partition_key_types);

/// An old value that is not a multiple of `W` is negative, while a spec value that is not a multiple
/// (the subtraction wrapping near `INT64_MIN`) is positive.
bool isLegacyTruncateValue(const DB::Field & value, Int64 width);

/// NOTE: tricky part to support RENAME column.
/// Takes ActionDAG representation of user's WHERE expression and
/// renames columns to their original numeric ID's in iceberg.
std::unique_ptr<DB::ActionsDAG> renameFilterDagColumnsToFieldIds(
    const IcebergSchemaProcessor & schema_processor,
    Int32 current_schema_id,
    Int32 target_schema_id,
    const DB::ActionsDAG * source_dag,
    std::vector<Int32> & used_columns_in_filter,
    std::unordered_map<Int32, DB::NameAndTypePair> & row_lineage_columns_in_filter);

/// Prune specific data files based on manifest content
class ManifestFilesPruner
{
private:
    const IcebergSchemaProcessor & schema_processor;
    Int32 current_schema_id;
    Int32 initial_schema_id;
    const DB::KeyDescription * partition_key;
    std::optional<DB::KeyCondition> partition_key_condition;
    std::vector<IntegerTruncateField> integer_truncate_fields;

    std::unordered_map<Int32, DB::KeyCondition> min_max_key_conditions;
    std::unordered_map<Int32, DB::NameAndTypePair> row_lineage_columns;
    std::unordered_map<Int32, DB::DataTypePtr> min_max_column_types;

public:
    ManifestFilesPruner(
        const IcebergSchemaProcessor & schema_processor_,
        Int32 current_schema_id_,
        Int32 initial_schema_id_,
        const DB::ActionsDAG * filter_dag,
        const ManifestFileIterator & manifest_file,
        DB::ContextPtr context);

    PruningReturnStatus canBePruned(const ProcessedManifestFileEntryPtr & entry, const std::unordered_map<Int32, DB::Range> & entry_hyperrectangles) const;

    const std::unordered_map<Int32, DB::DataTypePtr> & getMinMaxColumnTypes() const { return min_max_column_types; }
};

}

#endif
