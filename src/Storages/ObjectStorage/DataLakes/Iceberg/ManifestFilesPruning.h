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

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name);

struct PartitionKeyFromSpec
{
    PartitionSpecification partition_specification;
    std::optional<DB::KeyDescription> key_description;
};

/// With keep_nullable the key columns keep the Nullable type of an optional source column.
PartitionKeyFromSpec buildPartitionKeyFromSpec(
    const Poco::JSON::Array::Ptr & partition_specification_json,
    Int32 schema_id,
    const IcebergSchemaProcessor & schema_processor,
    DB::ContextPtr context,
    bool keep_nullable = false);

/// Java and iceberg-rust compute `truncate[W]` of an `int` in 32 bits: a truncated value below INT32_MIN is stored plus 2^32, while
/// `icebergTruncate` returns it as `Int64`. A column promoted to `long` keeps these values; no `long` value truncates to one of them.
/// For W > 2^30 Java's `(x % W) + W` also overflows and stores the same key for some x that truncate to 0, so it is not `exact`.
struct Int32TruncateWraparound
{
    size_t tuple_index;
    Int64 stored;
    Int64 value;
    bool exact;
};

std::vector<Int32TruncateWraparound> getInt32TruncateWraparounds(
    const PartitionSpecification & partition_specification, const IcebergSchemaProcessor & schema_processor, Int32 schema_id);

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
    const DB::KeyDescription * partition_key = nullptr;
    std::optional<DB::KeyCondition> partition_key_condition;
    std::vector<Int32TruncateWraparound> int32_truncate_wraparounds;
    /// The same key over Nullable inputs, for a partition tuple that holds NULL.
    std::optional<DB::KeyDescription> nullable_partition_key;
    std::optional<DB::KeyCondition> nullable_partition_key_condition;

    std::unordered_map<Int32, DB::KeyCondition> min_max_key_conditions;
    /// Float bounds exclude NaN, so these are used for a data file unless its `nan_value_counts` proves the column has none.
    std::unordered_map<Int32, DB::KeyCondition> nan_relaxed_min_max_key_conditions;
    std::unordered_map<Int32, DB::NameAndTypePair> row_lineage_columns;
    std::unordered_map<Int32, DB::DataTypePtr> min_max_column_types;

public:
    /// `partition_key_` may be nullptr and must outlive the pruner.
    /// `require_ready_sets` makes `IN` conditions whose sets are not built yet unknown instead of building them.
    ManifestFilesPruner(
        const IcebergSchemaProcessor & schema_processor_,
        Int32 current_schema_id_,
        Int32 initial_schema_id_,
        const DB::ActionsDAG * filter_dag,
        const DB::KeyDescription * partition_key_,
        const PartitionSpecification & partition_specification,
        DB::ContextPtr context,
        bool require_ready_sets = false);

    [[nodiscard]] PruningReturnStatus canBePruned(const ProcessedManifestFileEntryPtr & entry, const std::unordered_map<Int32, DB::Range> & entry_hyperrectangles) const;

    [[nodiscard]] const std::unordered_map<Int32, DB::DataTypePtr> & getMinMaxColumnTypes() const { return min_max_column_types; }
};

}

#endif
