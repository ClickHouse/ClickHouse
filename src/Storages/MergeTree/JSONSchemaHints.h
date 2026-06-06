#pragma once

#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <unordered_map>

namespace DB
{

/// Parsed schema hint: a "when" condition and a map of path → type name.
struct JSONSchemaHintRule
{
    String when_expression;
    std::unordered_map<String, String> paths; /// path → ClickHouse type name
};

/// Parsed schema hints for a single JSON column.
using JSONSchemaHintRules = std::vector<JSONSchemaHintRule>;

/// All schema hints: column name → list of rules.
using JSONSchemaHints = std::unordered_map<String, JSONSchemaHintRules>;

/// Parse the json_schema_hints setting string into structured form.
/// Throws on invalid JSON or unknown column references.
JSONSchemaHints parseJSONSchemaHints(const String & hints_json);

/// Validate that all `when` expressions in hints only reference columns
/// that appear in the partition key. Throws BAD_ARGUMENTS on violation.
void validateJSONSchemaHints(
    const JSONSchemaHints & hints,
    const StorageMetadataPtr & metadata_snapshot);

/// Evaluate hint rules against partition key values and apply matched hints
/// to ColumnObject columns in the block.
///
/// For the matched rule, each hinted path is pre-created as a Dynamic
/// subcolumn via tryToAddNewDynamicPath, ensuring it gets its own
/// ColumnDynamic storage (won't be squeezed into shared data by
/// max_dynamic_paths). Layer 1 auto-narrowing then stores these
/// homogeneous Dynamic paths as plain typed columns (2 or 4 files
/// instead of ~10).
///
/// @param block - the data block being written (columns may be mutated)
/// @param partition - the partition key values for this block
/// @param hints - parsed schema hints
/// @param metadata_snapshot - table metadata (for partition key expression)
void applyJSONSchemaHints(
    Block & block,
    const MergeTreePartition & partition,
    const JSONSchemaHints & hints,
    const StorageMetadataPtr & metadata_snapshot);

}
