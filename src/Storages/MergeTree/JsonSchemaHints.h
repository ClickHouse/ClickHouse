#pragma once

#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <unordered_map>

namespace DB
{

/// Parsed schema hint: a "when" condition and a map of path → type name.
struct JsonSchemaHintRule
{
    String when_expression;
    std::unordered_map<String, String> paths; /// path → ClickHouse type name
};

/// Parsed schema hints for a single JSON column.
using JsonSchemaHintRules = std::vector<JsonSchemaHintRule>;

/// All schema hints: column name → list of rules.
using JsonSchemaHints = std::unordered_map<String, JsonSchemaHintRules>;

/// Parse the json_schema_hints setting string into structured form.
/// Throws on invalid JSON or unknown column references.
JsonSchemaHints parseJsonSchemaHints(const String & hints_json);

/// Evaluate hint rules against partition key values and apply matched hints
/// to ColumnObject columns in the block. Pre-creates Dynamic paths for
/// hinted paths, ensuring they get their own ColumnDynamic subcolumn
/// (instead of potentially falling into shared data).
///
/// @param block - the data block being written (columns may be mutated)
/// @param partition - the partition key values for this block
/// @param hints - parsed schema hints
/// @param metadata_snapshot - table metadata (for partition key expression)
void applyJsonSchemaHints(
    Block & block,
    const MergeTreePartition & partition,
    const JsonSchemaHints & hints,
    const StorageMetadataPtr & metadata_snapshot);

}
