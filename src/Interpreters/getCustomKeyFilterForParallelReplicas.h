#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Cluster.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

#include <memory>

namespace DB
{

class ASTSelectQuery;
class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct ParallelReplicasCustomKeyFilter
{
    ParallelReplicasMode filter_type;
    UInt64 range_lower;
    UInt64 range_upper;
};

/// Get AST for filter created from custom_key
/// replica_num is the number of the replica for which we are generating filter starting from 0
ASTPtr getCustomKeyFilterForParallelReplica(
    size_t replicas_count,
    size_t replica_num,
    ASTPtr custom_key_ast,
    ParallelReplicasCustomKeyFilter filter,
    const ColumnsDescription & columns,
    const ContextPtr & context);

ASTPtr parseCustomKeyForTable(const String & custom_keys, const Context & context);

/// Custom key parallel replicas skip the merging step on the initiator (via distributed_group_by_no_merge),
/// concatenating per-replica results as-is. This is only correct when every row that shares a GROUP BY key
/// is guaranteed to be processed by a single replica, i.e. when the custom key is a function of the GROUP BY
/// keys (so all rows of a group get the same custom key value and land on the same replica). For queries with
/// aggregation whose GROUP BY keys do not cover the custom key columns (including plain aggregation without
/// GROUP BY, e.g. `SELECT count()`), concatenating per-replica partial results yields wrong results.
///
/// These helpers return true only when skipping the merge is safe: when the custom key is a deterministic
/// function of the GROUP BY key expressions (so every row of a group maps to the same replica). Expression
/// GROUP BY keys are supported (e.g. `GROUP BY mod(number, 3)` with a custom key over `mod(number, 3)`), and
/// non-deterministic/stateful custom keys (e.g. `y + rand()`) are rejected. GROUP BY WITH
/// TOTALS/ROLLUP/CUBE/GROUPING SETS is rejected. `custom_key` is the parsed custom key expression.
bool customKeyResultCanSkipMerge(const ASTSelectQuery & select, const ASTPtr & custom_key, const Context & context);
bool customKeyResultCanSkipMerge(const QueryTreeNodePtr & query_tree, const ASTPtr & custom_key, const Context & context);

}
