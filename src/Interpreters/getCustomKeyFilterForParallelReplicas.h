#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Cluster.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{
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

/// A row with a `NULL` custom key makes the filter `NULL`, so no replica would read it and the row would be silently lost.
/// The type of the filter can be known only after it is resolved against the table expression, so the callers check it.
void checkCustomKeyFilterType(const DataTypePtr & filter_type, const String & custom_key);

ASTPtr parseCustomKeyForTable(const String & custom_keys, const Context & context);

}
