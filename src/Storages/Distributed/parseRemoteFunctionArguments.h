#pragma once

#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Common/LoggingFormatStringHelpers.h>


namespace DB
{

/// The result of parsing the arguments of the `remote`/`remoteSecure`/`cluster`/`clusterAllReplicas`
/// table functions or the `Remote`/`RemoteSecure` storage engines. It is everything needed to
/// construct a `StorageDistributed`.
struct ParsedRemoteFunctionArguments
{
    ClusterPtr cluster;
    StorageID remote_table_id = StorageID::createEmpty();
    ASTPtr sharding_key;
    ASTPtr remote_table_function_ptr;
};

/// Parses the arguments shared by the `remote` family of table functions and the `Remote`/`RemoteSecure`
/// storage engines. `args` is the list of argument expressions (may be modified in place during evaluation).
///
/// Lives in `Storages` (the `dbms` library) rather than in `TableFunctions` so that it is available to
/// `StorageDistributed` without pulling in `clickhouse_table_functions`, which is not linked into every
/// target that uses `dbms` (e.g. `unit_tests_dbms`).
///
/// `dependent_table_id` is used only when the addresses are given as a named collection: the persistent
/// `Remote`/`RemoteSecure` engines pass their own `StorageID` so that the table is registered as a dependent
/// of the named collection (blocking `DROP NAMED COLLECTION` while the table exists). Table-function callers
/// leave it null because their lifetime is bound to the query.
ParsedRemoteFunctionArguments parseRemoteFunctionArguments(
    ASTs & args,
    ContextPtr context,
    const std::string & name,
    bool is_cluster_function,
    bool secure,
    const PreformattedMessage & help_message,
    const StorageID * dependent_table_id = nullptr);

}
