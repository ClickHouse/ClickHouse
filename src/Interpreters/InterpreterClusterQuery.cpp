#include <algorithm>
#include <csignal>
#include <Access/AllowedClientHosts.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/ClusterWorker.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/InterpreterClusterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Parsers/ASTClusterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ActionLock.h>
#include <Common/DNSResolver.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/typeid_cast.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace DB
{
InterpreterClusterQuery::InterpreterClusterQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_->clone()), log(&Poco::Logger::get("InterpreterClusterQuery"))
{
}

BlockIO InterpreterClusterQuery::execute()
{
    getContext()->checkAccess(AccessType::CLUSTER_NODE);
    return executeClusterQuery(query_ptr, getContext());
}

}
