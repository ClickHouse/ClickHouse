#include <Interpreters/InterpreterModifyEngineQuery.h>

#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include "Common/escapeForFileName.h"
#include "Common/logger_useful.h"
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include "Interpreters/DatabaseCatalog.h"
#include "Interpreters/StorageID.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ParserCreateQuery.h"
#include "Parsers/parseQuery.h"
#include "Parsers/queryToString.h"
#include <Access/Common/AccessType.h>

#include <boost/algorithm/string/replace.hpp>
#include <fmt/core.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}


InterpreterModifyEngineQuery::InterpreterModifyEngineQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

static void setReplicatedEngine(ASTCreateQuery * create_query, ContextPtr context)
{
    auto * storage = create_query->storage;

    /// Get replicated engine
    const auto & config = context->getConfigRef();
    String replica_path = StorageReplicatedMergeTree::getDefaultZooKeeperPath(config);
    String replica_name = StorageReplicatedMergeTree::getDefaultReplicaName(config);

    auto args = std::make_shared<ASTExpressionList>();
    args->children.push_back(std::make_shared<ASTLiteral>(replica_path));
    args->children.push_back(std::make_shared<ASTLiteral>(replica_name));

    /// Add old engine's arguments
    if (storage->engine->arguments)
    {
        for (size_t i = 0; i < storage->engine->arguments->children.size(); ++i)
            args->children.push_back(storage->engine->arguments->children[i]->clone());
    }

    auto engine = std::make_shared<ASTFunction>();
    engine->name = "Replicated" + storage->engine->name;
    engine->arguments = args;

    /// Set new engine for the old query
    create_query->storage->set(create_query->storage->engine, engine->clone());
}

BlockIO InterpreterModifyEngineQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const auto & query = query_ptr->as<ASTModifyEngineQuery &>();

    if (!query.cluster.empty())
        /// Doesn't work correctly if converting to replicated while tables on other hosts aren't empty
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Modify engine on cluster is not implemented.");


    StorageID table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);

    {
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);
    }

    try {
        auto * log = &Poco::Logger::get("InterpreterModifyEngineQuery");

        /// Detach table
        String detach_query = fmt::format("DETACH TABLE {} SYNC", table_id.getFullTableName());
        auto res = executeQuery(detach_query, getContext(), { .internal=true });
        executeTrivialBlockIO(res.second, getContext());

        /// Get attach query from metadata
        auto table_metadata_path = database->getObjectMetadataPath(table_id.getTableName());
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(log, getContext(), table_metadata_path);//database->getCreateTableQuery(table_id.getTableName(), getContext());
        auto * create_query = ast->as<ASTCreateQuery>();
        setReplicatedEngine(create_query, getContext());
        LOG_INFO(log, "Create query {}", create_query->formatForLogging());
        DatabaseCatalog::instance().removeUUIDMapping(create_query->uuid);

        /// Change metadata
        auto table_metadata_tmp_path = table_metadata_path + ".tmp";

        LOG_INFO(log, "Metadata path {}", table_metadata_path);
        String statement = getObjectDefinitionFromCreateQuery(ast);
        LOG_INFO(log, "Create query in metadata {}", statement);
        {
            WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
            writeString(statement, out);
            out.next();
            if (getContext()->getSettingsRef().fsync_metadata)
                out.sync();
            out.close();
        }
        fs::rename(table_metadata_tmp_path, table_metadata_path);

        /// Attach table
        String attach_query = fmt::format("ATTACH TABLE {}", table_id.getFullTableName());
        res = executeQuery(attach_query, getContext(), { .internal=true });
        executeTrivialBlockIO(res.second, getContext());

        /// If engine is ReplicatedMergeTree, restore metadata in zk
        String restore_query = fmt::format("SYSTEM RESTORE REPLICA {}", table_id.getFullTableName());
        res = executeQuery(restore_query, getContext(), { .internal=true });
        executeTrivialBlockIO(res.second, getContext());
    } catch (...) {
        throw;
    }

    return {};
}

void InterpreterModifyEngineQuery::checkEngineChangeIsPossible(StoragePtr)
{
    // String target_engine_name = query_ptr->as<ASTModifyEngineQuery &>().storage->as<ASTStorage &>().engine->name;

    // if (auto table_merge_tree = dynamic_pointer_cast<StorageMergeTree>(table))
    // {
    //     if (target_engine_name != "MergeTree" && target_engine_name != "ReplicatedMergeTree")
    //         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Converting from MergeTree to {} is not implemented", target_engine_name);
    //     auto unfinished_mutations = table_merge_tree->getUnfinishedMutationCommands();
    //     if (!unfinished_mutations.empty())
    //         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Modify engine while there are unfinished mutations left is prohibited");
    // }
    // else if (auto table_replicated_merge_tree = dynamic_pointer_cast<StorageReplicatedMergeTree>(table))
    // {
    //     if (target_engine_name != "MergeTree" && target_engine_name != "ReplicatedMergeTree")
    //         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Converting from ReplicatedMergeTree to {} is not implemented", target_engine_name);
    //     auto unfinished_mutations = table_replicated_merge_tree->getUnfinishedMutationCommands();
    //     if (!unfinished_mutations.empty())
    //         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Modify engine while there are unfinished mutations left is prohibited");
    // }
    // else
    //     throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only MergeTree family tables are supported");
}

AccessRightsElements InterpreterModifyEngineQuery::getRequiredAccess() const
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return {};

    AccessRightsElements required_access;
    const auto & query = query_ptr->as<const ASTModifyEngineQuery &>();

    String temp_name = query.getTable() + "_temp";

    required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, query.getDatabase(), query.getTable());
    required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, query.getDatabase(), temp_name);
    required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, query.getDatabase(), query.getTable());
    required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, query.getDatabase(), temp_name);

    // if (query.storage)
    // {
    //     auto & storage = query.storage->as<ASTStorage &>();
    //     auto source_access_type = StorageFactory::instance().getSourceAccessType(storage.engine->name);
    //     if (source_access_type != AccessType::NONE)
    //         required_access.emplace_back(source_access_type);
    // }

    return required_access;
}

}
