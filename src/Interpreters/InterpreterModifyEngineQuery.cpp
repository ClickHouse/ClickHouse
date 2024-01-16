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
#include <Interpreters/InterpreterFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
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

constexpr const char * const REPLICATED_PREFIX = "Replicated";

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
    engine->name = REPLICATED_PREFIX + storage->engine->name;
    engine->arguments = args;

    /// Set new engine for the old query
    create_query->storage->set(create_query->storage->engine, engine);
}

static void setNotReplicatedEngine(ASTCreateQuery * create_query, ContextPtr)
{
    auto * storage = create_query->storage;

    auto args = std::make_shared<ASTExpressionList>();

    /// Add old engine's arguments without first two
    if (storage->engine->arguments)
    {
        for (size_t i = 2; i < storage->engine->arguments->children.size(); ++i)
            args->children.push_back(storage->engine->arguments->children[i]->clone());
    }

    auto engine = std::make_shared<ASTFunction>();
    engine->name = storage->engine->name.substr(strlen(REPLICATED_PREFIX));
    engine->arguments = args;

    /// Set new engine for the old query
    create_query->storage->set(create_query->storage->engine, engine);
}

static void setNewEngine(ASTCreateQuery * create_query, bool to_replicated, ContextPtr context)
{
    if (to_replicated)
        setReplicatedEngine(create_query, context);
    else
        setNotReplicatedEngine(create_query, context);
}

static void checkEngineChangeIsPossible(ASTStorage * storage, bool to_replicated)
{
    String engine_name = storage->engine->name;

    if (!engine_name.ends_with("MergeTree"))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only MergeTree and ReplicatedMergeTree family engines are supported");

    if (engine_name.starts_with(REPLICATED_PREFIX))
    {
        if (to_replicated)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is already replicated");
    }
    else if (!to_replicated)
       throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is already not replicated");
}

BlockIO InterpreterModifyEngineQuery::execute()
{
    if (!getContext()->getSettingsRef().allow_modify_engine_query)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MODIFY ENGINE query is not allowed while allow_modify_engine_query setting is not set");

    FunctionNameNormalizer().visit(query_ptr.get());
    const auto & query = query_ptr->as<ASTModifyEngineQuery &>();

    getContext()->checkAccess(getRequiredAccess());

    if (!query.cluster.empty())
        /// Doesn't work correctly if converting to replicated while tables on other hosts aren't empty
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Modify engine on cluster is not implemented");

    StorageID table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);

    if (database->getEngineName() != "Atomic")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Table engine conversion is supported only for Atomic databases");

    {
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);
    }

    try {
        /// Get attach query from metadata
        auto table_metadata_path = database->getObjectMetadataPath(table_id.getTableName());
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(nullptr, getContext(), table_metadata_path);
        auto * create_query = ast->as<ASTCreateQuery>();

        checkEngineChangeIsPossible(create_query->storage, query.to_replicated);

        /// Detach table
        String detach_query = fmt::format("DETACH TABLE {} SYNC", table_id.getFullTableName());
        auto res = executeQuery(detach_query, getContext(), { .internal=true });
        executeTrivialBlockIO(res.second, getContext());

        /// Set new engine in the query
        setNewEngine(create_query, query.to_replicated, getContext());

        DatabaseCatalog::instance().removeUUIDMapping(create_query->uuid);

        /// Change metadata
        auto table_metadata_tmp_path = table_metadata_path + ".tmp";

        String statement = getObjectDefinitionFromCreateQuery(ast);
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
        if (query.to_replicated)
        {
            String restore_query = fmt::format("SYSTEM RESTORE REPLICA {}", table_id.getFullTableName());
            res = executeQuery(restore_query, getContext(), { .internal=true });
            executeTrivialBlockIO(res.second, getContext());
        }
    } catch (...) {
        throw;
    }

    return {};
}

AccessRightsElements InterpreterModifyEngineQuery::getRequiredAccess() const
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return {};

    AccessRightsElements required_access;
    const auto & query = query_ptr->as<const ASTModifyEngineQuery &>();

    required_access.emplace_back(AccessType::DROP_TABLE, query.getDatabase(), query.getTable());
    required_access.emplace_back(AccessType::CREATE_TABLE, query.getDatabase(), query.getTable());

    String engine_name = "MergeTree";
    if (query_ptr->as<ASTModifyEngineQuery>()->to_replicated)
        engine_name = REPLICATED_PREFIX + engine_name;

    auto source_access_type = StorageFactory::instance().getSourceAccessType(engine_name);
    if (source_access_type != AccessType::NONE)
        required_access.emplace_back(source_access_type);

    return required_access;
}

void registerInterpreterModifyEngineQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterModifyEngineQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterModifyEngineQuery", create_fn);
}
}
