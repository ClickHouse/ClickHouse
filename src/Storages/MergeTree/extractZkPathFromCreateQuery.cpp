#include <Storages/MergeTree/extractZkPathFromCreateQuery.h>
#include <Common/Macros.h>
#include <Databases/DatabaseReplicatedHelpers.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>


namespace DB
{

std::optional<String> tryExtractZkPathFromCreateQuery(const IAST & create_query, const ContextPtr & global_context)
{
    const auto * create = create_query.as<const ASTCreateQuery>();
    if (!create || !create->storage || !create->storage->engine)
        return {};

    /// Check if the table engine is one of the ReplicatedMergeTree family.
    const auto & ast_engine = *create->storage->engine;
    if (!ast_engine.name.starts_with("Replicated") || !ast_engine.name.ends_with("MergeTree"))
        return {};

    /// Get the first argument.
    const auto * ast_arguments = typeid_cast<ASTExpressionList *>(ast_engine.arguments.get());
    if (!ast_arguments || ast_arguments->children.empty())
        return {};

    auto * ast_zk_path = typeid_cast<ASTLiteral *>(ast_arguments->children.front().get());
    if (!ast_zk_path || (ast_zk_path->value.getType() != Field::Types::String))
        return {};

    String zk_path = ast_zk_path->value.safeGet<String>();

    /// Expand macros.
    Macros::MacroExpansionInfo info;
    info.table_id.table_name = create->getTable();
    info.table_id.database_name = create->getDatabase();
    info.table_id.uuid = create->uuid;
    auto database = DatabaseCatalog::instance().tryGetDatabase(info.table_id.database_name);
    if (database && database->getEngineName() == "Replicated")
    {
        info.shard = getReplicatedDatabaseShardName(database);
        info.replica = getReplicatedDatabaseReplicaName(database);
    }

    try
    {
        zk_path = global_context->getMacros()->expand(zk_path, info);
    }
    catch (...)
    {
        return {}; /// Couldn't expand macros.
    }

    return zk_path;
}

}
