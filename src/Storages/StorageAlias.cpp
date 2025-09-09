#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/CurrentThread.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}

StorageAlias::StorageAlias(const StorageID & table_id_, const StorageID & ref_table_id_)
    : IStorage(table_id_), ref_table_id(ref_table_id_)
{
    if (table_id_ == ref_table_id_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Alias table cannot refer to itself.");
}

void registerStorageAlias(StorageFactory & factory)
{
    factory.registerStorage("Alias", [](const StorageFactory::Arguments & args)
    {
        if (!args.columns.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "No need to define the schema of Alias table");

        /** Arguments of engine is following:
          * Alias(UUID)
          * or
          * Alias(database.table)
          * or
          * Alias(database, table)
          */

        ASTs & engine_args = args.engine_args;
        if (engine_args.empty() || engine_args.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires 1 or 2 arguments - Alias(UUID) or Alias(database.table) or Alias(database, table), where UUID or database.table is towards the reference table");

        const ContextPtr & local_context = args.getLocalContext();

        if (engine_args.size() == 1)
        {
            engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], local_context);
            auto arg = checkAndGetLiteralArgument<String>(engine_args[0], "database_with_table");
            auto qualified_name = QualifiedTableName::parseFromString(arg);
            /// The definition is Alias(database.table)
            if (!qualified_name.database.empty())
                return std::make_shared<StorageAlias>(args.table_id, StorageID(qualified_name));
            /// The definition is Alias(UUID)
            auto uuid = UUIDHelpers::Nil;
            ReadBufferFromMemory buffer(qualified_name.table.data(), qualified_name.table.size());
            if (!tryReadUUIDText(uuid, buffer))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Definition of Alias table should be either Alias(database.table) or Alias(UUID)");
            return std::make_shared<StorageAlias>(args.table_id, StorageID("", "", uuid));
        }

        /// The definition is Alias(database, table)
        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], local_context);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], local_context);
        auto database = checkAndGetLiteralArgument<String>(engine_args[0], "database");
        auto table = checkAndGetLiteralArgument<String>(engine_args[1], "table");
        return std::make_shared<StorageAlias>(args.table_id, StorageID(database, table));
    },
    {
        .supports_schema_inference = true,
    });
}

StoragePtr StorageAlias::getReferenceTable(ContextPtr context) const
{
    if (ref_table_id.hasUUID())
        return DatabaseCatalog::instance().getByUUID(ref_table_id.uuid).second;
    return DatabaseCatalog::instance().getTable(ref_table_id, context);
}

void StorageAlias::alter(const AlterCommands &, ContextPtr, AlterLockHolder &)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "StorageAlias::alter() is not supported");
}

void StorageAlias::modifyContextByQueryAST(ASTPtr query, ContextMutablePtr context)
{
    if (query->as<ASTRenameQuery>()
        || query->as<ASTShowTablesQuery>()
        || query->as<ASTShowCreateTableQuery>()
        || query->as<ASTShowCreateViewQuery>()
        || query->as<ASTShowCreateDatabaseQuery>()
        || query->as<ASTExistsDatabaseQuery>()
        || query->as<ASTExistsTableQuery>()
        || query->as<ASTExistsViewQuery>()
        || query->as<ASTExistsDictionaryQuery>())
    {
        context->setStorageAliasBehaviour(static_cast<uint8_t>(StorageAliasBehaviourKind::USE_ALIAS_TABLE));
    }
    else if (auto * drop_query = query->as<ASTDropQuery>())
    {
        if (drop_query->kind != ASTDropQuery::Truncate)
            context->setStorageAliasBehaviour(static_cast<uint8_t>(StorageAliasBehaviourKind::USE_ALIAS_TABLE));
        else
            context->setStorageAliasBehaviour(static_cast<uint8_t>(StorageAliasBehaviourKind::EXCEPTION));
    }
    else if (query->as<ASTAlterQuery>())
            context->setStorageAliasBehaviour(static_cast<uint8_t>(StorageAliasBehaviourKind::EXCEPTION));
}

}
