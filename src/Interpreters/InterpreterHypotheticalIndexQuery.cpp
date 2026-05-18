#include <Interpreters/InterpreterHypotheticalIndexQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTHypotheticalIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Storages/IStorage.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

BlockIO InterpreterHypotheticalIndexQuery::execute()
{
    const auto & query = query_ptr->as<ASTHypotheticalIndexQuery &>();
    auto context = getContext();

    if (query.kind == ASTHypotheticalIndexQuery::DropAll)
    {
        context->getHypotheticalIndexStore().clear();
        return {};
    }

    auto table_id = context->resolveStorageID(StorageID(query.getDatabase(), query.getTable()));
    auto table = DatabaseCatalog::instance().getTable(table_id, context);

    context->checkAccess(AccessType::SELECT, table_id);

    if (!dynamic_cast<const MergeTreeData *>(table.get()))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes are only supported for MergeTree family tables, got {}",
            table->getName());

    if (table_id.uuid == UUIDHelpers::Nil)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes require a table with a stable UUID; {}.{} has none "
            "(legacy `Ordinary` databases are not supported)",
            table_id.getDatabaseName(),
            table_id.getTableName());

    auto & store = context->getHypotheticalIndexStore();

    if (query.kind == ASTHypotheticalIndexQuery::Drop)
    {
        auto index_name = query.index_name->as<ASTIdentifier &>().name();
        store.remove(table_id, index_name, query.if_exists);
        return {};
    }

    /// CREATE HYPOTHETICAL INDEX
    auto metadata = table->getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);
    auto index_desc = IndexDescription::getIndexFromAST(
        query.index_decl,
        metadata->getColumns(),
        /* is_implicitly_created = */ false,
        /* escape_filenames = */ true,
        context);

    store.add(table_id, index_desc, query.if_not_exists);
    return {};
}


void registerInterpreterHypotheticalIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterHypotheticalIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterHypotheticalIndexQuery", create_fn);
}

}
