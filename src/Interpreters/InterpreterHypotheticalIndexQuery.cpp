#include <Interpreters/InterpreterHypotheticalIndexQuery.h>

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
    extern const int BAD_ARGUMENTS;
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

    /// Only MergeTree family tables support skip indexes.
    if (!dynamic_cast<const MergeTreeData *>(table.get()))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes are only supported for MergeTree family tables, got {}",
            table->getName());

    auto & store = context->getHypotheticalIndexStore();

    if (query.kind == ASTHypotheticalIndexQuery::Drop)
    {
        auto index_name = query.index_name->as<ASTIdentifier &>().name();
        if (query.if_exists)
        {
            try
            {
                store.remove(table_id, index_name);
            }
            catch (...)
            {
                /// IF EXISTS: silently ignore if not found
            }
        }
        else
        {
            store.remove(table_id, index_name);
        }
        return {};
    }

    /// CREATE HYPOTHETICAL INDEX
    auto metadata = table->getInMemoryMetadataPtr();
    auto index_desc = IndexDescription::getIndexFromAST(
        query.index_decl,
        metadata->getColumns(),
        /* is_implicitly_created = */ false,
        /* escape_filenames = */ true,
        context);

    if (query.if_not_exists)
    {
        auto existing = store.getForTable(table_id);
        for (const auto & idx : existing)
        {
            if (idx.name == index_desc.name)
                return {};
        }
    }

    store.add(table_id, index_desc);
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
