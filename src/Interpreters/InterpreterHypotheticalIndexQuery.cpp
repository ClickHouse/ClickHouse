#include <Interpreters/InterpreterHypotheticalIndexQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTHypotheticalIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Core/Settings.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsBool allow_suspicious_indices;
}

namespace
{

void checkSuspiciousIndex(const ASTPtr & expression_list)
{
    const auto * function = expression_list ? typeid_cast<const ASTFunction *>(expression_list.get()) : nullptr;
    if (!function || !function->arguments)
        return;

    std::unordered_set<UInt64> seen;
    for (const auto & child : function->arguments->children)
    {
        const auto hash = child->getTreeHash(/* ignore_aliases = */ true);
        if (!seen.emplace(hash.low64).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Primary key or secondary index contains a duplicate expression. "
                "To suppress this exception, rerun the command with setting 'allow_suspicious_indices = 1'");
    }
}

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

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(table.get());
    if (!merge_tree)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes are only supported for MergeTree family tables, got {}",
            table->getName());

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

    /// Reject unknown index types and invalid arguments at CREATE time,
    /// matching ALTER TABLE ... ADD INDEX semantics
    MergeTreeIndexFactory::instance().validate(index_desc, /* attach = */ false);

    /// Reject old-syntax MergeTree tables, mirroring `MergeTreeData::checkAlterIsPossible`
    /// for `ADD_INDEX` — a real `ALTER TABLE ... ADD INDEX` is not allowed there either
    if (!merge_tree->is_custom_partitioned)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical indexes are not supported for tables with the old MergeTree syntax");

    /// Reject index types whose real `ALTER TABLE ... ADD INDEX` validation needs
    /// table-level constraints that the hypothetical store cannot replicate
    /// (`text` has the one-per-column rule and `enable_full_text_index`,
    /// `vector_similarity` requires `index_granularity_bytes != 0`)
    if (index_desc.type == "text" || index_desc.type == "vector_similarity")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes of type '{}' are not supported",
            index_desc.type);

    /// Reject name conflicts with existing real secondary indexes
    for (const auto & existing : metadata->getSecondaryIndices())
    {
        if (existing.name == index_desc.name)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Hypothetical index '{}' conflicts with an existing secondary index on {}.{}",
                index_desc.name,
                table_id.getDatabaseName(),
                table_id.getTableName());
    }

    if (!context->getSettingsRef()[Setting::allow_suspicious_indices])
    {
        const auto * index_ast = query.index_decl ? query.index_decl->as<ASTIndexDeclaration>() : nullptr;
        if (index_ast)
            checkSuspiciousIndex(index_ast->getExpression());
    }

    store.add(table_id, index_desc, query.if_not_exists);
    return {};
}

void registerInterpreterHypotheticalIndexQuery(InterpreterFactory & factory);

void registerInterpreterHypotheticalIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterHypotheticalIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterHypotheticalIndexQuery", create_fn);
}

}
