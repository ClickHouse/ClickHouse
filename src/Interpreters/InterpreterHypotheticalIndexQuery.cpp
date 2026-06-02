#include <Interpreters/InterpreterHypotheticalIndexQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
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

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(table.get());
    if (!merge_tree)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes are only supported for MergeTree family tables, got {}",
            table->getName());

    /// The store keys entries by UUID; without one (Ordinary databases) a stored
    /// index would never match later lookups, so reject it up front.
    if (table_id.uuid == UUIDHelpers::Nil)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes require a table with a UUID (Atomic database); {}.{} has none",
            table_id.getDatabaseName(),
            table_id.getTableName());

    auto & store = context->getHypotheticalIndexStore();

    if (query.kind == ASTHypotheticalIndexQuery::Drop)
    {
        /// No access check: a session-local drop leaks nothing (unlike CREATE, whose
        /// column-level SELECT guards EXPLAIN WHATIF empirical leakage), and every entry
        /// in the session store already passed that check at creation.
        auto index_name = query.index_name->as<ASTIdentifier &>().name();
        store.remove(table_id, index_name, query.if_exists);
        return {};
    }

    /// CREATE HYPOTHETICAL INDEX
    const auto & index_ast = query.index_decl->as<ASTIndexDeclaration &>();
    auto metadata = table->getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);

    /// `IF NOT EXISTS` must short-circuit before building/validating the descriptor,
    /// matching `ALTER TABLE ... ADD INDEX IF NOT EXISTS`. The name is taken if a
    /// hypothetical index already uses it or a real secondary index does.
    if (query.if_not_exists)
    {
        for (const auto & existing : store.getForTable(table_id))
            if (existing.name == index_ast.name)
                return {};
        if (metadata->getSecondaryIndices().has(index_ast.name))
            return {};
    }

    auto index_desc = IndexDescription::getIndexFromAST(
        query.index_decl,
        metadata->getColumns(),
        /* is_implicitly_created = */ false,
        /* escape_filenames = */ true,
        context);

    /// Empirical estimation reads the index's columns, so require column-level
    /// SELECT — otherwise a user with table-level access could infer a restricted
    /// column's distribution from the reported skip ratio.
    if (index_desc.expression)
        context->checkAccess(AccessType::SELECT, table_id, index_desc.expression->getRequiredColumns());

    /// Reject unsupported types before the type-specific validator can throw a confusing error.
    if (index_desc.type == "text" || index_desc.type == "vector_similarity")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Hypothetical indexes of type '{}' are not supported",
            index_desc.type);

    MergeTreeIndexFactory::instance().validate(index_desc, /* attach = */ false);

    /// Old-syntax MergeTree rejects `ALTER TABLE ... ADD INDEX`, so reject it here too.
    if (!merge_tree->is_custom_partitioned)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical indexes are not supported for tables with the old MergeTree syntax");

    /// Mirror real ADD INDEX: the `auto_minmax_index_` prefix is reserved when
    /// implicit minmax indexes are enabled.
    const bool using_auto_minmax_index =
           metadata->add_minmax_index_for_numeric_columns
        || metadata->add_minmax_index_for_string_columns
        || metadata->add_minmax_index_for_temporal_columns
        || metadata->add_minmax_index_for_block_number_column
        || metadata->add_minmax_index_for_block_offset_column;
    if (using_auto_minmax_index && index_desc.name.starts_with(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot add hypothetical index {} because it uses a reserved index name",
            index_desc.name);

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
        checkSuspiciousIndex(index_ast.getExpression());

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
