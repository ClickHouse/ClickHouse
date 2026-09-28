#include <Interpreters/JoinedTables.h>

#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/getTableExpressions.h>
#include <Functions/FunctionsExternalDictionaries.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageValues.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool asterisk_include_alias_columns;
    extern const SettingsBool asterisk_include_materialized_columns;
    extern const SettingsBool joined_subquery_requires_alias;
    extern const SettingsJoinAlgorithm join_algorithm;
}

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

class RenameQualifiedIdentifiersMatcher
{
public:
    using Data = const std::vector<DatabaseAndTableWithAlias>;

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);
        if (auto * node = ast->as<ASTQualifiedAsterisk>())
            visit(*node, ast, data);
    }

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child)
    {
        if (node->as<ASTTableExpression>() ||
            node->as<ASTQualifiedAsterisk>() ||
            child->as<ASTSubquery>())
            return false; // NOLINT
        return true;
    }

private:
    static void visit(ASTIdentifier & identifier, ASTPtr &, Data & data)
    {
        if (identifier.isShort())
            return;

        bool rewritten = false;
        for (const auto & table : data)
        {
            /// Table has an alias. We do not need to rewrite qualified names with table alias (match == ColumnMatch::TableName).
            auto match = IdentifierSemantic::canReferColumnToTable(identifier, table);
            if (match == IdentifierSemantic::ColumnMatch::AliasedTableName ||
                match == IdentifierSemantic::ColumnMatch::DBAndTable)
            {
                if (rewritten)
                    throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Failed to rewrite distributed table names. Ambiguous column '{}'",
                                    identifier.name());
                /// Table has an alias. So we set a new name qualified by table alias.
                IdentifierSemantic::setColumnLongName(identifier, table);
                rewritten = true;
            }
        }
    }

    static void visit(const ASTQualifiedAsterisk & node, const ASTPtr &, Data & data)
    {
        auto & identifier = node.qualifier->as<ASTIdentifier &>();
        bool rewritten = false;
        for (const auto & table : data)
        {
            if (identifier.name() == table.table)
            {
                if (rewritten)
                    throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Failed to rewrite distributed table. Ambiguous column '{}'",
                                    identifier.name());
                identifier.setShortName(table.alias);
                rewritten = true;
            }
        }
    }
};
using RenameQualifiedIdentifiersVisitor = InDepthNodeVisitor<RenameQualifiedIdentifiersMatcher, true>;

}

JoinedTables::JoinedTables(ContextPtr context_, const ASTSelectQuery & select_query_, bool include_all_columns_, bool is_create_parameterized_view_)
    : context(context_)
    , table_expressions(getTableExpressions(select_query_))
    , include_all_columns(include_all_columns_)
    , left_table_expression(extractTableExpression(select_query_, 0))
    , left_db_and_table(getDatabaseAndTable(select_query_, 0))
    , select_query(select_query_)
    , is_create_parameterized_view(is_create_parameterized_view_)
{}

bool JoinedTables::isLeftTableSubquery() const
{
    return left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>();
}

bool JoinedTables::isLeftTableFunction() const
{
    return left_table_expression && left_table_expression->as<ASTFunction>();
}

std::unique_ptr<InterpreterSelectWithUnionQuery> JoinedTables::makeLeftTableSubquery(const SelectQueryOptions & select_options)
{
    if (!isLeftTableSubquery())
        return {};

    /// Only build dry_run interpreter during analysis. We will reconstruct the subquery interpreter during plan building.
    return std::make_unique<InterpreterSelectWithUnionQuery>(left_table_expression, context, select_options.copy().analyze());
}

StoragePtr JoinedTables::getLeftTableStorage()
{
    if (isLeftTableSubquery())
        return {};

    if (isLeftTableFunction())
    {
        /// For parameterized views in refreshable materialized views, use the current context's database
        /// instead of the query context's database. This ensures unqualified parameterized view
        /// references resolve in the correct database (MV's database, not session's database).
        auto table_function_context = context->getQueryContext();
        if (is_create_parameterized_view)
        {
            /// Temporarily set the current database to match the context we're analyzing in
            table_function_context = Context::createCopy(table_function_context);
            table_function_context->setCurrentDatabase(context->getCurrentDatabase());
        }
        return table_function_context->executeTableFunction(left_table_expression, &select_query);
    }

    StorageID table_id = StorageID::createEmpty();
    if (left_db_and_table)
    {
        table_id = context->resolveStorageID(StorageID(left_db_and_table->database, left_db_and_table->table, left_db_and_table->uuid));
    }
    else /// If the table is not specified - use the table `system.one`.
    {
        table_id = StorageID("system", "one");
    }

    if (auto view_source = context->getViewSource())
    {
        const auto & storage_values = static_cast<const StorageValues &>(*view_source);
        auto tmp_table_id = storage_values.getStorageID();
        if (tmp_table_id.database_name == table_id.database_name && tmp_table_id.table_name == table_id.table_name)
        {
            /// Read from view source.
            return context->getViewSource();
        }
    }

    /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
    return DatabaseCatalog::instance().getTable(table_id, context);
}

bool JoinedTables::resolveTables()
{
    const auto & settings = context->getSettingsRef();
    bool include_alias_cols = include_all_columns || settings[Setting::asterisk_include_alias_columns];
    bool include_materialized_cols = include_all_columns || settings[Setting::asterisk_include_materialized_columns];
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context, include_alias_cols, include_materialized_cols, is_create_parameterized_view);
    if (tables_with_columns.size() != table_expressions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected tables count");

    if (settings[Setting::joined_subquery_requires_alias] && tables_with_columns.size() > 1)
    {
        for (size_t i = 0; i < tables_with_columns.size(); ++i)
        {
            const auto & t = tables_with_columns[i];
            if (t.table.table.empty() && t.table.alias.empty())
            {
                throw Exception(ErrorCodes::ALIAS_REQUIRED,
                                "No alias for subquery or table function "
                                "in JOIN (set joined_subquery_requires_alias=0 to disable restriction). "
                                "While processing '{}'", table_expressions[i]->formatForErrorMessage());
            }
        }
    }

    return !tables_with_columns.empty();
}

void JoinedTables::makeFakeTable(StoragePtr storage, const StorageMetadataPtr & metadata_snapshot, const Block & source_header)
{
    if (storage)
    {
        const ColumnsDescription & storage_columns = metadata_snapshot->columns;
        const VirtualColumnsDescription & virtual_columns = metadata_snapshot->virtuals;
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, storage_columns.getOrdinary());

        auto & table = tables_with_columns.back();
        table.addHiddenColumns(storage_columns.getMaterialized());
        table.addHiddenColumns(storage_columns.getAliases());
        table.addHiddenColumns(virtual_columns.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::All).getNamesAndTypesList());
    }
    else
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, source_header.getNamesAndTypesList());
}

void JoinedTables::rewriteDistributedInAndJoins(ASTPtr & query)
{
    /// Rewrite IN and/or JOIN for distributed tables according to distributed_product_mode setting.
    InJoinSubqueriesPreprocessor::SubqueryTables renamed_tables;
    InJoinSubqueriesPreprocessor(context, renamed_tables).visit(query);

    String database;
    if (!renamed_tables.empty())
        database = context->getCurrentDatabase();

    for (auto & [subquery, ast_tables] : renamed_tables)
    {
        std::vector<DatabaseAndTableWithAlias> renamed;
        renamed.reserve(ast_tables.size());
        for (auto & ast : ast_tables)
            renamed.emplace_back(DatabaseAndTableWithAlias(ast->as<ASTTableIdentifier &>(), database));

        /// Change qualified column names in distributed subqueries using table aliases.
        RenameQualifiedIdentifiersVisitor::Data data(renamed);
        RenameQualifiedIdentifiersVisitor(data).visit(subquery);
    }
}

std::shared_ptr<TableJoin> JoinedTables::makeTableJoin(const ASTSelectQuery & select_query_)
{
    if (tables_with_columns.size() < 2)
        return {};

    const auto & settings = context->getSettingsRef();
    MultiEnum<JoinAlgorithm> join_algorithm = settings[Setting::join_algorithm];
    bool try_use_direct_join = join_algorithm.isSet(JoinAlgorithm::DIRECT) || join_algorithm.isSet(JoinAlgorithm::DEFAULT);
    auto table_join = std::make_shared<TableJoin>(
        settings, context->getJoinAnalyzeMode(), context->getGlobalTemporaryVolume(), context->getTempDataOnDisk());

    const ASTTablesInSelectQueryElement * ast_join = select_query_.join();
    const auto & table_to_join = ast_join->table_expression->as<ASTTableExpression &>();

    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        auto joined_table_id = context->resolveStorageID(table_to_join.database_and_table_name);
        StoragePtr storage = DatabaseCatalog::instance().tryGetTable(joined_table_id, context);

        /// A special storage replaces the right-side plan, and with it the `FilterStep` carrying the
        /// table's row policy, so such a table has to be joined as an ordinary stream. A `Join` table
        /// is a prebuilt hash table read as is, so it cannot be filtered at all.
        if (storage)
        {
            auto row_policy_filter = context->getRowPolicyFilter(
                joined_table_id.getDatabaseName(), joined_table_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
            if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
            {
                if (typeid_cast<StorageJoin *>(storage.get()))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Row policies are not supported for table {} with the Join engine", joined_table_id.getNameForLogs());
                storage = nullptr;
            }
        }

        if (storage)
        {
            if (auto storage_join = std::dynamic_pointer_cast<StorageJoin>(storage); storage_join)
            {
                table_join->setStorageJoin(storage_join);
            }

            auto storage_dict = std::dynamic_pointer_cast<StorageDictionary>(storage);
            if (storage_dict && try_use_direct_join && storage_dict->getDictionary()->getSpecialKeyType() != DictionarySpecialKeyType::Range)
            {
                FunctionDictHelper dictionary_helper(context);

                auto dictionary_name = storage_dict->getDictionaryName();
                auto dictionary = dictionary_helper.getDictionary(dictionary_name);
                if (!dictionary)
                {
                    LOG_TRACE(getLogger("JoinedTables"), "Can't use dictionary join: dictionary '{}' was not found", dictionary_name);
                    return nullptr;
                }
                if (dictionary->getSpecialKeyType() == DictionarySpecialKeyType::Range)
                {
                    LOG_TRACE(getLogger("JoinedTables"), "Can't use dictionary join: dictionary '{}' is a range dictionary", dictionary_name);
                    return nullptr;
                }

                auto dictionary_kv = std::dynamic_pointer_cast<const IKeyValueEntity>(dictionary);
                table_join->setStorageJoin(dictionary_kv);
            }

            if (auto storage_kv = std::dynamic_pointer_cast<IKeyValueEntity>(storage); storage_kv && try_use_direct_join)
            {
                table_join->setStorageJoin(storage_kv);
            }
        }
    }

    return table_join;
}

void JoinedTables::reset(const ASTSelectQuery & select_query_)
{
    table_expressions = getTableExpressions(select_query_);
    left_table_expression = extractTableExpression(select_query_, 0);
    left_db_and_table = getDatabaseAndTable(select_query_, 0);
}

}
