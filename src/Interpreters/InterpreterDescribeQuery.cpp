#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Analyzer/Utils.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Core/Settings.h>
#include <Databases/DatabaseOverlay.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <DataTypes/NestedUtils.h>
#include <Common/Exception.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool describe_compact_output;
    extern const SettingsBool describe_include_subcolumns;
    extern const SettingsBool describe_include_virtual_columns;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool print_pretty_type_names;
}

namespace ErrorCodes
{

extern const int ACCESS_DENIED;
extern const int UNSUPPORTED_METHOD;
extern const int UNKNOWN_FUNCTION;

}

InterpreterDescribeQuery::InterpreterDescribeQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    , settings(getContext()->getSettingsRef())
{
}

Block InterpreterDescribeQuery::getSampleBlock(bool include_subcolumns, bool include_virtuals, bool compact)
{
    Block block;

    ColumnWithTypeAndName col;
    col.name = "name";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "type";
    block.insert(col);

    if (!compact)
    {
        col.name = "default_type";
        block.insert(col);

        col.name = "default_expression";
        block.insert(col);

        col.name = "comment";
        block.insert(col);

        col.name = "codec_expression";
        block.insert(col);

        col.name = "ttl_expression";
        block.insert(col);
    }

    if (include_subcolumns)
    {
        col.name = "is_subcolumn";
        col.type = std::make_shared<DataTypeUInt8>();
        col.column = col.type->createColumn();
        block.insert(col);
    }

    if (include_virtuals)
    {
        col.name = "is_virtual";
        col.type = std::make_shared<DataTypeUInt8>();
        col.column = col.type->createColumn();
        block.insert(col);
    }

    return block;
}

BlockIO InterpreterDescribeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTDescribeQuery &>();
    const auto & table_expression = ast.table_expression->as<ASTTableExpression &>();

    if (table_expression.subquery)
        fillColumnsFromSubquery(table_expression);
    else if (table_expression.table_function)
        fillColumnsFromTableFunction(table_expression);
    else
        fillColumnsFromTable(table_expression, ast.temporary);

    Block sample_block = getSampleBlock(
        settings[Setting::describe_include_subcolumns], settings[Setting::describe_include_virtual_columns], settings[Setting::describe_compact_output]);

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & column : columns)
        addColumn(column, false, res_columns);

    for (const auto & column : virtual_columns)
        addColumn(column, true, res_columns);

    if (settings[Setting::describe_include_subcolumns])
    {
        for (const auto & column : columns)
            addSubcolumns(column, false, res_columns);

        for (const auto & column : virtual_columns)
            addSubcolumns(column, true, res_columns);
    }

    BlockIO res;
    size_t num_rows = res_columns[0]->size();
    auto source = std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(sample_block)), Chunk(std::move(res_columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));

    return res;
}

void InterpreterDescribeQuery::fillColumnsFromSubquery(const ASTTableExpression & table_expression)
{
    auto select_query = table_expression.subquery->children.at(0);
    auto current_context = getContext();
    fillColumnsFromSubqueryImpl(select_query, current_context);
}

void InterpreterDescribeQuery::fillColumnsFromSubqueryImpl(const ASTPtr & select_query, const ContextPtr & current_context)
{
    SharedHeader sample_block;
    if (settings[Setting::allow_experimental_analyzer])
    {
        SelectQueryOptions select_query_options;
        sample_block = InterpreterSelectQueryAnalyzer(select_query, current_context, select_query_options).getSampleBlock();
    }
    else
    {
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(select_query, current_context);
    }

    for (auto && column : *sample_block)
        columns.emplace_back(column.name, column.type);
}

void InterpreterDescribeQuery::fillColumnsFromTableFunction(const ASTTableExpression & table_expression)
{
    auto current_context = getContext();

    auto table_function_name = table_expression.table_function->as<ASTFunction>()->name;

    /// A parameterized view takes precedence over a table function with a colliding name, mirroring
    /// `Context::executeTableFunction`, which does the catalog lookup first and only falls back to the
    /// table-function factory. Look the name up without throwing: a missing or inaccessible object must
    /// still produce the `UNKNOWN_FUNCTION` error (with hints) below, not a table-resolution or
    /// access-check exception.
    {
        auto [database_name, table_name] = extractDatabaseAndTableNameForParameterizedView(table_function_name, current_context);

        /// The written name can resolve to a parameterized view through a read-only `Overlay`
        /// facade. The lookup below would load the underlying source view before any source-side
        /// grant is proven, so a fail-closed visibility precheck runs first: without
        /// `SHOW_COLUMNS` on the source name the branch is skipped and the name falls through to
        /// the `UNKNOWN_FUNCTION` branch, exactly as for a missing name — a denied name, a
        /// missing name, and a hidden broken source stay indistinguishable.
        bool source_visible = true;
        if (!table_name.empty())
            if (const auto facade = DatabaseOverlay::tryGetReadonlyFacade(database_name))
                source_visible = facade->isSourceTableVisibleNoLoad(table_name, current_context, AccessType::SHOW_COLUMNS);

        StoragePtr table;
        if (!table_name.empty() && source_visible)
            table = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, current_context);

        /// Re-verify against the loaded storage: the name could have started resolving to a
        /// (different) source between the metadata-only check above and the lookup. Through a
        /// facade the described columns are those of the underlying source view, so
        /// `SHOW_COLUMNS` is required on the source name too — the facade must not widen access.
        bool source_show_columns_granted = true;
        if (auto source_id = DatabaseOverlay::getSourceTableIdForReadonlyFacade(StorageID{database_name, table_name}, table))
            source_show_columns_granted
                = current_context->getAccess()->isGranted(AccessType::SHOW_COLUMNS, source_id->database_name, source_id->table_name);

        /// An existing parameterized view the user cannot see (`SHOW COLUMNS` not granted, on the
        /// facade name or on the underlying source name) must also fall through to the
        /// `UNKNOWN_FUNCTION` branch rather than throw `ACCESS_DENIED`, which would leak the
        /// existence of the view.
        if (auto * storage_view = table ? table->as<StorageView>() : nullptr;
            storage_view && storage_view->isParameterizedView()
            && source_show_columns_granted
            && current_context->getAccess()->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name))
        {
            auto view_metadata = storage_view->getInMemoryMetadataPtr(current_context, false);
            auto query = view_metadata->getSelectQuery().inner_query->clone();
            NameToNameMap parameterized_view_values = analyzeFunctionParamValues(table_expression.table_function, current_context);
            StorageView::replaceQueryParametersIfParameterizedView(query, parameterized_view_values);
            /// Analyze the substituted query under the view's SQL security context (`DEFINER`/`INVOKER`),
            /// matching execution via `Context::buildParameterizedViewStorage`, so that a user with
            /// `SHOW COLUMNS` on the view but without direct grants on the inner tables can still describe
            /// a `SQL SECURITY DEFINER` view.
            auto view_context = view_metadata->getSQLSecurityOverriddenContext(current_context);
            fillColumnsFromSubqueryImpl(query, view_context);
            return;
        }
    }

    TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().tryGet(table_function_name, current_context);

    if (!table_function_ptr)
    {
        auto hints = TableFunctionFactory::instance().getHints(table_function_name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown table function {}. Maybe you meant: {}", table_function_name, toString(hints));
        else
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown table function {}", table_function_name);
    }

    table_function_ptr->parseArguments(table_expression.table_function, current_context);

    auto column_descriptions = table_function_ptr->getActualTableStructureWithAccess(current_context, /*is_insert_query*/ true);
    for (const auto & column : column_descriptions)
        columns.emplace_back(column);

    if (settings[Setting::describe_include_virtual_columns])
    {
        auto table = table_function_ptr->execute(table_expression.table_function, getContext(), table_function_ptr->getName());
        if (table)
        {
            const auto metadata_snapshot = table->getInMemoryMetadataPtr(current_context, false);
            const auto & virtuals = metadata_snapshot->virtuals;
            for (const auto & column : virtuals)
                if (!column_descriptions.has(column.name))
                    virtual_columns.push_back(column);
        }
    }
}

void InterpreterDescribeQuery::fillColumnsFromTable(const ASTTableExpression & table_expression, bool temporary)
{
    auto query_context = getContext();
    auto resolve_type = temporary ? Context::ResolveExternal : Context::ResolveAll;
    auto table_id = query_context->resolveStorageID(table_expression.database_and_table_name, resolve_type);
    query_context->checkAccess(AccessType::SHOW_COLUMNS, table_id);

    /// Through a read-only `Overlay` facade the described columns are those of the underlying
    /// source table, so `SHOW_COLUMNS` is required on the source too: the facade must not widen
    /// access (see the `Overlay` access-control contract). The source id is resolved from
    /// metadata only, without loading the source table, and fail-closed: the check must run
    /// *before* the lookup below, which loads the source table and could throw its own load
    /// error — and a failing existence probe on a source backed by a remote catalog is remasked
    /// as the same `ACCESS_DENIED` a denied healthy source would produce — otherwise a user
    /// without the source-side grant could observe the source's error and use the facade as an
    /// oracle for hidden broken sources.
    if (table_id.hasDatabase())
        if (const auto facade = DatabaseOverlay::tryGetReadonlyFacade(table_id.database_name))
            facade->checkSourceTableAccess(table_id.table_name, query_context, AccessType::SHOW_COLUMNS);

    auto table = DatabaseCatalog::instance().getTable(table_id, query_context);

    /// Re-verify against the loaded storage: the name could have started resolving to a
    /// (different) source between the metadata-only check above and the lookup.
    if (auto source_id = DatabaseOverlay::getSourceTableIdForReadonlyFacade(table_id, table))
        query_context->checkAccess(AccessType::SHOW_COLUMNS, *source_id);

    if (const auto * alias = table->as<StorageAlias>();
        alias && !alias->isTargetTableGranted(query_context, AccessType::SHOW_COLUMNS, {}))
        throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to describe metadata exposed by {}", table_id.getNameForLogs());


    if (auto * storage_view = table->as<StorageView>())
    {
        if (storage_view->isParameterizedView())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Cannot infer table schema for the parameterized view when no query parameters are provided");
    }

    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);
    table->updateExternalDynamicMetadataIfExists(query_context);

    auto metadata_snapshot = table->getInMemoryMetadataPtr(query_context, false);
    const auto & column_descriptions = metadata_snapshot->getColumns();
    for (const auto & column : column_descriptions)
        columns.emplace_back(column);

    if (settings[Setting::describe_include_virtual_columns])
    {
        const auto & virtuals = metadata_snapshot->virtuals;
        for (const auto & column : virtuals)
            if (!column_descriptions.has(column.name))
                virtual_columns.push_back(column);
    }
}

void InterpreterDescribeQuery::addColumn(const ColumnDescription & column, bool is_virtual, MutableColumns & res_columns)
{
    size_t i = 0;
    res_columns[i++]->insert(column.name);

    if (settings[Setting::print_pretty_type_names])
        res_columns[i++]->insert(column.type->getPrettyName());
    else
        res_columns[i++]->insert(column.type->getName());

    if (!settings[Setting::describe_compact_output])
    {
        if (column.default_desc.expression)
        {
            res_columns[i++]->insert(toString(column.default_desc.kind));
            res_columns[i++]->insert(column.default_desc.expression->formatForLogging());
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(column.comment);

        if (column.codec)
            res_columns[i++]->insert(column.codec->as<ASTFunction>()->arguments->formatForLogging());
        else
            res_columns[i++]->insertDefault();

        if (column.ttl)
            res_columns[i++]->insert(column.ttl->formatForLogging());
        else
            res_columns[i++]->insertDefault();
    }

    if (settings[Setting::describe_include_subcolumns])
        res_columns[i++]->insertDefault();

    if (settings[Setting::describe_include_virtual_columns])
        res_columns[i++]->insert(is_virtual);
}

void InterpreterDescribeQuery::addSubcolumns(const ColumnDescription & column, bool is_virtual, MutableColumns & res_columns)
{
    IDataType::forEachSubcolumn([&](const auto & path, const auto & name, const auto & data)
    {
        size_t i = 0;
        res_columns[i++]->insert(Nested::concatenateName(column.name, name));

        if (settings[Setting::print_pretty_type_names])
            res_columns[i++]->insert(data.type->getPrettyName());
        else
            res_columns[i++]->insert(data.type->getName());

        if (!settings[Setting::describe_compact_output])
        {
            /// It's not trivial to calculate default expression for subcolumn.
            /// So, leave it empty.
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insert(column.comment);

            if (column.codec && ISerialization::isSpecialCompressionAllowed(path))
                res_columns[i++]->insert(column.codec->as<ASTFunction>()->arguments->formatForLogging());
            else
                res_columns[i++]->insertDefault();

            if (column.ttl)
                res_columns[i++]->insert(column.ttl->formatForLogging());
            else
                res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(1U);

        if (settings[Setting::describe_include_virtual_columns])
            res_columns[i++]->insert(is_virtual);

    }, ISerialization::SubstreamData(column.type->getDefaultSerialization()).withType(column.type));
}

void registerInterpreterDescribeQuery(InterpreterFactory & factory);
void registerInterpreterDescribeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDescribeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDescribeQuery", create_fn);
}

}
