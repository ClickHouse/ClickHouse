#include <Storages/IStorage.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
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
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool describe_compact_output;
    extern const SettingsBool describe_extend_object_types;
    extern const SettingsBool describe_include_subcolumns;
    extern const SettingsBool describe_include_virtual_columns;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool print_pretty_type_names;
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
        fillColumnsFromTable(table_expression);

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
    auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));

    return res;
}

void InterpreterDescribeQuery::fillColumnsFromSubquery(const ASTTableExpression & table_expression)
{
    Block sample_block;
    auto select_query = table_expression.subquery->children.at(0);
    auto current_context = getContext();

    if (settings[Setting::allow_experimental_analyzer])
    {
        SelectQueryOptions select_query_options;
        sample_block = InterpreterSelectQueryAnalyzer(select_query, current_context, select_query_options).getSampleBlock();
    }
    else
    {
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(select_query, current_context);
    }

    for (auto && column : sample_block)
        columns.emplace_back(std::move(column.name), std::move(column.type));
}

void InterpreterDescribeQuery::fillColumnsFromTableFunction(const ASTTableExpression & table_expression)
{
    auto current_context = getContext();
    TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression.table_function, current_context);

    auto column_descriptions = table_function_ptr->getActualTableStructure(getContext(), /*is_insert_query*/ true);
    for (const auto & column : column_descriptions)
        columns.emplace_back(column);

    if (settings[Setting::describe_include_virtual_columns])
    {
        auto table = table_function_ptr->execute(table_expression.table_function, getContext(), table_function_ptr->getName());
        if (table)
        {
            auto virtuals = table->getVirtualsPtr();
            for (const auto & column : *virtuals)
            {
                if (!column_descriptions.has(column.name))
                    virtual_columns.push_back(column);
            }
        }
    }
}

void InterpreterDescribeQuery::fillColumnsFromTable(const ASTTableExpression & table_expression)
{
    auto table_id = getContext()->resolveStorageID(table_expression.database_and_table_name);
    getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (table->hasExternalDynamicMetadata())
    {
        table->updateExternalDynamicMetadata(getContext());
    }

    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    const auto & column_descriptions = metadata_snapshot->getColumns();
    for (const auto & column : column_descriptions)
        columns.emplace_back(column);

    if (settings[Setting::describe_include_virtual_columns])
    {
        auto virtuals = table->getVirtualsPtr();
        for (const auto & column : *virtuals)
        {
            if (!column_descriptions.has(column.name))
                virtual_columns.push_back(column);
        }
    }

    if (settings[Setting::describe_extend_object_types])
        storage_snapshot = table->getStorageSnapshot(metadata_snapshot, getContext());
}

void InterpreterDescribeQuery::addColumn(const ColumnDescription & column, bool is_virtual, MutableColumns & res_columns)
{
    size_t i = 0;
    res_columns[i++]->insert(column.name);

    auto type = storage_snapshot ? storage_snapshot->getConcreteType(column.name) : column.type;
    if (settings[Setting::print_pretty_type_names])
        res_columns[i++]->insert(type->getPrettyName());
    else
        res_columns[i++]->insert(type->getName());

    if (!settings[Setting::describe_compact_output])
    {
        if (column.default_desc.expression)
        {
            res_columns[i++]->insert(toString(column.default_desc.kind));
            res_columns[i++]->insert(queryToString(column.default_desc.expression));
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(column.comment);

        if (column.codec)
            res_columns[i++]->insert(queryToString(column.codec->as<ASTFunction>()->arguments));
        else
            res_columns[i++]->insertDefault();

        if (column.ttl)
            res_columns[i++]->insert(queryToString(column.ttl));
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
    auto type = storage_snapshot ? storage_snapshot->getConcreteType(column.name) : column.type;

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
                res_columns[i++]->insert(queryToString(column.codec->as<ASTFunction>()->arguments));
            else
                res_columns[i++]->insertDefault();

            if (column.ttl)
                res_columns[i++]->insert(queryToString(column.ttl));
            else
                res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(1U);

        if (settings[Setting::describe_include_virtual_columns])
            res_columns[i++]->insert(is_virtual);

    }, ISerialization::SubstreamData(type->getDefaultSerialization()).withType(type));
}

void registerInterpreterDescribeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDescribeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDescribeQuery", create_fn);
}

}
