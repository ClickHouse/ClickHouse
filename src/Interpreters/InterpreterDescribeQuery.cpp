#include <Storages/IStorage.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
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

Block InterpreterDescribeQuery::getSampleBlock(bool include_subcolumns)
{
    Block block;

    ColumnWithTypeAndName col;
    col.name = "name";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "type";
    block.insert(col);

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

    if (include_subcolumns)
    {
        col.name = "is_subcolumn";
        col.type = std::make_shared<DataTypeUInt8>();
        col.column = col.type->createColumn();
        block.insert(col);
    }

    return block;
}


BlockIO InterpreterDescribeQuery::execute()
{
    ColumnsDescription columns;
    StorageSnapshotPtr storage_snapshot;

    const auto & ast = query_ptr->as<ASTDescribeQuery &>();
    const auto & table_expression = ast.table_expression->as<ASTTableExpression &>();
    const auto & settings = getContext()->getSettingsRef();

    if (table_expression.subquery)
    {
        auto names_and_types = InterpreterSelectWithUnionQuery::getSampleBlock(
            table_expression.subquery->children.at(0), getContext()).getNamesAndTypesList();
        columns = ColumnsDescription(std::move(names_and_types));
    }
    else if (table_expression.table_function)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression.table_function, getContext());
        columns = table_function_ptr->getActualTableStructure(getContext());
    }
    else
    {
        auto table_id = getContext()->resolveStorageID(table_expression.database_and_table_name);
        getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);
        auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
        auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings.lock_acquire_timeout);

        auto metadata_snapshot = table->getInMemoryMetadataPtr();
        storage_snapshot = table->getStorageSnapshot(metadata_snapshot);
        columns = metadata_snapshot->getColumns();
    }

    bool extend_object_types = settings.describe_extend_object_types && storage_snapshot;
    bool include_subcolumns = settings.describe_include_subcolumns;

    Block sample_block = getSampleBlock(include_subcolumns);
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & column : columns)
    {
        res_columns[0]->insert(column.name);

        if (extend_object_types)
            res_columns[1]->insert(storage_snapshot->getConcreteType(column.name)->getName());
        else
            res_columns[1]->insert(column.type->getName());

        if (column.default_desc.expression)
        {
            res_columns[2]->insert(toString(column.default_desc.kind));
            res_columns[3]->insert(queryToString(column.default_desc.expression));
        }
        else
        {
            res_columns[2]->insertDefault();
            res_columns[3]->insertDefault();
        }

        res_columns[4]->insert(column.comment);

        if (column.codec)
            res_columns[5]->insert(queryToString(column.codec->as<ASTFunction>()->arguments));
        else
            res_columns[5]->insertDefault();

        if (column.ttl)
            res_columns[6]->insert(queryToString(column.ttl));
        else
            res_columns[6]->insertDefault();

        if (include_subcolumns)
            res_columns[7]->insertDefault();
    }

    if (include_subcolumns)
    {
        for (const auto & column : columns)
        {
            auto type = extend_object_types ? storage_snapshot->getConcreteType(column.name) : column.type;

            IDataType::forEachSubcolumn([&](const auto & path, const auto & name, const auto & data)
            {
                res_columns[0]->insert(Nested::concatenateName(column.name, name));
                res_columns[1]->insert(data.type->getName());

                /// It's not trivial to calculate default expression for subcolumn.
                /// So, leave it empty.
                res_columns[2]->insertDefault();
                res_columns[3]->insertDefault();
                res_columns[4]->insert(column.comment);

                if (column.codec && ISerialization::isSpecialCompressionAllowed(path))
                    res_columns[5]->insert(queryToString(column.codec->as<ASTFunction>()->arguments));
                else
                    res_columns[5]->insertDefault();

                if (column.ttl)
                    res_columns[6]->insert(queryToString(column.ttl));
                else
                    res_columns[6]->insertDefault();

                res_columns[7]->insert(1u);
            }, { type->getDefaultSerialization(), type, nullptr, nullptr });
        }
    }

    BlockIO res;
    size_t num_rows = res_columns[0]->size();
    auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));

    return res;
}

}
