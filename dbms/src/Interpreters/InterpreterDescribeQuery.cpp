#include <Storages/IStorage.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB
{

BlockIO InterpreterDescribeQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterDescribeQuery::getSampleBlock()
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

    return block;
}


BlockInputStreamPtr InterpreterDescribeQuery::executeImpl()
{
    ColumnsDescription columns;

    const auto & ast = typeid_cast<const ASTDescribeQuery &>(*query_ptr);
    const auto & table_expression = typeid_cast<const ASTTableExpression &>(*ast.table_expression);
    if (table_expression.subquery)
    {
        auto names_and_types = InterpreterSelectWithUnionQuery::getSampleBlock(
            table_expression.subquery->children.at(0), context).getNamesAndTypesList();
        columns = ColumnsDescription(std::move(names_and_types));
    }
    else
    {
        StoragePtr table;
        if (table_expression.table_function)
        {
            const auto & table_function = typeid_cast<const ASTFunction &>(*table_expression.table_function);
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_function.name, context);
            /// Run the table function and remember the result
            table = table_function_ptr->execute(table_expression.table_function, context);
        }
        else
        {
            const auto & identifier = typeid_cast<const ASTIdentifier &>(*table_expression.database_and_table_name);

            String database_name;
            String table_name;
            std::tie(database_name, table_name) = IdentifierSemantic::extractDatabaseAndTable(identifier);

            table = context.getTable(database_name, table_name);
        }

        auto table_lock = table->lockStructureForShare(false, context.getCurrentQueryId());
        columns = table->getColumns();
    }

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & column : columns)
    {
        res_columns[0]->insert(column.name);
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
            res_columns[5]->insert(column.codec->getCodecDesc());
        else
            res_columns[5]->insertDefault();
    }

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}
