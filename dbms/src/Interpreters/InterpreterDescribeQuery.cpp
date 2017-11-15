#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Common/typeid_cast.h>



namespace DB
{

BlockIO InterpreterDescribeQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    res.in_sample = getSampleBlock();

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


    return block;
}


BlockInputStreamPtr InterpreterDescribeQuery::executeImpl()
{
    const ASTDescribeQuery & ast = typeid_cast<const ASTDescribeQuery &>(*query_ptr);

    NamesAndTypesList columns;
    ColumnDefaults column_defaults;

    {
        StoragePtr table = context.getTable(ast.database, ast.table);
        auto table_lock = table->lockStructure(false, __PRETTY_FUNCTION__);
        columns = table->getColumnsList();
        columns.insert(std::end(columns), std::begin(table->alias_columns), std::end(table->alias_columns));
        column_defaults = table->column_defaults;
    }

    ColumnWithTypeAndName name_column{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
    ColumnWithTypeAndName type_column{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "type" };
    ColumnWithTypeAndName default_type_column{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "default_type" };
    ColumnWithTypeAndName default_expression_column{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "default_expression" };;

    for (const auto column : columns)
    {
        name_column.column->insert(column.name);
        type_column.column->insert(column.type->getName());

        const auto it = column_defaults.find(column.name);
        if (it == std::end(column_defaults))
        {
            default_type_column.column->insertDefault();
            default_expression_column.column->insertDefault();
        }
        else
        {
            default_type_column.column->insert(toString(it->second.type));
            default_expression_column.column->insert(queryToString(it->second.expression));
        }
    }

    return std::make_shared<OneBlockInputStream>(
        Block{name_column, type_column, default_type_column, default_expression_column});
}

}
