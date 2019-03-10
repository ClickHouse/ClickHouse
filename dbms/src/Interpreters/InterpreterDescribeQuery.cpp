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
    const ASTDescribeQuery & ast = typeid_cast<const ASTDescribeQuery &>(*query_ptr);

    NamesAndTypesList columns;
    ColumnDefaults column_defaults;
    ColumnComments column_comments;
    ColumnCodecs column_codecs;
    StoragePtr table;

    auto table_expression = typeid_cast<const ASTTableExpression *>(ast.table_expression.get());

    if (table_expression->subquery)
    {
        columns = InterpreterSelectWithUnionQuery::getSampleBlock(table_expression->subquery->children[0], context).getNamesAndTypesList();
    }
    else
    {
        if (table_expression->table_function)
        {
            auto table_function = typeid_cast<const ASTFunction *>(table_expression->table_function.get());
            /// Get the table function
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_function->name, context);
            /// Run it and remember the result
            table = table_function_ptr->execute(table_expression->table_function, context);
        }
        else
        {
            auto identifier = typeid_cast<const ASTIdentifier *>(table_expression->database_and_table_name.get());

            String database_name;
            String table_name;
            std::tie(database_name, table_name) = IdentifierSemantic::extractDatabaseAndTable(*identifier);

            table = context.getTable(database_name, table_name);
        }

        auto table_lock = table->lockStructureForShare(false, context.getCurrentQueryId());
        columns = table->getColumns().getAll();
        column_defaults = table->getColumns().defaults;
        column_comments = table->getColumns().comments;
        column_codecs = table->getColumns().codecs;
    }

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & column : columns)
    {
        res_columns[0]->insert(column.name);
        res_columns[1]->insert(column.type->getName());

        const auto defaults_it = column_defaults.find(column.name);
        if (defaults_it == std::end(column_defaults))
        {
            res_columns[2]->insertDefault();
            res_columns[3]->insertDefault();
        }
        else
        {
            res_columns[2]->insert(toString(defaults_it->second.kind));
            res_columns[3]->insert(queryToString(defaults_it->second.expression));
        }

        const auto comments_it = column_comments.find(column.name);
        if (comments_it == std::end(column_comments))
        {
            res_columns[4]->insertDefault();
        }
        else
        {
            res_columns[4]->insert(comments_it->second);
        }

        const auto codecs_it = column_codecs.find(column.name);
        if (codecs_it == std::end(column_codecs))
        {
            res_columns[5]->insertDefault();
        }
        else
        {
            res_columns[5]->insert(codecs_it->second->getCodecDesc());
        }
    }

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}
