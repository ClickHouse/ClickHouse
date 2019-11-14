#include <sstream>

#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query;
    if (show_query = query_ptr->as<ASTShowCreateTableQuery>(); show_query)
    {
        if (show_query->temporary)
            create_query = context.getCreateExternalTableQuery(show_query->tableName());
        else
            create_query = context.getCreateTableQuery(show_query->databaseName(), show_query->tableName());
    }
    else if (show_query = query_ptr->as<ASTShowCreateDatabaseQuery>(); show_query)
    {
        if (show_query->temporary)
            throw Exception("Temporary databases are not possible.", ErrorCodes::SYNTAX_ERROR);
        create_query = context.getCreateDatabaseQuery(show_query->databaseName());
    }
    else if (show_query = query_ptr->as<ASTShowCreateDictionaryQuery>(); show_query)
    {
        if (show_query->temporary)
            throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);
        create_query = context.getCreateDictionaryQuery(show_query->databaseName(), show_query->tableName());
    }

    if (!create_query && show_query->temporary)
        throw Exception("Unable to show the create query of " + show_query->tableName() + ". Maybe it was created by the system.", ErrorCodes::THERE_IS_NO_QUERY);

    std::stringstream stream;
    formatAST(*create_query, stream, false, true);
    String res = stream.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return std::make_shared<OneBlockInputStream>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}});
}

}
