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
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    res.in_sample = getSampleBlock();

    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return {{ std::make_shared<DataTypeString>(), "statement" }};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    const ASTShowCreateQuery & ast = typeid_cast<const ASTShowCreateQuery &>(*query_ptr);

    if (ast.temporary && !ast.database.empty())
        throw Exception("Can't add database When using `TEMPORARY`", ErrorCodes::SYNTAX_ERROR);

    ASTPtr createQuery = (ast.temporary ? context.getCreateExternalQuery(ast.table) :
                          context.getCreateQuery(ast.database, ast.table));

    if (!createQuery && ast.temporary)
        throw Exception("Unable to show the create query of " + ast.table + ", It maybe created by system.");

    std::stringstream stream;
    formatAST(*createQuery, stream, false, true);
    String res = stream.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return std::make_shared<OneBlockInputStream>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}});
}

}
