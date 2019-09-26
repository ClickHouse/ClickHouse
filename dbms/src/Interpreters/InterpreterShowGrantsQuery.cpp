#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/formatAST.h>
#include <ACL/AccessControlManager.h>
#include <ACL/Role.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <sstream>


namespace DB
{
BlockIO InterpreterShowGrantsQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowGrantsQuery::executeImpl()
{
    MutableColumnPtr column = ColumnString::create();

    const auto & query = query_ptr->as<ASTShowGrantsQuery &>();
    auto grant_queries = context.getAccessControlManager().get<ConstRole>(query.role).getGrantQueries();
    for (const auto & grant_query : grant_queries)
    {
        std::stringstream stream;
        formatAST(*grant_query, stream, false, true);
        column->insert(stream.str());
    }

    return std::make_shared<OneBlockInputStream>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "Grants for " + query.role}});
}

}
