#include <IO/ReadBufferFromString.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{


BlockIO InterpreterShowProcesslistQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}


String InterpreterShowProcesslistQuery::getRewrittenQuery()
{
    return "SELECT * FROM system.processes";
}


}
