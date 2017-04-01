#include <DB/IO/ReadBufferFromString.h>

#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/InterpreterShowProcesslistQuery.h>

#include <DB/Parsers/ASTQueryWithOutput.h>
#include <DB/Parsers/ASTIdentifier.h>


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
