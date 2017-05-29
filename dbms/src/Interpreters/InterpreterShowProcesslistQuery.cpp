#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

BlockIO InterpreterShowProcesslistQuery::execute()
{
    return executeQuery("SELECT * FROM system.processes", context, true);
}

}
