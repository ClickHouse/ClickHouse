#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowUserProcessesQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

BlockIO InterpreterShowUserProcessesQuery::execute()
{
    return executeQuery("SELECT * FROM system.user_processes ORDER BY user DESC", getContext(), true);
}

}
