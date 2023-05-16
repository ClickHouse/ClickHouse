#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowEngineQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

BlockIO InterpreterShowEnginesQuery::execute()
{
    return executeQuery("SELECT * FROM system.table_engines ORDER BY name", getContext(), true);
}

}
