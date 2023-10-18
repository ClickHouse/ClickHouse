#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowMergesQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

BlockIO InterpreterShowMergesQuery::execute()
{
    return executeQuery("SELECT * FROM system.merges ORDER BY elapsed DESC", getContext(), true);
}

}
