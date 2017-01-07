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
	const ASTQueryWithOutput & query = dynamic_cast<const ASTQueryWithOutput &>(*query_ptr);

	std::stringstream rewritten_query;
	rewritten_query << "SELECT * FROM system.processes";

	if (query.format)
		rewritten_query << " FORMAT " << typeid_cast<const ASTIdentifier &>(*query.format).name;

	return rewritten_query.str();
}


}
