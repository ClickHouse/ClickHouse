#include <DB/IO/ReadBufferFromString.h>

#include <DB/Parsers/ASTShowTablesQuery.h>

#include <DB/Interpreters/executeQuery.h>
#include <DB/Interpreters/InterpreterShowTablesQuery.h>

#include <mysqlxx/Manip.h>


namespace DB
{


InterpreterShowTablesQuery::InterpreterShowTablesQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


String InterpreterShowTablesQuery::getRewrittenQuery()
{
	ASTShowTablesQuery query = dynamic_cast<const ASTShowTablesQuery &>(*query_ptr);
	
	/// SHOW DATABASES
	if (query.databases)
		return "SELECT name FROM system.databases";

	String database = query.from.empty() ? context.current_database : query.from;
	context.assertDatabaseExists(database);

	std::stringstream rewritten_query;
	rewritten_query << "SELECT name FROM system.tables WHERE database = " << mysqlxx::quote << database;

	if (!query.like.empty())
		rewritten_query << " AND name LIKE " << mysqlxx::quote << query.like;

	return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
	return executeQuery(getRewrittenQuery(), context);
}


BlockInputStreamPtr InterpreterShowTablesQuery::executeAndFormat(WriteBuffer & buf)
{
	String query = getRewrittenQuery();
	ReadBufferFromString in(query);
	BlockInputStreamPtr query_plan;
	executeQuery(in, buf, context, query_plan);
	return query_plan;
}


}
