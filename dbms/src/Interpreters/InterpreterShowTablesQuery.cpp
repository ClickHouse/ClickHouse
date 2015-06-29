#include <DB/IO/ReadBufferFromString.h>

#include <DB/Parsers/ASTShowTablesQuery.h>
#include <DB/Parsers/ASTIdentifier.h>

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
	const ASTShowTablesQuery & query = typeid_cast<const ASTShowTablesQuery &>(*query_ptr);

	String format_or_nothing;
	if (query.format)
		format_or_nothing = " FORMAT " + typeid_cast<const ASTIdentifier &>(*query.format).name;

	/// SHOW DATABASES
	if (query.databases)
		return "SELECT name FROM system.databases" + format_or_nothing;

	String database = query.from.empty() ? context.getCurrentDatabase() : query.from;
	context.assertDatabaseExists(database);

	std::stringstream rewritten_query;
	rewritten_query << "SELECT name FROM system.tables WHERE database = " << mysqlxx::quote << database;

	if (!query.like.empty())
		rewritten_query << " AND name " << (query.not_like ? "NOT " : "") << "LIKE " << mysqlxx::quote << query.like;

	rewritten_query << format_or_nothing;

	return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
	return executeQuery(getRewrittenQuery(), context, true);
}


}
