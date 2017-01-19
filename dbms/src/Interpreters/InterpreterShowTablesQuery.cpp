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

	/// SHOW DATABASES
	if (query.databases)
		return "SELECT name FROM system.databases";

	String database = query.from.empty() ? context.getCurrentDatabase() : query.from;

	/** Параметр check_database_access_rights сбрасывается при обработке запроса SHOW TABLES для того,
	  * чтобы все клиенты могли видеть список всех БД и таблиц в них независимо от их прав доступа
	  * к этим БД.
	  */
	context.assertDatabaseExists(database, false);

	std::stringstream rewritten_query;
	rewritten_query << "SELECT name FROM system.tables WHERE database = " << mysqlxx::quote << database;

	if (!query.like.empty())
		rewritten_query << " AND name " << (query.not_like ? "NOT " : "") << "LIKE " << mysqlxx::quote << query.like;

	return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
	return executeQuery(getRewrittenQuery(), context, true);
}


}
