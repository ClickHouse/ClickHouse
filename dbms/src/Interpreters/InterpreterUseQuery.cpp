#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/InterpreterUseQuery.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
	const String & new_database = typeid_cast<const ASTUseQuery &>(*query_ptr).database;
	context.getSessionContext().setCurrentDatabase(new_database);
	return {};
}

}
