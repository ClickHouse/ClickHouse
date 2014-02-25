#include <boost/assign/list_inserter.hpp>

#include <DB/TableFunctions/TableFunctionMerge.h>
#include <DB/TableFunctions/TableFunctionRemote.h>

#include <DB/TableFunctions/TableFunctionFactory.h>


namespace DB
{


TableFunctionPtr TableFunctionFactory::get(
	const String & name,
	const Context & context) const
{
	if (context.getSettings().limits.readonly)
		throw Exception("Table functions are forbidden in readonly mode", ErrorCodes::READONLY);

		 if (name == "merge")						return new TableFunctionMerge;
	else if (name == "remote")						return new TableFunctionRemote;
	else
		throw Exception("Unknown function " + name, ErrorCodes::UNKNOWN_FUNCTION);
}

}
