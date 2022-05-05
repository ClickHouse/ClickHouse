#include <Parsers/MySQLCompatibility/util.h>

namespace MySQLCompatibility
{
String removeQuotes(const String & quoted)
{
	
	if (quoted.size() < 2)
		return "";
	
	// TODO: other quotes
	if (!(quoted.front() == '\'' && quoted.back() == '\'') 
		&& !(quoted.front() == '"' && quoted.back() == '"')
		&& !(quoted.front() == '`' && quoted.back() == '`'))
		return quoted;
	
	return quoted.substr(1, quoted.size() - 2);
}
Poco::Logger * getLogger()
{
	auto * logger = &Poco::Logger::get("AST");
	return logger;
}
}
