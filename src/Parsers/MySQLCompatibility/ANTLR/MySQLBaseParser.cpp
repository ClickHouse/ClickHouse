#include "MySQLBaseParser.h"

void MySQLBaseParser::setMode(uint32_t mode)
{
	sqlMode = mode;
}

bool MySQLBaseParser::isSqlModeActive(SqlMode mode) const
{
	if (!sqlMode)
		return false;
	
	return (sqlMode & mode) != 0;
}
