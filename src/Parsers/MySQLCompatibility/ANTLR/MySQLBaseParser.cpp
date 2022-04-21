#include "MySQLBaseParser.h"

bool MySQLBaseParser::isSqlModeActive(int mode)
{
	if (!sqlMode)
		return false;
	
	return (sqlMode & mode) != 0;
}
