#pragma once

#include <DB/Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{
	
	/** EXISTS запрос
	 */
	DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(ASTExistsQuery, "ExistsQuery")
	
	/** SHOW CREATE TABLE запрос
	 */
	DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(ASTShowCreateQuery, "ShowCreateQuery")
	
	/** DESCRIBE TABLE запрос
	 */
	DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(ASTDescribeQuery, "DescribeQuery")
	
}
