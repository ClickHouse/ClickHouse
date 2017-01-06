#include <DB/Analyzers/OptimizeGroupOrderLimitBy.h>
#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Interpreters/Context.h>
#include <DB/Parsers/ASTSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int UNEXPECTED_AST_STRUCTURE;
}


//void OptimizeGroupOrderLimitBy::process(ASTPtr & ast, TypeAndConstantInference & expression_info)


void OptimizeGroupOrderLimitBy::process(ASTPtr & ast, TypeAndConstantInference & expression_info)
{
	ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get());
	if (!select)
		throw Exception("AnalyzeResultOfQuery::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
	if (!select->select_expression_list)
		throw Exception("SELECT query doesn't have select_expression_list", ErrorCodes::UNEXPECTED_AST_STRUCTURE);


}


}
