#include <DB/Analyzers/AnalyzeResultOfQuery.h>
#include <DB/Analyzers/CollectAliases.h>
#include <DB/Analyzers/CollectTables.h>
#include <DB/Analyzers/AnalyzeColumns.h>
#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/ASTSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int UNEXPECTED_AST_STRUCTURE;
}


void AnalyzeResultOfQuery::process(ASTPtr & ast, Context & context)
{
	CollectAliases collect_aliases;
	collect_aliases.process(ast);

	CollectTables collect_tables;
	collect_tables.process(ast, context, collect_aliases);

	AnalyzeColumns analyze_columns;
	analyze_columns.process(ast, collect_aliases, collect_tables);

	TypeAndConstantInference inference;
	inference.process(ast, context, collect_aliases, analyze_columns);

	const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get());
	if (!select)
		throw Exception("AnalyzeResultOfQuery::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
	if (!select->select_expression_list)
		throw Exception("SELECT query doesn't have select_expression_list", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

	result.reserve(select->select_expression_list->children.size());
	for (const ASTPtr & child : select->select_expression_list->children)
	{
		auto it = inference.info.find(child->getColumnName());
		if (it == inference.info.end())
			throw Exception("Logical error: type information for result column of SELECT query was not inferred", ErrorCodes::LOGICAL_ERROR);

		String name = child->getAliasOrColumnName();
		const TypeAndConstantInference::ExpressionInfo & info = it->second;

		result.emplace_back(std::move(name), info.data_type);
	}
}


void AnalyzeResultOfQuery::dump(WriteBuffer & out) const
{
	for (size_t i = 0, size = result.size(); i < size; ++i)
	{
		const NameAndTypePair & name_type = result[i];

		writeProbablyBackQuotedString(name_type.name, out);
		writeChar(' ', out);
		writeString(name_type.type->getName(), out);
		if (i + 1 < size)
			writeChar(',', out);
		writeChar('\n', out);
	}
}


}
