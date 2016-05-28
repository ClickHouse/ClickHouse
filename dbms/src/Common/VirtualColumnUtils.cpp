#include <numeric>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Common/VirtualColumnUtils.h>


namespace DB
{

namespace VirtualColumnUtils
{

String chooseSuffix(const NamesAndTypesList & columns, const String & name)
{
	int id = 0;
	String current_suffix;
	while (true)
	{
		bool done = true;
		for (const auto & it : columns)
			if (it.name == name + current_suffix)
			{
				done = false;
				break;
			}
		if (done) break;
		++id;
		current_suffix = toString<Int32>(id);
	}
	return current_suffix;
}

String chooseSuffixForSet(const NamesAndTypesList & columns, const std::vector<String> & names)
{
	int id = 0;
	String current_suffix;
	while (true)
	{
		bool done = true;
		for (const auto & it : columns)
		{
			for (size_t i = 0; i < names.size(); ++i)
			{
				if (it.name == names[i] + current_suffix)
				{
					done = false;
					break;
				}
			}
			if (!done)
				break;
		}
		if (done)
			break;
		++id;
		current_suffix = toString<Int32>(id);
	}
	return current_suffix;
}

void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value)
{
	ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*ast);
	ASTExpressionList & node = typeid_cast<ASTExpressionList &>(*select.select_expression_list);
	ASTs & asts = node.children;
	auto cur = std::make_shared<ASTLiteral>(StringRange(), value);
	cur->alias = column_name;
	ASTPtr column_value = cur;
	bool is_replaced = false;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		if (const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(&* asts[i]))
		{
			if (identifier->kind == ASTIdentifier::Kind::Column && identifier->name == column_name)
			{
				asts[i] = column_value;
				is_replaced = true;
			}
		}
	}
	if (!is_replaced)
		asts.insert(asts.begin(), column_value);
}

/// Проверка, что функция зависит только от заданных столбцов
static bool isValidFunction(ASTPtr expression, const NameSet & columns)
{
	for (size_t i = 0; i < expression->children.size(); ++i)
		if (!isValidFunction(expression->children[i], columns))
			return false;

	if (const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(&*expression))
	{
		if (identifier->kind == ASTIdentifier::Kind::Column)
			return columns.count(identifier->name);
	}
	return true;
}

/// Извлечь все подфункции главной конъюнкции, но зависящие только от заданных столбцов
static void extractFunctions(ASTPtr expression, const NameSet & columns, std::vector<ASTPtr> & result)
{
	const ASTFunction * function = typeid_cast<const ASTFunction *>(&*expression);
	if (function && function->name == "and")
	{
		for (size_t i = 0; i < function->arguments->children.size(); ++i)
			extractFunctions(function->arguments->children[i], columns, result);
	}
	else if (isValidFunction(expression, columns))
	{
		result.push_back(expression->clone());
	}
}

/// Построить конъюнкцию из заданных функций
static ASTPtr buildWhereExpression(const ASTs & functions)
{
	if (functions.size() == 0) return nullptr;
	if (functions.size() == 1) return functions[0];
	ASTPtr new_query = std::make_shared<ASTFunction>();
	ASTFunction & new_function = typeid_cast<ASTFunction & >(*new_query);
	new_function.name = "and";
	new_function.arguments = std::make_shared<ASTExpressionList>();
	new_function.arguments->children = functions;
	new_function.children.push_back(new_function.arguments);
	return new_query;
}

bool filterBlockWithQuery(ASTPtr query, Block & block, const Context & context)
{
	query = query->clone();
	const ASTSelectQuery & select = typeid_cast<ASTSelectQuery & >(*query);
	if (!select.where_expression && !select.prewhere_expression)
		return false;

	NameSet columns;
	for (const auto & it : block.getColumnsList())
		columns.insert(it.name);

	/// Составим выражение, вычисляющее выражения в WHERE и PREWHERE, зависящие только от имеющихся столбцов.
	std::vector<ASTPtr> functions;
	if (select.where_expression)
		extractFunctions(select.where_expression, columns, functions);
	if (select.prewhere_expression)
		extractFunctions(select.prewhere_expression, columns, functions);
	ASTPtr expression_ast = buildWhereExpression(functions);
	if (!expression_ast)
		return false;

	/// Распарсим и вычислим выражение.
	ExpressionAnalyzer analyzer(expression_ast, context, {}, block.getColumnsList());
	ExpressionActionsPtr actions = analyzer.getActions(false);
	actions->execute(block);

	/// Отфильтруем блок.
	String filter_column_name = expression_ast->getColumnName();
	ColumnPtr filter_column = block.getByName(filter_column_name).column;
	if (auto converted = filter_column->convertToFullColumnIfConst())
		filter_column = converted;
	const IColumn::Filter & filter = dynamic_cast<ColumnUInt8 &>(*filter_column).getData();

	if (std::accumulate(filter.begin(), filter.end(), 0ul) == filter.size())
		return false;

	for (size_t i = 0; i < block.columns(); ++i)
	{
		ColumnPtr & column = block.getByPosition(i).column;
		column = column->filter(filter, -1);
	}

	return true;
}

}

}
