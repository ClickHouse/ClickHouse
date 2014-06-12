#include <DB/Common/VirtualColumnUtils.h>

#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>

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
			if (it.first == name + current_suffix)
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
				if (it.first == names[i] + current_suffix)
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
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*ast);
	ASTExpressionList & node = dynamic_cast<ASTExpressionList &>(*select.select_expression_list);
	ASTs & asts = node.children;
	ASTLiteral * cur = new ASTLiteral(StringRange(), value);
	cur->alias = column_name;
	ASTPtr column_value = cur;
	bool is_replaced = false;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		if (const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(&* asts[i]))
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
static bool isValidFunction(ASTPtr expression, const std::vector<String> & columns)
{
	for (size_t i = 0; i < expression->children.size(); ++i)
		if (!isValidFunction(expression->children[i], columns))
			return false;

	if (const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(&* expression))
	{
		if (identifier->kind == ASTIdentifier::Kind::Column)
		{
			for (size_t i = 0; i < columns.size(); ++i)
				if (columns[i] == identifier->name)
					return true;
			return false;
		}
	}
	return true;
}

/// Извлечь все подфункции главной конъюнкции, но зависящие только от заданных столбцов
static void extractFunctions(ASTPtr expression, const std::vector<String> & columns, std::vector<ASTPtr> & result)
{
	if (const ASTFunction * function = dynamic_cast<const ASTFunction *>(&* expression))
	{
		if (function->name == "and")
		{
			for (size_t i = 0; i < function->arguments->children.size(); ++i)
				extractFunctions(function->arguments->children[i], columns, result);
		}
		else
		{
			if (isValidFunction(expression, columns))
				result.push_back(expression->clone());
		}
	}
}

/// Построить конъюнкцию из заданных функций
static ASTPtr buildWhereExpression(const ASTs & functions)
{
	if (functions.size() == 0) return nullptr;
	if (functions.size() == 1) return functions[0];
	ASTPtr new_query = new ASTFunction();
	ASTFunction & new_function = dynamic_cast<ASTFunction & >(*new_query);
	new_function.name = "and";
	new_function.arguments = new ASTExpressionList();
	new_function.arguments->children = functions;
	new_function.children.push_back(new_function.arguments);
	return new_query;
}

BlockInputStreamPtr getVirtualColumnsBlocks(ASTPtr query, const Block & input, const Context & context)
{
	const ASTSelectQuery & select = dynamic_cast<ASTSelectQuery & >(*query);
	if (!select.where_expression)
		return new OneBlockInputStream(input);

	ASTPtr new_query = new ASTSelectQuery();

	/// Вычисляем имена виртуальных столбцов
	std::vector<String> columns;
	for (const auto & it : input.getColumnsList())
		columns.push_back(it.first);

	/// Формируем запрос и записываем имена виртуальных столбцов
	ASTSelectQuery & new_select = dynamic_cast<ASTSelectQuery & >(*new_query);

	new_select.select_expression_list = new ASTExpressionList();
	ASTExpressionList & select_list = dynamic_cast<ASTExpressionList & >(*new_select.select_expression_list);
	for (size_t i = 0; i < columns.size(); ++i)
		select_list.children.push_back(new ASTIdentifier(StringRange(), columns[i]));

	std::vector<ASTPtr> functions;
	extractFunctions(select.where_expression, columns, functions);
	new_select.where_expression = buildWhereExpression(functions);

	if (new_select.select_expression_list)
		new_select.children.push_back(new_select.select_expression_list);
	if (new_select.where_expression)
		new_select.children.push_back(new_select.where_expression);

	/// Возвращаем результат выполнения нового запроса на блоке виртуальных функций
	InterpreterSelectQuery interpreter(new_query, context, columns, false, input.getColumnsList(),
		QueryProcessingStage::Complete, 0, new OneBlockInputStream(input));

	return interpreter.execute();
}




}
}
