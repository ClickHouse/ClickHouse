#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Storages/StoragePtr.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>

namespace DB
{

namespace VirtualColumnUtils
{

/// Вычислить минимальный числовый суффикс, который надо добавить к строке, чтобы она не присутствовала в множестве
inline String chooseSuffix(const NamesAndTypesList & columns, const String & name)
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

/// Вычислить минимальный общий числовый суффикс, который надо добавить к каждой строке,
/// чтобы ни одна не присутствовала в множестве.
inline String chooseSuffixForSet(const NamesAndTypesList & columns, const std::vector<String> & names)
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
		id ++;
		current_suffix = toString<Int32>(id);
	}
	return current_suffix;
}

/// Добавляет в селект запрос секцию select column_name as value
/// Например select _port as 9000.
inline void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value)
{
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery & >(*ast);
	ASTExpressionList & node = dynamic_cast<ASTExpressionList & >(*select.select_expression_list);
	ASTs & asts = node.children;
	ASTLiteral * cur = new ASTLiteral(StringRange(NULL, NULL), value);
	cur->alias = column_name;
	ASTPtr column_value = cur;
	asts.insert(asts.begin(), column_value);
}

/// Проверка, что функция зависит только от заданных столбцов
inline bool validFunction(ASTPtr expression, const std::vector<String> & columns)
{
	for (size_t i = 0; i < expression->children.size(); ++i)
		if (!validFunction(expression->children[i], columns))
			return false;

	if (ASTIdentifier * identifier = dynamic_cast<ASTIdentifier *>(&* expression))
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
inline void extractFunctions(ASTPtr expression, const std::vector<String> & columns, std::vector<ASTPtr> & result)
{
	if (ASTFunction * function = dynamic_cast<ASTFunction *>(&* expression))
	{
		if (function->name == "and")
		{
			for (size_t i = 0; i < function->arguments->children.size(); ++i)
				extractFunctions(function->arguments->children[i], columns, result);
		} else {
			if (validFunction(expression, columns))
				result.push_back(expression->clone());
		}
	}
}

/// Построить конъюнкцию из заданных функций
inline ASTPtr buildWhereExpression(const std::vector<ASTPtr> & functions)
{
	if (functions.size() == 0) return ASTPtr();
	ASTPtr result = functions[0];
	for (size_t i = 1; i < functions.size(); ++i)
	{
		ASTPtr new_query = new ASTFunction();
		ASTFunction & new_function = dynamic_cast<ASTFunction & >(*new_query);
		new_function.name = "and";
		new_function.arguments = new ASTExpressionList();
		new_function.arguments->children.push_back(result);
		new_function.arguments->children.push_back(functions[i]);
		new_function.children.push_back(new_function.arguments);
		result = new_query;
	}
	return result;
}

/// Получить поток блоков содержащий интересующие нас значения виртуальных столбцов
/// На вход подается исходный запрос, блок с значениями виртуальных столбцов и контекст
inline BlockInputStreamPtr getVirtualColumnsBlocks(ASTPtr query, Block input, const Context & context)
{
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery & >(*query);

	if (!select.where_expression) return new OneBlockInputStream(input);

	ASTPtr new_query = new ASTSelectQuery();

	/// Вычисляем имена виртуальных столбцов
	std::vector<String> columns;
	for (const auto & it : input.getColumnsList())
		columns.push_back(it.first);

	/// Формируем запрос и вычисляем имена виртуальных столбцов
	{
		ASTSelectQuery & new_select = dynamic_cast<ASTSelectQuery & >(*new_query);

		new_select.select_expression_list = new ASTExpressionList();
		ASTExpressionList & select_list = dynamic_cast<ASTExpressionList & >(*new_select.select_expression_list);;
		for (size_t i = 0; i < columns.size(); ++i)
			select_list.children.push_back(new ASTIdentifier(StringRange(NULL, NULL), columns[i]));

		std::vector<ASTPtr> functions;
		extractFunctions(select.where_expression, columns, functions);
		new_select.where_expression = buildWhereExpression(functions);

//		new_select.table = select.table->clone();

		new_select.children.push_back(new_select.select_expression_list);
//		new_select.children.push_back(new_select.table);
		new_select.children.push_back(new_select.where_expression);
	}

	/// Возвращаем результат нового запроса на блоке виртуальных фукнций
	InterpreterSelectQuery interpreter(new_query, context, columns, input.getColumnsList(), QueryProcessingStage::Complete, 0, new OneBlockInputStream(input));
	return interpreter.execute();
}

/// Извлечь из входного потока множество значений столбца name
template<typename T1>
inline std::set<T1> extractSingleValueFromBlocks(BlockInputStreamPtr input, const String & name)
{
	std::set<T1> res;
	input->readPrefix();
	while(1)
	{
		Block block = input->read();
		if (!block) break;
		const ColumnWithNameAndType & data = block.getByName(name);
		for (size_t i = 0; i < block.rows(); ++i)
			res.insert((*data.column)[i].get<T1>());
	}
	return res;
}

/// Извлечь из входного потока множество пар значений в столбцах first_name и second_name
template<typename T1, typename T2>
inline std::set< std::pair<T1, T2> > extractTwoValuesFromBlocks(BlockInputStreamPtr input,
															   const String & first_name, const String & second_name)
{
	std::set< std::pair<T1, T2> > res;
	input->readPrefix();
	while(1)
	{
		Block block = input->read();
		if (!block) break;
		const ColumnWithNameAndType & first = block.getByName(first_name);
		const ColumnWithNameAndType & second = block.getByName(second_name);
		for (size_t i = 0; i < block.rows(); ++i)
		{
			T1 val1 = (*first.column)[i].get<T1>();
			T2 val2 = (*second.column)[i].get<T2>();
			res.insert(std::make_pair(val1, val2));
		}
	}
	return res;
}


}
}
