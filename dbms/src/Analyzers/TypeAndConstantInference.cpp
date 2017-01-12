#include <Poco/String.h>
#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Analyzers/CollectAliases.h>
#include <DB/Analyzers/AnalyzeColumns.h>
#include <DB/Analyzers/AnalyzeResultOfQuery.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTTablesInSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/ASTSubquery.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/DataTypes/FieldToDataType.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/Functions/FunctionFactory.h>
#include <ext/range.hpp>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
}


namespace
{

Field getValueFromConstantColumn(const ColumnPtr & column)
{
	if (!column->isConst())
		throw Exception("Logical error: expected that column is constant", ErrorCodes::LOGICAL_ERROR);
	if (column->size() != 1)
		throw Exception("Logical error: expected that column with constant has single element", ErrorCodes::LOGICAL_ERROR);
	return (*column)[0];
}


/// Description of single parameter of lambda expression.
struct LambdaParameter
{
	String name;
	DataTypePtr type;
};

/** Description of all parameters of lambda expression.
  * For example, "x -> x + 1" have single parameter x. And "(x, y) -> x + y" have two paramters: x and y.
  */
using LambdaParameters = std::vector<LambdaParameter>;

/** Nested scopes of lambda expressions.
  * For example,
  *  arrayMap(x -> arrayMap(y -> x[y], x), [[1], [2, 3]])
  * have two scopes: first with parameter x and second with parameter y.
  *
  * In x[y] expression, all scopes will be visible.
  */
using LambdaScopes = std::vector<LambdaParameters>;


void processImpl(
	ASTPtr & ast, Context & context,
	CollectAliases & aliases, const AnalyzeColumns & columns,
	TypeAndConstantInference::Info & info,
	LambdaScopes & lambda_scopes);


void processLiteral(const String & column_name, const ASTPtr & ast, TypeAndConstantInference::Info & info)
{
	const ASTLiteral * literal = static_cast<const ASTLiteral *>(ast.get());

	TypeAndConstantInference::ExpressionInfo expression_info;
	expression_info.node = ast;
	expression_info.is_constant_expression = true;
	expression_info.value = literal->value;
	expression_info.data_type = applyVisitor(FieldToDataType(), expression_info.value);
	info.emplace(column_name, std::move(expression_info));
}


void processIdentifier(const String & column_name, const ASTPtr & ast, TypeAndConstantInference::Info & info,
	Context & context, CollectAliases & aliases, const AnalyzeColumns & columns,
	LambdaScopes & lambda_scopes)
{
	/// Column from table
	auto it = columns.columns.find(column_name);
	if (it != columns.columns.end())
	{
		TypeAndConstantInference::ExpressionInfo expression_info;
		expression_info.node = ast;
		expression_info.data_type = it->second.data_type;

		/// If it comes from subquery and we know, that it is constant expression.
		const Block & structure_of_subquery = it->second.table.structure_of_subquery;
		if (structure_of_subquery)
		{
			const ColumnWithTypeAndName & column_from_subquery = structure_of_subquery.getByName(it->second.name_in_table);
			if (column_from_subquery.column)
			{
				expression_info.is_constant_expression = true;
				expression_info.value = getValueFromConstantColumn(column_from_subquery.column);
			}
		}

		info.emplace(column_name, std::move(expression_info));
	}
	else
	{
		/// Alias
		auto it = aliases.aliases.find(column_name);
		if (it != aliases.aliases.end())
		{
			/// TODO Cyclic aliases.

			if (it->second.kind != CollectAliases::Kind::Expression)
				throw Exception("Logical error: unexpected kind of alias", ErrorCodes::LOGICAL_ERROR);

			processImpl(it->second.node, context, aliases, columns, info, lambda_scopes);
			info[column_name] = info[it->second.node->getColumnName()];
		}
	}
}


void processFunction(const String & column_name, ASTPtr & ast, TypeAndConstantInference::Info & info,
	const Context & context)
{
	ASTFunction * function = static_cast<ASTFunction *>(ast.get());

	/// TODO Special case for lambda functions

	DataTypes argument_types;

	if (function->arguments)
	{
		for (const auto & child : function->arguments->children)
		{
			auto it = info.find(child->getColumnName());
			if (it == info.end())
				throw Exception("Logical error: type of function argument was not inferred during depth-first search", ErrorCodes::LOGICAL_ERROR);

			argument_types.emplace_back(it->second.data_type);
		}
	}

	/// Special cases for COUNT(DISTINCT ...) function.
	bool column_name_changed = false;
	String func_name_lowercase = Poco::toLower(function->name);
	if (func_name_lowercase == "countdistinct")	/// It comes in that form from parser.
	{
		/// Select implementation of countDistinct based on settings.
		/// Important that it is done as query rewrite. It means rewritten query
		///  will be sent to remote servers during distributed query execution,
		///  and on all remote servers, function implementation will be same.
		function->name = context.getSettingsRef().count_distinct_implementation;
		column_name_changed = true;
	}

	/// Aggregate function.
	if (AggregateFunctionPtr aggregate_function_ptr = context.getAggregateFunctionFactory().tryGet(function->name, argument_types))
	{
		/// NOTE Not considering aggregate function parameters in type inference. It could become needed in future.
		/// Note that aggregate function could never be constant expression.

		aggregate_function_ptr->setArguments(argument_types);

		/// Replace function name to canonical one. Because same function could be referenced by different names.
		// function->name = aggregate_function_ptr->getName();

		TypeAndConstantInference::ExpressionInfo expression_info;
		expression_info.node = ast;
		expression_info.data_type = aggregate_function_ptr->getReturnType();
		expression_info.aggregate_function = aggregate_function_ptr;
		info.emplace(column_name_changed ? ast->getColumnName() : column_name, std::move(expression_info));
		return;
	}

	/// Ordinary function.
	if (function->parameters)
		throw Exception("The only parametric functions (functions with two separate parenthesis pairs) are aggregate functions"
			", and '" + function->name + "' is not an aggregate function.", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

	/// IN operator. This is special case, because subqueries in right hand side are not scalar subqueries.
	if (function->name == "in"
		|| function->name == "notIn"
		|| function->name == "globalIn"
		|| function->name == "globalNotIn")
	{
		/// For simplicity reasons, do not consider this as constant expression. We may change it in future.
		TypeAndConstantInference::ExpressionInfo expression_info;
		expression_info.node = ast;
		expression_info.data_type = std::make_shared<DataTypeUInt8>();
		info.emplace(column_name, std::move(expression_info));
		return;
	}

	const FunctionPtr & function_ptr = FunctionFactory::instance().get(function->name, context);

	/// Replace function name to canonical one. Because same function could be referenced by different names.
	// function->name = function_ptr->getName();

	ColumnsWithTypeAndName columns_for_analysis;
	columns_for_analysis.reserve(argument_types.size());

	bool all_consts = true;
	if (function->arguments)
	{
		for (const auto & child : function->arguments->children)
		{
			String child_name = child->getColumnName();
			const TypeAndConstantInference::ExpressionInfo & child_info = info.at(child_name);
			columns_for_analysis.emplace_back(
				child_info.is_constant_expression ? child_info.data_type->createConstColumn(1, child_info.value) : nullptr,
				child_info.data_type,
				child_name);

			if (!child_info.is_constant_expression)
				all_consts = false;
		}
	}

	TypeAndConstantInference::ExpressionInfo expression_info;
	expression_info.node = ast;
	expression_info.function = function_ptr;
	std::vector<ExpressionAction> unused_prerequisites;
	function_ptr->getReturnTypeAndPrerequisites(columns_for_analysis, expression_info.data_type, unused_prerequisites);

	if (all_consts && function_ptr->isSuitableForConstantFolding())
	{
		Block block_with_constants(columns_for_analysis);

		ColumnNumbers argument_numbers(columns_for_analysis.size());
		for (size_t i = 0, size = argument_numbers.size(); i < size; ++i)
			argument_numbers[i] = i;

		size_t result_position = argument_numbers.size();
		block_with_constants.insert({nullptr, expression_info.data_type, column_name});

		function_ptr->execute(block_with_constants, argument_numbers, result_position);

		const auto & result_column = block_with_constants.getByPosition(result_position).column;
		if (result_column->isConst())
		{
			expression_info.is_constant_expression = true;
			expression_info.value = (*result_column)[0];
		}
	}

	info.emplace(column_name, std::move(expression_info));
}


void processScalarSubquery(const String & column_name, ASTPtr & ast, TypeAndConstantInference::Info & info,
	Context & context)
{
	ASTSubquery * subquery = static_cast<ASTSubquery *>(ast.get());

	AnalyzeResultOfQuery analyzer;
	analyzer.process(subquery->children.at(0), context);

	if (!analyzer.result)
		throw Exception("Logical error: no columns returned from scalar subquery", ErrorCodes::LOGICAL_ERROR);

	TypeAndConstantInference::ExpressionInfo expression_info;
	expression_info.node = ast;

	if (analyzer.result.columns() == 1)
	{
		const auto & elem = analyzer.result.getByPosition(0);
		expression_info.data_type = elem.type;

		if (elem.column)
		{
			expression_info.is_constant_expression = true;
			expression_info.value = getValueFromConstantColumn(elem.column);
		}
	}
	else
	{
		/// Result of scalar subquery is interpreted as tuple.
		size_t size = analyzer.result.columns();
		DataTypes types;
		types.reserve(size);
		bool all_consts = true;
		for (size_t i = 0; i < size; ++i)
		{
			const auto & elem = analyzer.result.getByPosition(i);
			types.emplace_back(elem.type);
			if (!elem.column)
				all_consts = false;
		}

		expression_info.data_type = std::make_shared<DataTypeTuple>(types);

		if (all_consts)
		{
			TupleBackend value(size);

			for (size_t i = 0; i < size; ++i)
				value[i] = getValueFromConstantColumn(analyzer.result.getByPosition(i).column);

			expression_info.is_constant_expression = true;
			expression_info.value = Tuple(std::move(value));
		}
	}

	info.emplace(column_name, std::move(expression_info));
}


void processImpl(
	ASTPtr & ast, Context & context,
	CollectAliases & aliases, const AnalyzeColumns & columns,
	TypeAndConstantInference::Info & info,
	LambdaScopes & lambda_scopes)
{
	/// Depth-first

	/// Don't go into components of compound identifiers.
	if (!typeid_cast<const ASTIdentifier *>(ast.get()))
	{
		for (auto & child : ast->children)
		{
			/** Don't go into subqueries and table-like expressions.
			  * Also don't go into components of compound identifiers.
			  */
			if (typeid_cast<const ASTSelectQuery *>(child.get())
				|| typeid_cast<const ASTTableExpression *>(child.get()))
				continue;

			processImpl(child, context, aliases, columns, info, lambda_scopes);
		}
	}

	const ASTLiteral * literal = nullptr;
	const ASTIdentifier * identifier = nullptr;
	const ASTFunction * function = nullptr;
	const ASTSubquery * subquery = nullptr;

	false
		|| (literal = typeid_cast<const ASTLiteral *>(ast.get()))
		|| (identifier = typeid_cast<const ASTIdentifier *>(ast.get()))
		|| (function = typeid_cast<const ASTFunction *>(ast.get()))
		|| (subquery = typeid_cast<const ASTSubquery *>(ast.get()));

	if (!literal && !identifier && !function && !subquery)
		return;

	/// Same expression is already processed.
	String column_name = ast->getColumnName();
	if (info.count(column_name))
		return;

	if (literal)
		processLiteral(column_name, ast, info);
	else if (identifier)
		processIdentifier(column_name, ast, info, context, aliases, columns, lambda_scopes);
	else if (function)
		processFunction(column_name, ast, info, context);
	else if (subquery)
		processScalarSubquery(column_name, ast, info, context);
}

}


void TypeAndConstantInference::process(ASTPtr & ast, Context & context, CollectAliases & aliases, const AnalyzeColumns & columns)
{
	LambdaScopes lambda_scopes;
	processImpl(ast, context, aliases, columns, info, lambda_scopes);
}


void TypeAndConstantInference::dump(WriteBuffer & out) const
{
	/// For need of tests, we need to dump result in some fixed order.
	std::vector<Info::const_iterator> vec;
	vec.reserve(info.size());
	for (auto it = info.begin(); it != info.end(); ++it)
		vec.emplace_back(it);

	std::sort(vec.begin(), vec.end(), [](const auto & a, const auto & b) { return a->first < b->first; });

	for (const auto & it : vec)
	{
		writeString(it->first, out);
		writeCString(" -> ", out);
		writeString(it->second.data_type->getName(), out);

		if (it->second.is_constant_expression)
		{
			writeCString(" = ", out);
			String value = applyVisitor(FieldVisitorToString(), it->second.value);
			writeString(value, out);
		}

		writeCString(". AST: ", out);
		if (!it->second.node)
			writeCString("(none)", out);
		else
		{
			std::stringstream formatted_ast;
			formatAST(*it->second.node, formatted_ast, 0, false, true);
			writeString(formatted_ast.str(), out);
		}

		writeChar('\n', out);
	}
}

}
