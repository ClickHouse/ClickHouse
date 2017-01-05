#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Analyzers/CollectAliases.h>
#include <DB/Analyzers/AnalyzeColumns.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTTablesInSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/DataTypes/FieldToDataType.h>
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


static void processImpl(
	const ASTPtr & ast, const Context & context,
	const CollectAliases & aliases, const AnalyzeColumns & columns,
	TypeAndConstantInference::Info & info)
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

			processImpl(child, context, aliases, columns, info);
		}
	}

	const ASTLiteral * literal = nullptr;
	const ASTIdentifier * identifier = nullptr;
	const ASTFunction * function = nullptr;

	false
		|| (literal = typeid_cast<const ASTLiteral *>(ast.get()))
		|| (identifier = typeid_cast<const ASTIdentifier *>(ast.get()))
		|| (function = typeid_cast<const ASTFunction *>(ast.get()));

	if (!literal && !identifier && !function)
		return;

	/// TODO Scalar subqueries.
	/// TODO Subqueries in IN.

	/// Same expression is already processed.
	String column_name = ast->getColumnName();
	if (info.count(column_name))
		return;

	if (literal)
	{
		TypeAndConstantInference::ExpressionInfo expression_info;
		expression_info.node = ast;
		expression_info.is_constant_expression = true;
		expression_info.value = literal->value;
		expression_info.data_type = apply_visitor(FieldToDataType(), expression_info.value);
		info.emplace(column_name, std::move(expression_info));
	}
	else if (identifier)
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
					if (!column_from_subquery.column->isConst())
						throw Exception("Logical error: expected that column is constant", ErrorCodes::LOGICAL_ERROR);
					if (column_from_subquery.column->size() != 1)
						throw Exception("Logical error: expected that column with constant has single element", ErrorCodes::LOGICAL_ERROR);

					expression_info.is_constant_expression = true;
					expression_info.value = (*column_from_subquery.column)[0];
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

				processImpl(it->second.node, context, aliases, columns, info);
				info[column_name] = info[it->second.node->getColumnName()];
			}
		}
	}
	else if (function)
	{
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

		if (AggregateFunctionPtr aggregate_function_ptr = context.getAggregateFunctionFactory().tryGet(function->name, argument_types))
		{
			/// Aggregate function.
			/// NOTE Not considering aggregate function parameters in type inference. It could become needed in future.
			/// Note that aggregate function could never be constant expression.

			aggregate_function_ptr->setArguments(argument_types);

			TypeAndConstantInference::ExpressionInfo expression_info;
			expression_info.node = ast;
			expression_info.data_type = aggregate_function_ptr->getReturnType();
			info.emplace(column_name, std::move(expression_info));
		}
		else
		{
			/// Ordinary function.
			if (function->parameters)
				throw Exception("The only parametric functions (functions with two separate parenthesis pairs) are aggregate functions"
					", and '" + function->name + "' is not an aggregate function.", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

			const FunctionPtr & function_ptr = FunctionFactory::instance().get(function->name, context);

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
	}
}


void TypeAndConstantInference::process(ASTPtr & ast, const Context & context, const CollectAliases & aliases, const AnalyzeColumns & columns)
{
	processImpl(ast, context, aliases, columns, info);
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
			String value = apply_visitor(FieldVisitorToString(), it->second.value);
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
