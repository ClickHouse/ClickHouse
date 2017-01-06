#include <DB/Analyzers/OptimizeGroupOrderLimitBy.h>
#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Interpreters/Context.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int UNEXPECTED_AST_STRUCTURE;
}


static bool isInjectiveFunction(
	const ASTFunction * ast_function,
	const TypeAndConstantInference::ExpressionInfo & function_info,
	const TypeAndConstantInference::Info & all_info)
{
	if (!function_info.function)
		return false;

	Block block_with_constants;

	const ASTs & children = ast_function->arguments->children;
	for (const auto & child : children)
	{
		String child_name = child->getColumnName();
		const TypeAndConstantInference::ExpressionInfo & child_info = all_info.at(child_name);

		block_with_constants.insert(ColumnWithTypeAndName(
			child_info.is_constant_expression ? child_info.data_type->createConstColumn(1, child_info.value) : nullptr,
			child_info.data_type,
			child_name));
	}

	return function_info.function->isInjective(block_with_constants);
}


static bool isDeterministicFunctionOfKeys(
	const ASTFunction * ast_function,
	const TypeAndConstantInference::ExpressionInfo & function_info,
	const TypeAndConstantInference::Info & all_info,
	const ASTs & keys)
{
	if (!function_info.function || !function_info.function->isDeterministicInScopeOfQuery())
		return false;

	for (const auto & child : ast_function->arguments->children)
	{
		String child_name = child->getColumnName();
		const TypeAndConstantInference::ExpressionInfo & child_info = all_info.at(child_name);

		/// Function argument is constant.
		if (child_info.is_constant_expression)
			continue;

		/// Function argument is one of keys.
		if (keys.end() != std::find_if(keys.begin(), keys.end(),
			[&child_name](const auto & key) { return key->getColumnName() == child_name; }))
			continue;

		/// Function argument is a function, that deterministically depend on keys.
		if (const ASTFunction * child_function = typeid_cast<const ASTFunction *>(child.get()))
		{
			if (isDeterministicFunctionOfKeys(child_function, child_info, all_info, keys))
				continue;
		}

		return false;
	}

	return true;
}


static void processGroupByLikeList(ASTPtr & ast, TypeAndConstantInference & expression_info)
{
	if (!ast)
		return;

	ASTs & elems = ast->children;

	std::unordered_set<std::string> unique_keys;
	size_t i = 0;

	auto restart = [&]
	{
		i = 0;
		unique_keys.clear();
	};

	/// Always leave last element in GROUP BY, even if it is constant.
	while (i < elems.size() && elems.size() > 1)
	{
		ASTPtr & elem = elems[i];

		String column_name = elem->getColumnName();				/// TODO canonicalization of names
		auto it = expression_info.info.find(column_name);
		if (it == expression_info.info.end())
			throw Exception("Type inference was not done for " + column_name, ErrorCodes::LOGICAL_ERROR);
		const TypeAndConstantInference::ExpressionInfo & info = it->second;

		/// Removing constant expressions.
		/// Removing duplicate keys.
		if (info.is_constant_expression
			|| !unique_keys.emplace(column_name).second)
		{
			elems.erase(elems.begin() + i);
			continue;
		}

		if (info.function && !elem->children.empty())
		{
			const ASTFunction * ast_function = typeid_cast<const ASTFunction *>(elem.get());
			if (!ast_function)
				throw Exception("Column is marked as function during type inference, but corresponding AST node "
					+ column_name + " is not a function", ErrorCodes::LOGICAL_ERROR);

			/// Unwrap injective functions.
			if (isInjectiveFunction(ast_function, info, expression_info.info))
			{
				auto args = ast_function->arguments;
				elems.erase(elems.begin() + i);
				elems.insert(elems.begin() + i, args->children.begin(), args->children.end());

				restart();	/// Previous keys may become deterministic function of newly added keys.
				continue;
			}

			/// Remove deterministic functions of another keys.
			ASTs other_keys;
			other_keys.reserve(elems.size() - 1);
			for (size_t j = 0, size = elems.size(); j < size; ++j)
				if (j != i)
					other_keys.emplace_back(elems[j]);

			if (isDeterministicFunctionOfKeys(ast_function, info, expression_info.info, other_keys))
			{
				elems.erase(elems.begin() + i);
				continue;
			}
		}

		++i;
	}
}


void OptimizeGroupOrderLimitBy::process(ASTPtr & ast, TypeAndConstantInference & expression_info)
{
	ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get());
	if (!select)
		throw Exception("AnalyzeResultOfQuery::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
	if (!select->select_expression_list)
		throw Exception("SELECT query doesn't have select_expression_list", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

	processGroupByLikeList(select->group_expression_list, expression_info);
	processGroupByLikeList(select->limit_by_expression_list, expression_info);
}


}
