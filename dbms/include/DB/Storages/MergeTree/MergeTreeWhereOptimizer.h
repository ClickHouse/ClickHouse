#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSubquery.h>
#include <DB/Parsers/ASTSet.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <ext/scope_guard.hpp>
#include <ext/map.hpp>
#include <memory>
#include <unordered_map>
#include <map>
#include <limits>
#include <cstddef>


namespace DB
{


/** Identifies WHERE expressions that can be placed in PREWHERE by calculating respective
 *  sizes of columns used in particular expression and identifying "good" conditions of
 *  form "column_name = constant", where "constant" is outside some `threshold` specified in advance.
 *
 *  If there are "good" conditions present in WHERE, the one with minimal summary column size is
 *  transferred to PREWHERE.
 *  Otherwise any condition with minimal summary column size can be transferred to PREWHERE, if only
 *  its relative size (summary column size divided by query column size) is less than `max_columns_relative_size`.
 */
class MergeTreeWhereOptimizer
{
	static constexpr auto threshold = 10;
	/// Решили убрать ограничение в виду отсутствия штрафа по скорости на перенос в PREWHERE
	static constexpr auto max_columns_relative_size = 1.0f;
	static constexpr auto and_function_name = "and";
	static constexpr auto equals_function_name = "equals";
	static constexpr auto array_join_function_name = "arrayJoin";
	static constexpr auto global_in_function_name = "globalIn";
	static constexpr auto global_not_in_function_name = "globalNotIn";

public:
	MergeTreeWhereOptimizer(const MergeTreeWhereOptimizer&) = delete;
	MergeTreeWhereOptimizer& operator=(const MergeTreeWhereOptimizer&) = delete;

	MergeTreeWhereOptimizer(
		ASTPtr & query, const Context & context, const MergeTreeData & data, const Names & column_names,
		Logger * log)
		: primary_key_columns{ext::map<std::unordered_set>(data.getSortDescription(),
			[] (const SortColumnDescription & col) { return col.column_name; })
		  },
		  table_columns{ext::map<std::unordered_set>(data.getColumnsList(),
			[] (const NameAndTypePair & col) { return col.name; })
		  }, block_with_constants{PKCondition::getBlockWithConstants(query, context, data.getColumnsList())},
		  log{log}
	{
		calculateColumnSizes(data, column_names);
		auto & select = typeid_cast<ASTSelectQuery &>(*query);
		determineArrayJoinedNames(select);
		optimize(select);
	}

private:
	void optimize(ASTSelectQuery & select) const
	{
		if (!select.where_expression || select.prewhere_expression)
			return;

		const auto function = typeid_cast<ASTFunction *>(select.where_expression.get());
		if (function && function->name == and_function_name)
			optimizeConjunction(select, function);
		else
			optimizeArbitrary(select);
	}

	void calculateColumnSizes(const MergeTreeData & data, const Names & column_names)
	{
		for (const auto & column_name : column_names)
		{
			const auto column_size = data.getColumnSize(column_name);

			column_sizes[column_name] = column_size;
			total_column_size += column_size;
		}
	}

	void optimizeConjunction(ASTSelectQuery & select, ASTFunction * const fun) const
	{
		/// used as max possible size and indicator that appropriate condition has not been found
		const auto no_such_condition = std::numeric_limits<std::size_t>::max();

		/// { first: condition index, second: summary column size }
		std::pair<std::size_t, std::size_t> lightest_good_condition{no_such_condition, no_such_condition};
		std::pair<std::size_t, std::size_t> lightest_viable_condition{no_such_condition, no_such_condition};

		auto & conditions = fun->arguments->children;

		/// remove condition by swapping it with the last one and calling ::pop_back()
		const auto remove_condition_at_index = [&conditions] (const std::size_t idx) {
			if (idx < conditions.size() - 1)
				std::swap(conditions[idx], conditions.back());
			conditions.pop_back();
		};

		/// linearize conjunction and divide conditions into "good" and not-"good" ones
		for (std::size_t idx = 0; idx < conditions.size();)
		{
			const auto condition = conditions[idx].get();

			/// linearize sub-conjunctions
			if (const auto function = typeid_cast<ASTFunction *>(condition))
			{
				if (function->name == and_function_name)
				{
					for (auto & child : function->arguments->children)
						conditions.emplace_back(std::move(child));

					/// remove the condition corresponding to conjunction
					remove_condition_at_index(idx);

					/// continue iterating without increment to ensure the just added conditions are processed
					continue;
				}
			}

			SCOPE_EXIT(++idx);

			if (cannotBeMoved(condition))
				continue;

			IdentifierNameSet identifiers{};
			collectIdentifiersNoSubqueries(condition, identifiers);

			/// do not take into consideration the conditions consisting only of primary key columns
			if (!hasPrimaryKeyAtoms(condition) && isSubsetOfTableColumns(identifiers))
			{
				/// calculate size of columns involved in condition
				const auto cond_columns_size = getIdentifiersColumnSize(identifiers);

				/// place condition either in good or viable conditions set
				auto & good_or_viable_condition = isConditionGood(condition) ? lightest_good_condition : lightest_viable_condition;
				if (good_or_viable_condition.second > cond_columns_size)
				{
					good_or_viable_condition.first = idx;
					good_or_viable_condition.second = cond_columns_size;
				}
			}
		}

		const auto move_condition_to_prewhere = [&] (const std::size_t idx) {
			select.prewhere_expression = conditions[idx];
			select.children.push_back(select.prewhere_expression);
			LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition `" << select.prewhere_expression << "` moved to PREWHERE");

			/** Replace conjunction with the only remaining argument if only two conditions were present,
			 *  remove selected condition from conjunction otherwise. */
			if (conditions.size() == 2)
			{
				/// find old where_expression in children of select
				const auto it = std::find(std::begin(select.children), std::end(select.children), select.where_expression);
				/// replace where_expression with the remaining argument
				select.where_expression = std::move(conditions[idx == 0 ? 1 : 0]);
				/// overwrite child entry with the new where_expression
				*it = select.where_expression;
			}
			else
				remove_condition_at_index(idx);
		};

		/// if there is a "good" condition - move it to PREWHERE
		if (lightest_good_condition.first != no_such_condition)
		{
			move_condition_to_prewhere(lightest_good_condition.first);
		}
		else if (lightest_viable_condition.first != no_such_condition)
		{
			/// check that the relative column size is less than max
			if (total_column_size != 0)
			{
				/// calculate relative size of condition's columns
				const auto cond_columns_size = lightest_viable_condition.second;
				const auto columns_relative_size = static_cast<float>(cond_columns_size) / total_column_size;

				/// do nothing if it exceeds max relative size
				if (columns_relative_size > max_columns_relative_size)
					return;
			}

			move_condition_to_prewhere(lightest_viable_condition.first);
		}
	}

	void optimizeArbitrary(ASTSelectQuery & select) const
	{
		auto & condition = select.where_expression;

		/// do not optimize restricted expressions
		if (cannotBeMoved(select.where_expression.get()))
			return;

		IdentifierNameSet identifiers{};
		collectIdentifiersNoSubqueries(condition.get(), identifiers);

		if (hasPrimaryKeyAtoms(condition.get()) || !isSubsetOfTableColumns(identifiers))
			return;

		/// if condition is not "good" - check that it can be moved
		if (!isConditionGood(condition.get()) && total_column_size != 0)
		{
			const auto cond_columns_size = getIdentifiersColumnSize(identifiers);
			const auto columns_relative_size = static_cast<float>(cond_columns_size) / total_column_size;

			if (columns_relative_size > max_columns_relative_size)
				return;
		}

		/// add the condition to PREWHERE, remove it from WHERE
		std::swap(select.prewhere_expression, condition);
		LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition `" << select.prewhere_expression << "` moved to PREWHERE");
	}

	std::size_t getIdentifiersColumnSize(const IdentifierNameSet & identifiers) const
	{
		/** for expressions containing no columns (or where columns could not be determined otherwise) assume maximum
		 *	possible size so they do not have priority in eligibility over other expressions. */
		if (identifiers.empty())
			return std::numeric_limits<std::size_t>::max();

		std::size_t size{};

		for (const auto & identifier : identifiers)
			if (column_sizes.count(identifier))
				size += column_sizes.find(identifier)->second;

		return size;
	}

	bool isConditionGood(const IAST * condition) const
	{
		const auto function = typeid_cast<const ASTFunction *>(condition);
		if (!function)
			return false;

		/** we are only considering conditions of form `equals(one, another)` or `one = another`,
		  * especially if either `one` or `another` is ASTIdentifier */
		if (function->name != equals_function_name)
			return false;

		auto left_arg = function->arguments->children.front().get();
		auto right_arg = function->arguments->children.back().get();

		/// try to ensure left_arg points to ASTIdentifier
		if (!typeid_cast<const ASTIdentifier *>(left_arg) && typeid_cast<const ASTIdentifier *>(right_arg))
			std::swap(left_arg, right_arg);

		if (typeid_cast<const ASTIdentifier *>(left_arg))
		{
			/// condition may be "good" if only right_arg is a constant and its value is outside the threshold
			if (const auto literal = typeid_cast<const ASTLiteral *>(right_arg))
			{
				const auto & field = literal->value;
				const auto type = field.getType();

				/// check the value with respect to threshold
				if (type == Field::Types::UInt64)
				{
					const auto value = field.get<UInt64>();
					return value > threshold;
				}
				else if (type == Field::Types::Int64)
				{
					const auto value = field.get<Int64>();
					return value < -threshold || threshold < value;
				}
				else if (type == Field::Types::Float64)
				{
					const auto value = field.get<Float64>();
					return value < threshold || threshold < value;
				}
			}
		}

		return false;
	}

	static void collectIdentifiersNoSubqueries(const IAST * const ast, IdentifierNameSet & set)
	{
		if (const auto identifier = typeid_cast<const ASTIdentifier *>(ast))
			return (void) set.insert(identifier->name);

		if (typeid_cast<const ASTSubquery *>(ast))
			return;

		for (const auto & child : ast->children)
			collectIdentifiersNoSubqueries(child.get(), set);
	}

	bool hasPrimaryKeyAtoms(const IAST * ast) const
	{
		if (const auto func = typeid_cast<const ASTFunction *>(ast))
		{
			const auto & args = func->arguments->children;

			if ((func->name == "not" && 1 == args.size()) || func->name == "and" || func->name == "or")
			{
				for (const auto & arg : args)
					if (hasPrimaryKeyAtoms(arg.get()))
						return true;

				return false;
			}
		}

		return isPrimaryKeyAtom(ast);
	}

	bool isPrimaryKeyAtom(const IAST * const ast) const
	{
		if (const auto func = typeid_cast<const ASTFunction *>(ast))
		{
			if (!PKCondition::atom_map.count(func->name))
				return false;

			const auto & args = func->arguments->children;
			if (args.size() != 2)
				return false;

			const auto & first_arg_name = args.front()->getColumnName();
			const auto & second_arg_name = args.back()->getColumnName();

			if ((primary_key_columns.count(first_arg_name) && isConstant(args[1])) ||
				(primary_key_columns.count(second_arg_name) && isConstant(args[0])) ||
				(primary_key_columns.count(first_arg_name)
					&& (typeid_cast<const ASTSet *>(args[1].get()) || typeid_cast<const ASTSubquery *>(args[1].get()))))
				return true;
		}

		return false;
	}

	bool isConstant(const ASTPtr & expr) const
	{
		const auto column_name = expr->getColumnName();

		if (typeid_cast<const ASTLiteral *>(expr.get()) ||
			(block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isConst()))
			return true;

		return false;
	}

	bool isSubsetOfTableColumns(const IdentifierNameSet & identifiers) const
	{
		for (const auto & identifier : identifiers)
			if (table_columns.count(identifier) == 0)
				return false;

		return true;
	}

	/** ARRAY JOIN'ed columns as well as arrayJoin() result cannot be used in PREWHERE, therefore expressions
	  *	containing said columns should not be moved to PREWHERE at all.
	  *	We assume all AS aliases have been expanded prior to using this class
	  *
	  * Also, disallow moving expressions with GLOBAL [NOT] IN.
	  */
	bool cannotBeMoved(const IAST * ptr) const
	{
		if (const auto function_ptr = typeid_cast<const ASTFunction *>(ptr))
		{
			/// disallow arrayJoin expressions to be moved to PREWHERE for now
			if (array_join_function_name == function_ptr->name)
				return true;

			/// disallow GLOBAL IN, GLOBAL NOT IN
			if (global_in_function_name == function_ptr->name
				|| global_not_in_function_name == function_ptr->name)
				return true;

			/// indexHint - особая функция, которую не имеет смысла переносить в PREWHERE
			if ("indexHint" == function_ptr->name)
				return true;
		}
		else if (const auto identifier_ptr = typeid_cast<const ASTIdentifier *>(ptr))
		{
			/// disallow moving result of ARRAY JOIN to PREWHERE
			if (identifier_ptr->kind == ASTIdentifier::Column)
				if (array_joined_names.count(identifier_ptr->name) ||
					array_joined_names.count(DataTypeNested::extractNestedTableName(identifier_ptr->name)))
					return true;
		}

		for (const auto & child : ptr->children)
			if (cannotBeMoved(child.get()))
				return true;

		return false;
	}

	void determineArrayJoinedNames(ASTSelectQuery & select)
	{
		auto array_join_expression_list = select.array_join_expression_list();

		/// much simplified code from ExpressionAnalyzer::getArrayJoinedColumns()
		if (!array_join_expression_list)
			return;

		for (const auto & ast : array_join_expression_list->children)
			array_joined_names.emplace(ast->getAliasOrColumnName());
	}

	using string_set_t = std::unordered_set<std::string>;

	const string_set_t primary_key_columns;
	const string_set_t table_columns;
	const Block block_with_constants;
	Logger * log;
	std::unordered_map<std::string, std::size_t> column_sizes{};
	std::size_t total_column_size{};
	NameSet array_joined_names;
};


}
