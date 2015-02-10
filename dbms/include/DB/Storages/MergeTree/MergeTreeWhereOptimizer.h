#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSubquery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Common/escapeForFileName.h>
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
	static constexpr auto max_columns_relative_size = 0.25f;
	static constexpr auto and_function_name = "and";
	static constexpr auto equals_function_name = "equals";

public:
	MergeTreeWhereOptimizer(const MergeTreeWhereOptimizer&) = delete;
	MergeTreeWhereOptimizer& operator=(const MergeTreeWhereOptimizer&) = delete;

	MergeTreeWhereOptimizer(
		ASTSelectQuery & select, const MergeTreeData & data,
		const Names & column_names, Logger * log)
		: primary_key_columns{toUnorderedSet(data.getPrimaryExpression()->getRequiredColumnsWithTypes())},
		table_columns{toUnorderedSet(data.getColumnsList())}, log{log}
	{
		calculateColumnSizes(data, column_names);
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

			IdentifierNameSet identifiers{};
			collectIdentifiersNoSubqueries(condition, identifiers);

			/// do not take into consideration the conditions consisting only of primary key columns
			if (hasNonPrimaryKeyColumns(identifiers) && isSubsetOfTableColumns(identifiers))
			{
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

			++idx;
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

		IdentifierNameSet identifiers{};
		collectIdentifiersNoSubqueries(condition, identifiers);

		if (!hasNonPrimaryKeyColumns(identifiers) || !isSubsetOfTableColumns(identifiers))
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

	using string_set_t = std::unordered_set<std::string>;

	static string_set_t toUnorderedSet(const NamesAndTypesList & columns)
	{
		string_set_t result{};

		for (const auto column : columns)
			result.insert(column.name);

		return result;
	}

	bool hasNonPrimaryKeyColumns(const IdentifierNameSet & identifiers) const {
		for (const auto & identifier : identifiers)
			if (primary_key_columns.count(identifier) == 0)
				return true;

		return false;
	}

	bool isSubsetOfTableColumns(const IdentifierNameSet & identifiers) const {
		for (const auto & identifier : identifiers)
			if (table_columns.count(identifier) == 0)
				return false;

		return true;
	}

	string_set_t primary_key_columns{};
	string_set_t table_columns{};
	Logger * log;
	std::unordered_map<std::string, std::size_t> column_sizes{};
	std::size_t total_column_size{};
};


}
