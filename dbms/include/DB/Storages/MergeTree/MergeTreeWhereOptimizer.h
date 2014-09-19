#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Common/escapeForFileName.h>
#include <statdaemons/stdext.h>
#include <unordered_map>
#include <map>
#include <limits>
#include <cstddef>

namespace DB
{


class MergeTreeWhereOptimizer
{
	static constexpr auto threshold = 10;
	static constexpr auto max_columns_relative_size = 0.25f;
	static constexpr auto and_function_name = "and";
	static constexpr auto equals_function_name = "equals";

public:
	MergeTreeWhereOptimizer(const MergeTreeWhereOptimizer&) = delete;
	MergeTreeWhereOptimizer& operator=(const MergeTreeWhereOptimizer&) = delete;

	MergeTreeWhereOptimizer(ASTSelectQuery & select, const MergeTreeData & data, const Names & column_names)
	{
		fillPrimaryKeyColumns(data);
		calculateColumnSizes(data, column_names);
		optimize(select);
	}

private:
	void optimize(ASTSelectQuery & select) const
	{
		if (!select.where_expression)
			return;

		const auto function = typeid_cast<ASTFunction *>(select.where_expression.get());
		if (function && function->name == and_function_name)
			optimizeConjunction(select, function);
		else
			optimizeArbitrary(select);
	}

	void fillPrimaryKeyColumns(const MergeTreeData & data)
	{
		for (const auto column : data.getPrimaryExpression()->getRequiredColumnsWithTypes())
			primary_key_columns.insert(column.name);
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
			if (idx < conditions.size())
				conditions[idx] = std::move(conditions.back());
			conditions.pop_back();
		};

		/// linearize conjunction and divide conditions into "good" and not-"good" ones
		for (std::size_t idx = 0; idx < conditions.size();)
		{
			const auto condition = conditions[idx].get();

			IdentifierNameSet identifiers{};
			condition->collectIdentifierNames(identifiers);

			/// do not take into consideration the conditions consisting only of primary key columns
			if (hasNonPrimaryKeyColumns(identifiers))
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
			addConditionTo(conditions[idx], select.prewhere_expression);

			/** Replace conjunction with the only remaining argument if only two conditions were presentotherwise,
			 *  remove selected condition from conjunction otherwise. */
			if (conditions.size() == 2)
				select.where_expression = std::move(conditions[idx == 0 ? 1 : 0]);
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
		condition->collectIdentifierNames(identifiers);

		if (!hasNonPrimaryKeyColumns(identifiers))
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
		addConditionTo(std::move(condition), select.prewhere_expression);
		condition = nullptr;
	}

	std::size_t getIdentifiersColumnSize(const IdentifierNameSet & identifiers) const
	{
		std::size_t size{};

		for (const auto & identifier : identifiers)
			size += column_sizes.find(identifier)->second;

		return size;
	}

	void addConditionTo(ASTPtr condition, ASTPtr & ast) const
	{
		/** if there already are some conditions - either combine them using conjunction
		 *  or add new argument to existing conjunction; just set ast to condition otherwise. */
		if (ast)
		{
			const auto function = typeid_cast<ASTFunction *>(ast.get());
			if (function && function->name == and_function_name)
			{
				/// add new argument to the conjunction
				function->arguments->children.emplace_back(std::move(condition));
			}
			else
			{
				/// create a conjunction which will host old condition and the one being added
				auto conjunction = stdext::make_unique<ASTFunction>();
				conjunction->name = and_function_name;
				conjunction->arguments = stdext::make_unique<ASTExpressionList>().release();
				conjunction->children.push_back(conjunction->arguments);

				conjunction->arguments->children.emplace_back(std::move(ast));
				conjunction->arguments->children.emplace_back(std::move(condition));

				ast = conjunction.release();
			}
		}
		else
			ast = std::move(condition);
	}

	bool hasNonPrimaryKeyColumns(const IdentifierNameSet & identifiers) const {
		for (const auto & identifier : identifiers)
			if (primary_key_columns.count(identifier) == 0)
				return true;

		return false;
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

		if (const auto identifier = typeid_cast<const ASTIdentifier *>(left_arg))
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

	std::unordered_set<std::string> primary_key_columns{};
	std::unordered_map<std::string, std::size_t> column_sizes{};
	std::size_t total_column_size{};
};


}
