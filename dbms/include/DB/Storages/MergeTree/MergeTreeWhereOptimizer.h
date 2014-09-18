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
	MergeTreeWhereOptimizer(const MergeTreeData & data, const MergeTreeData::DataPartsVector & parts)
	{
		fillPrimaryKeyColumns(data);
		calculateColumnSizes(parts);
	}

	void optimize(ASTSelectQuery & select)
	{
		if (!select.where_expression)
			return;

		const auto function = typeid_cast<ASTFunction *>(select.where_expression.get());
		if (function && function->name == and_function_name)
			optimize(select, function);
	}

private:
	void fillPrimaryKeyColumns(const MergeTreeData & data)
	{
		for (const auto column : data.getPrimaryExpression()->getRequiredColumnsWithTypes())
			primary_key_columns.insert(column.name);
	}

	void calculateColumnSizes(const MergeTreeData::DataPartsVector & parts)
	{
		for (const auto & part : parts)
		{
			for (const auto & file : part->checksums.files)
			{
				const auto file_name = unescapeForFileName(file.first);
				const auto column_name = file_name.substr(0, file_name.find_last_of('.'));

				column_sizes[column_name] += file.second.file_size;
			}
		}
	}

	void optimize(ASTSelectQuery & select, ASTFunction * const fun)
	{
		/// column size => index of condition which uses said row
		std::map<size_t, size_t> good_conditions{};
		std::map<size_t, size_t> viable_conditions{};

		auto & conditions = fun->arguments->children;

		/// remove condition by swapping it with the last one and calling ::pop_back()
		const auto remove_condition_at_index = [&conditions] (const std::size_t idx) {
			if (idx < conditions.size())
				conditions[idx] = std::move(conditions.back());
			conditions.pop_back();
		};

		/// linearize conjunction and divide conditions into "good" and not-"good" ones
		for (std::size_t i = 0; i < conditions.size();)
		{
			const auto condition = conditions[i].get();

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
						remove_condition_at_index(i);

						/// continue iterating without increment to ensure the just added conditions are processed
						continue;
					}
				}

				/// calculate size of columns involved in condition
				std::size_t cond_columns_size{};
				for (const auto & identifier : identifiers)
					cond_columns_size += column_sizes[identifier];

				/// place condition either in good or viable conditions set
				(isConditionGood(condition) ? good_conditions : viable_conditions).emplace(cond_columns_size, i);
			}

			++i;
		}

		const auto move_condition_to_prewhere = [&] (const std::size_t cond_idx) {
			addConditionTo(conditions[cond_idx], select.prewhere_expression);

			/** Replace conjunction with the only remaining argument if only two conditions were presentotherwise,
			 *  remove selected condition from conjunction otherwise. */
			if (conditions.size() == 2)
				select.where_expression = std::move(conditions[cond_idx == 0 ? 1 : 0]);
			else
				remove_condition_at_index(cond_idx);
		};

		/// if there are "good" conditions - select the one with the least compressed size
		if (!good_conditions.empty())
		{
			move_condition_to_prewhere(good_conditions.begin()->second);
		}
		else if (!viable_conditions.empty())
		{
			/// find all columns used in query
			IdentifierNameSet identifiers{};
			if (select.select_expression_list) select.select_expression_list->collectIdentifierNames(identifiers);
			if (select.sample_size) select.sample_size->collectIdentifierNames(identifiers);
			if (select.prewhere_expression) select.prewhere_expression->collectIdentifierNames(identifiers);
			if (select.where_expression) select.where_expression->collectIdentifierNames(identifiers);
			if (select.group_expression_list) select.group_expression_list->collectIdentifierNames(identifiers);
			if (select.having_expression) select.having_expression->collectIdentifierNames(identifiers);
			if (select.order_expression_list) select.order_expression_list->collectIdentifierNames(identifiers);
			std::size_t total_column_size{};

			/// calculate size of columns involved in query
			for (const auto & identifier : identifiers)
				total_column_size += column_sizes[identifier];

			/// calculate relative size of condition's columns
			const auto cond_columns_size = viable_conditions.begin()->first;
			const auto columns_relative_size = static_cast<float>(cond_columns_size) / total_column_size;

			/// do nothing if it exceeds max relative size
			if (columns_relative_size > max_columns_relative_size)
				return;

			move_condition_to_prewhere(viable_conditions.begin()->second);
		}
	}

	void optimizeArbitrary(ASTSelectQuery & select)
	{
	}

	void addConditionTo(ASTPtr condition, ASTPtr & ast)
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
};


}
