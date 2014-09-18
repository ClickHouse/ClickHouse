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
#include <set>
#include <cstddef>

#include <DB/Parsers/formatAST.h>

namespace DB
{


class MergeTreeWhereOptimizer
{
	static constexpr auto threshold = 10;
	static constexpr auto max_column_relative_size = 0.25f;
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
			optimizeAnd(select, function);
		else
			optimizeArbitrary(select);

		std::cout << "(possibly) transformed query is: ";
		formatAST(select, std::cout);
		std::cout << std::endl;
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
				const auto column_file_size = file.second.file_size;

				column_sizes[column_name] += column_file_size;
				total_size += column_file_size;
			}
		}
	}

	void optimizeAnd(ASTSelectQuery & select, ASTFunction * const fun)
	{
		/// column size => index of condition which uses said row
		std::map<size_t, size_t> good_conditions{};
		/// index of condition
		std::set<size_t> viable_conditions{};

		auto & conditions = fun->arguments->children;

		/// remove condition by swapping it with the last one and calling ::pop_back()
		const auto remove_condition_at_index = [&conditions] (const size_t idx) {
			if (idx < conditions.size())
				conditions[idx] = std::move(conditions.back());
			conditions.pop_back();
		};

		/// linearize conjunction and divide conditions into "good" and not-"good" ones
		for (size_t i = 0; i < conditions.size();)
		{
			const auto condition = conditions[i].get();

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

			/// identify condition as either "good" or not
			std::string column_name{};
			if (isConditionGood(condition, column_name))
				good_conditions.emplace(column_sizes[column_name], i);
			else
				viable_conditions.emplace(i);

			++i;
		}

		/// if there are "good" conditions - select the one with the least compressed size
		if (!good_conditions.empty())
		{
			const auto idx = good_conditions.begin()->second;
			addConditionTo(conditions[idx], select.prewhere_expression);

			/** Replace conjunction with the only remaining argument if only two conditions were presentotherwise,
			 *  remove selected condition from conjunction otherwise. */
			if (conditions.size() == 2)
				select.where_expression = std::move(conditions[idx == 0 ? 1 : 0]);
			else
				remove_condition_at_index(idx);
		}
		else if (!viable_conditions.empty())
		{
			/// @todo implement not-"good" condition transformation
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

	bool isConditionGood(const IAST * condition, std::string & column_name)
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
			/// if the identifier is part of the primary key, the condition is not "good"
			if (primary_key_columns.count(identifier->name))
				return false;

			column_name = identifier->name;

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
	std::size_t total_size{};
};


}
