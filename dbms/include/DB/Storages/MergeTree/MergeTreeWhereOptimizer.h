#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Common/escapeForFileName.h>
#include <unordered_map>
#include <set>
#include <cstddef>

#include <DB/Parsers/formatAST.h>

namespace DB
{


class MergeTreeWhereOptimizer
{
	static constexpr auto threshold = 10;
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
		/** @todo consider relaxing requirement on the absence of prewhere_expression
		  * by transforming it to conjunction form if it is not already. */
		if (!select.where_expression || select.prewhere_expression)
			return;

		const auto fun = typeid_cast<ASTFunction *>(select.where_expression.get());
		if (fun && fun->name == and_function_name)
			optimizeAnd(select, fun);
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

		const auto conditions = typeid_cast<ASTExpressionList *>(fun->arguments.get());

		/// remove condition by swapping it with the last one and calling ::pop_back()
		const auto remove_condition_at_index = [conditions] (const size_t idx) {
			if (idx < conditions->children.size())
				conditions->children[idx] = std::move(conditions->children.back());
			conditions->children.pop_back();
		};

		/// linearize conjunction and divide conditions into "good" and not-"good" ones
		for (size_t i = 0; i < conditions->children.size();)
		{
			const auto condition = conditions->children[i].get();

			/// linearize sub-conjunctions
			if (const auto fun = typeid_cast<ASTFunction *>(condition))
			{
				if (fun->name == and_function_name)
				{
					const auto sub_conditions = typeid_cast<ASTExpressionList *>(fun->arguments.get());

					for (auto & child : sub_conditions->children)
						conditions->children.push_back(std::move(child));

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
			select.prewhere_expression = std::move(conditions->children[idx]);

			/** Replace conjunction with the only remaining argument if only two conditions were presentotherwise,
			 *  remove selected condition from conjunction otherwise. */
			if (conditions->children.size() == 2)
				select.where_expression = std::move(conditions->children[idx == 0 ? 1 : 0]);
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


	bool isConditionGood(const IAST * condition, std::string & column_name)
	{
		const auto fun = typeid_cast<const ASTFunction *>(condition);
		if (!fun)
			return false;

		/** We are only considering conditions of form `equals(one, another)` or `one = another`,
		  * especially if either `one` or `another` is ASTIdentifier */
		if (fun->name != equals_function_name)
			return false;

		const auto args = static_cast<const ASTExpressionList *>(fun->arguments.get());
		auto left_arg = args->children.front().get();
		auto right_arg = args->children.back().get();

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
