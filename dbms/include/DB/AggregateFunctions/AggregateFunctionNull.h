#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

/// This class implements a wrapper around an aggregate function. Despite its name,
/// this is an adapter. It is used to handle aggregate functions that are called with
/// at least one nullable argument. It implements the logic according to which any
/// row that contains at least one NULL is skipped.
class AggregateFunctionNull : public IAggregateFunction
{
public:
	AggregateFunctionNull(AggregateFunctionPtr nested_function_)
		: nested_function{nested_function_}
	{
	}

	String getName() const override
	{
		return nested_function->getName();
	}

	void setArguments(const DataTypes & arguments) override
	{
		argument_count = arguments.size();
		is_nullable.reserve(arguments.size());

		for (const auto & arg : arguments)
		{
			bool res = arg->isNullable();
			is_nullable.push_back(res);
		}

		DataTypes new_args;
		new_args.reserve(arguments.size());

		for (const auto & arg : arguments)
		{
			if (arg->isNullable())
			{
				const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*arg);
				const DataTypePtr & nested_type = nullable_type.getNestedType();
				new_args.push_back(nested_type);
			}
			else
				new_args.push_back(arg);
		}

		nested_function->setArguments(new_args);
	}

	void setParameters(const Array & params) override
	{
		nested_function->setParameters(params);
	}

	DataTypePtr getReturnType() const override
	{
		return nested_function->getReturnType();
	}

	void create(AggregateDataPtr place) const override
	{
		nested_function->create(place);
	}

	void destroy(AggregateDataPtr place) const noexcept override
	{
		nested_function->destroy(place);
	}

	bool hasTrivialDestructor() const override
	{
		return nested_function->hasTrivialDestructor();
	}

	size_t sizeOfData() const override
	{
		return nested_function->sizeOfData();
	}

	size_t alignOfData() const override
	{
		return nested_function->alignOfData();
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
	{
		/// This container stores the columns we really pass to the nested function.
		const IColumn * passed_columns[argument_count];

		for (size_t i = 0; i < argument_count; ++i)
		{
			if (is_nullable[i])
			{
				const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*columns[i]);
				if (nullable_col.isNullAt(row_num))
				{
					/// If at least one column has a null value in the current row,
					/// we don't process this row.
					return;
				}
				passed_columns[i] = nullable_col.getNestedColumn().get();
			}
			else
				passed_columns[i] = columns[i];
		}

		nested_function->add(place, passed_columns, row_num, arena);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
	{
		nested_function->merge(place, rhs, arena);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		nested_function->serialize(place, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
	{
		nested_function->deserialize(place, buf, arena);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		nested_function->insertResultInto(place, to);
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place,
		const IColumn ** columns, size_t row_num, Arena * arena)
	{
		return static_cast<const AggregateFunctionNull &>(*that).add(place, columns, row_num, arena);
	}

	AddFunc getAddressOfAddFunction() const override
	{
		return &addFree;
	}

private:
	AggregateFunctionPtr nested_function;
	std::vector<bool> is_nullable;
	size_t argument_count = 0;
};

}
