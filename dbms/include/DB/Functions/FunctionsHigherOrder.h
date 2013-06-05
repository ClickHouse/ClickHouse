#pragma once

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeExpression.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnExpression.h>

#include <DB/Functions/IFunction.h>
#include "FunctionsMiscellaneous.h"


namespace DB
{
	
/** Функции высшего порядка для массивов:
  * 
  * arrayMap(x -> expression, array) - применить выражение к каждому элементу массива.
  * arrayFilter(x -> predicate, array) - оставить в массиве только элементы, для которых выражение истинно.
  * arrayCount(x -> predicate, array) - для скольки элементов массива выражение истинно.
  * arrayExists(x -> predicate, array) - истинно ли выражение для хотя бы одного элемента массива.
  */

struct ArrayMapImpl
{
	static bool needBooleanExpression() { return false; }
	
	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeArray(expression_return);
	}
	
	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		return new ColumnArray(mapped, array->getOffsetsColumn());
	}
};

struct ArrayFilterImpl
{
	static bool needBooleanExpression() { return true; }
	
	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeArray(array_element);
	}
	
	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		ColumnVector<UInt8> * column_filter = dynamic_cast<ColumnVector<UInt8> *>(&*mapped);
		if (!column_filter)
			throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);
		
		const IColumn::Filter & filter = column_filter->getData();
		ColumnPtr filtered = array->getData().filter(filter);
		
		const IColumn::Offsets_t & in_offsets = array->getOffsets();
		ColumnArray::ColumnOffsets_t * column_offsets = new ColumnArray::ColumnOffsets_t(in_offsets.size());
		ColumnPtr column_offsets_ptr = column_offsets;
		IColumn::Offsets_t & out_offsets = column_offsets->getData();
		
		size_t in_pos = 0;
		size_t out_pos = 0;
		for (size_t i = 0; i < in_offsets.size(); ++i)
		{
			for (; in_pos < in_offsets[i]; ++in_pos)
			{
				if (filter[in_pos])
					++out_pos;
			}
			out_offsets[i] = out_pos;
		}
		
		return new ColumnArray(filtered, column_offsets_ptr);
	}
};

struct ArrayCountImpl
{
	static bool needBooleanExpression() { return true; }
	
	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeUInt32;
	}
	
	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		ColumnVector<UInt8> * column_filter = dynamic_cast<ColumnVector<UInt8> *>(&*mapped);
		if (!column_filter)
			throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);
		
		const IColumn::Filter & filter = column_filter->getData();
		const IColumn::Offsets_t & offsets = array->getOffsets();
		ColumnVector<UInt32> * out_column = new ColumnVector<UInt32>(offsets.size());
		ColumnPtr out_column_ptr = out_column;
		ColumnVector<UInt32>::Container_t & out_counts = out_column->getData();
		
		size_t pos = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			size_t count = 0;
			for (; pos < offsets[i]; ++pos)
			{
				if (filter[pos])
					++count;
			}
			out_counts[i] = count;
		}
		
		return out_column_ptr;
	}
};

struct ArrayExistsImpl
{
	static bool needBooleanExpression() { return true; }
	
	static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & array_element)
	{
		return new DataTypeUInt8;
	}
	
	static ColumnPtr execute(const ColumnArray * array, ColumnPtr mapped)
	{
		ColumnVector<UInt8> * column_filter = dynamic_cast<ColumnVector<UInt8> *>(&*mapped);
		if (!column_filter)
			throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);
		
		const IColumn::Filter & filter = column_filter->getData();
		const IColumn::Offsets_t & offsets = array->getOffsets();
		ColumnVector<UInt8> * out_column = new ColumnVector<UInt8>(offsets.size());
		ColumnPtr out_column_ptr = out_column;
		ColumnVector<UInt8>::Container_t & out_exists = out_column->getData();
		
		size_t pos = 0;
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			UInt8 exists = 0;
			for (; pos < offsets[i]; ++pos)
			{
				if (filter[pos])
				{
					exists = true;
					break;
				}
			}
			out_exists[i] = exists;
		}
		
		return out_column_ptr;
	}
};

template <typename Impl, typename Name>
class FunctionArrayMapped : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}
	
	void checkTypes(const DataTypes & arguments, const DataTypeExpression *& expression_type, const DataTypeArray *& array_type) const {
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
							+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		array_type = dynamic_cast<const DataTypeArray *>(&*arguments[1]);
		if (!array_type)
			throw Exception("Second argument for function " + getName() + " must be array. Found "
							+ arguments[1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		expression_type = dynamic_cast<const DataTypeExpression *>(&*arguments[0]);
		if (!expression_type || expression_type->getArgumentTypes().size() != 1)
			throw Exception("First argument for function " + getName() + " must be an expression with one argument. Found "
							+ arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	
	/// Вызывается, если хоть один агрумент функции - лямбда-выражение.
	/// Для аргументов-лямбда-выражений определяет типы аргументов этих выражений.
	void getLambdaArgumentTypes(DataTypes & arguments) const
	{
		const DataTypeArray * array_type;
		const DataTypeExpression * expression_type;
		checkTypes(arguments, expression_type, array_type);
		arguments[0] = new DataTypeExpression(DataTypes(1, array_type->getNestedType()));
	}

	void getReturnTypeAndPrerequisites(const ColumnsWithNameAndType & arguments,
										DataTypePtr & out_return_type,
										ExpressionActions::Actions & out_prerequisites)
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const ColumnExpression * column_expression = dynamic_cast<const ColumnExpression *>(arguments[0].column);
		const DataTypeArray * array_type = dynamic_cast<const DataTypeArray *>(&*arguments[1].type);
		
		if (!column_expression)
			throw Exception("First argument for function " + getName() + " must be an expression with one argument.",
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		if (!array_type)
			throw Exception("Second argument for function " + getName() + " must be array. Found "
							+ arguments[1].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		/// Попросим добавить в блок все столбцы, упоминаемые в выражении, размноженные в массив, параллельный обрабатываемому.
		ExpressionActions & expression = *column_expression->getExpression();
		Names required_columns = expression.getRequiredColumns();
		Names::iterator it = std::find(required_columns.begin(), required_columns.end(), column_expression->getArguments()[0].first);
		if (it != required_columns.end())
			required_columns.erase(it);
		for (size_t i = 0; i < required_columns.size(); ++i)
		{
			Names replicate_arguments;
			replicate_arguments.push_back(required_columns[i]);
			replicate_arguments.push_back(arguments[1].name);
			out_prerequisites.push_back(ExpressionActions::Action::applyFunction(new FunctionReplicate, replicate_arguments));
		}

		/// Если массив константный, попросим его материализовать.
		if (arguments[1].column)
		{
			out_prerequisites.push_back(ExpressionActions::Action::applyFunction(new FunctionMaterialize, Names(1, arguments[1].name)));
		}
		
		DataTypePtr return_type = column_expression->getReturnType();
		if (Impl::needBooleanExpression() && !dynamic_cast<const DataTypeUInt8 *>(&*return_type))
			throw Exception("Expression for function " + getName() + " must return UInt8, found "
							+ return_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return Impl::getReturnType(return_type, array_type->getNestedType());
	}
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
	{
		ColumnExpression * column_expression = dynamic_cast<ColumnExpression *>(&*block.getByPosition(arguments[0]).column);
		const ColumnArray * column_array = dynamic_cast<const ColumnArray *>(&*block.getByPosition(arguments[1]).column);
		
		/// Если это не ColumnArray, значит это константа, и мы просили ее материализовать.
		if (!column_array)
			column_array = dynamic_cast<const ColumnArray *>(&*block.getByPosition(prerequisites.back()).column);
		
		Block temp_block;
		ExpressionActions & expression = *column_expression->getExpression();
		String argument_name = column_expression->getArguments()[0].first;
		DataTypePtr element_type = column_expression->getArguments()[0].second;
		
		Names required_columns = expression.getRequiredColumns();
		Names::iterator it = std::find(required_columns.begin(), required_columns.end(), argument_name);
		if (it != required_columns.end())
			required_columns.erase(it);
		
		/// Положим в блок аргумент выражения.
		temp_block.insert(ColumnWithNameAndType(column_array->getDataPtr(), element_type, argument_name));
		
		/// Положим в блок все нужные столбцы, размноженные по размерам массивов.
		for (size_t i = 0; i < required_columns.size(); ++i)
		{
			const String & name = required_columns[i];
			ColumnWithNameAndType replicated_column = block.getByPosition(prerequisites[i]);
			
			const ColumnArray * col = dynamic_cast<const ColumnArray *>(&*replicated_column.column);
			const DataTypeArray * type = dynamic_cast<const DataTypeArray *>(&*replicated_column.type);
			if (!col || !type)
				throw Exception("Unexpected replicated column", ErrorCodes::LOGICAL_ERROR);
			
			replicated_column.name = name;
			replicated_column.column = col->getDataPtr();
			replicated_column.type = type->getNestedType();
			
			temp_block.insert(replicated_column);
		}
		
		expression.execute(temp_block);
		
		block.getByPosition(result).column = Impl::execute(column_array, temp_block.getByName(column_expression->getReturnName()).column);
	}
};


struct NameArrayMap		{ static const char * get() { return "arrayMap"; } };
struct NameArrayFilter	{ static const char * get() { return "arrayFilter"; } };
struct NameArrayCount	{ static const char * get() { return "arrayCount"; } };
struct NameArrayExists	{ static const char * get() { return "arrayExists"; } };

typedef FunctionArrayMapped<ArrayMapImpl, 		NameArrayMap>		FunctionArrayMap;
typedef FunctionArrayMapped<ArrayFilterImpl, 	NameArrayFilter>	FunctionArrayFilter;
typedef FunctionArrayMapped<ArrayCountImpl, 	NameArrayCount>		FunctionArrayCount;
typedef FunctionArrayMapped<ArrayExistsImpl, 	NameArrayExists>	FunctionArrayExists;
	
}
