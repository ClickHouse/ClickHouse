#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функция выбора по условию: if(cond, then, else).
  */


template <typename T>
struct NumIfImpl
{
	static void vector_vector(
		const std::vector<UInt8> & cond,
		const std::vector<T> & a, const std::vector<T> & b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a[i] : b[i];
	}

	static void vector_constant(
		const std::vector<UInt8> & cond,
		const std::vector<T> & a, T b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a[i] : b;
	}

	static void constant_vector(
		const std::vector<UInt8> & cond,
		T a, const std::vector<T> & b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a : b[i];
	}

	static void constant_constant(
		const std::vector<UInt8> & cond,
		T a, T b,
		std::vector<T> & res)
	{
		size_t size = cond.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? a : b;
	}
};


class FunctionIf : public IFunction
{
private:
	template <typename T>
	bool executeType(const ColumnVector<UInt8> * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnVector<T> * col_then_vec = dynamic_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[1]).column);
		ColumnVector<T> * col_else_vec = dynamic_cast<ColumnVector<T> *>(&*block.getByPosition(arguments[2]).column);
		ColumnConst<T> * col_then_const = dynamic_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[1]).column);
		ColumnConst<T> * col_else_const = dynamic_cast<ColumnConst<T> *>(&*block.getByPosition(arguments[2]).column);

		ColumnVector<T> * col_res = new ColumnVector<T>;
		block.getByPosition(result).column = col_res;

		typename ColumnVector<T>::Container_t & vec_res = col_res->getData();
		vec_res.resize(block.getByPosition(arguments[0]).column->size());

		if (col_then_vec && col_else_vec)
			NumIfImpl<T>::vector_vector(cond_col->getData(), col_then_vec->getData(), col_else_vec->getData(), vec_res);
		else if (col_then_vec && col_else_const)
			NumIfImpl<T>::vector_constant(cond_col->getData(), col_then_vec->getData(), col_else_const->getData(), vec_res);
		else if (col_then_const && col_else_vec)
			NumIfImpl<T>::constant_vector(cond_col->getData(), col_then_const->getData(), col_else_vec->getData(), vec_res);
		else if (col_then_const && col_else_const)
			NumIfImpl<T>::constant_constant(cond_col->getData(), col_then_const->getData(), col_else_const->getData(), vec_res);
		else
			return false;

		return true;
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return "if";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[0]))
			throw Exception("Illegal type of first argument (condition) of function if. Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments[1]->getName() != arguments[2]->getName())
			throw Exception("Second and third arguments for function " + getName() + " must have same type. Passed: "
				+ arguments[1]->getName() + " and " + arguments[2]->getName() + ".",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[1];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnVector<UInt8> * cond_col = dynamic_cast<const ColumnVector<UInt8> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConst<UInt8> * cond_const_col = dynamic_cast<const ColumnConst<UInt8> *>(&*block.getByPosition(arguments[0]).column);

		/// Если аргумент-условие - константа - то материализуем её.
		ColumnPtr materialized_const_holder;

		if (cond_const_col)
		{
			materialized_const_holder = cond_const_col->convertToFullColumn();
			cond_col = dynamic_cast<const ColumnVector<UInt8> *>(&*materialized_const_holder);
		}

		if (!cond_col)
			throw Exception("Illegal column " + cond_col->getName() + " of first argument of function " + getName() + ". Must be ColumnUInt8 or ColumnConstUInt8.",
				ErrorCodes::ILLEGAL_COLUMN);

		/// TODO: для строк.
		
		if (!(	executeType<UInt8>(cond_col, block, arguments, result)
			||	executeType<UInt16>(cond_col, block, arguments, result)
			||	executeType<UInt32>(cond_col, block, arguments, result)
			||	executeType<UInt64>(cond_col, block, arguments, result)
			||	executeType<Int8>(cond_col, block, arguments, result)
			||	executeType<Int16>(cond_col, block, arguments, result)
			||	executeType<Int32>(cond_col, block, arguments, result)
			||	executeType<Int64>(cond_col, block, arguments, result)
			||	executeType<Float32>(cond_col, block, arguments, result)
			||	executeType<Float64>(cond_col, block, arguments, result)))
		   throw Exception("Illegal columns " + block.getByPosition(arguments[1]).column->getName()
					+ " and " + block.getByPosition(arguments[2]).column->getName()
					+ " of second (then) and third (else) arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};

}
