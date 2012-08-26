#pragma once

#include <math.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Вспомогательные функции:
  *
  * visibleWidth(x) - вычисляет приблизительную ширину при выводе значения в текстовом (tab-separated) виде на консоль.
  * 
  * toTypeName(x) 	- получить имя типа
  * blockSize()     - получить размер блока
  * materialize(x)  - материализовать константу
  * ignore(...)		- функция, принимающая любые аргументы, и всегда возвращающая 0.
  *
  * in(x, set)      - функция для вычисления оператора IN
  * notIn(x, set)   -  и NOT IN.
  *
  * tuple(x, y, ...) - функция, позволяющая сгруппировать несколько столбцов
  * tupleElement(tuple, n) - функция, позволяющая достать столбец из tuple.
  */


template <typename T>
static void numWidthVector(const std::vector<T> & a, std::vector<UInt64> & c)
{
	size_t size = a.size();
	for (size_t i = 0; i < size; ++i)
		if (a[i] >= 0)
			c[i] = a[i] ? 1 + log10(a[i]) : 1;
		else
			c[i] = 2 + log10(-a[i]);
}

template <typename T>
static void numWidthConstant(T a, UInt64 & c)
{
	if (a >= 0)
		c = a ? 1 + log10(a) : 1;
	else
		c = 2 + log10(-a);
}

inline UInt64 floatWidth(double x)
{
	/// Не быстро.
	unsigned size = WRITE_HELPERS_DEFAULT_FLOAT_PRECISION + 10;
	char tmp[size];	/// знаки, +0.0e+123\0
	int res = std::snprintf(tmp, size, "%.*g", WRITE_HELPERS_DEFAULT_FLOAT_PRECISION, x);

	if (res >= static_cast<int>(size) || res <= 0)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);
	
	return res;
}

template <typename T>
static void floatWidthVector(const std::vector<T> & a, std::vector<UInt64> & c)
{
	size_t size = a.size();
	for (size_t i = 0; i < size; ++i)
		c[i] = floatWidth(a[i]);
}

template <typename T>
static void floatWidthConstant(T a, UInt64 & c)
{
	c = floatWidth(a);
}

template <> inline void numWidthVector<Float64>(const std::vector<Float64> & a, std::vector<UInt64> & c) { floatWidthVector(a, c); }
template <> inline void numWidthVector<Float32>(const std::vector<Float32> & a, std::vector<UInt64> & c) { floatWidthVector(a, c); }
template <> inline void numWidthConstant<Float64>(Float64 a, UInt64 & c) { floatWidthConstant(a, c); }
template <> inline void numWidthConstant<Float32>(Float32 a, UInt64 & c) { floatWidthConstant(a, c); }

static inline UInt64 stringWidth(const UInt8 * pos, const UInt8 * end)
{
	UInt64 res = 0;
 	for (; pos < end; ++pos)
	{
		if (*pos == '\b' || *pos == '\f' || *pos == '\n' || *pos == '\r' || *pos == '\t' || *pos == '\0' || *pos == '\'' || *pos == '\\')
			++res;
		if (*pos <= 0x7F || *pos >= 0xC0)
			++res;
	}
	return res;
}

static inline void stringWidthVector(const std::vector<UInt8> & data, const std::vector<UInt64> & offsets, std::vector<UInt64> & res)
{
	size_t size = offsets.size();

	size_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		res[i] = stringWidth(&data[prev_offset], &data[offsets[i] - 1]);
		prev_offset = offsets[i];
	}
}

static inline void stringWidthFixedVector(const std::vector<UInt8> & data, size_t n, std::vector<UInt64> & res)
{
	size_t size = data.size() / n;
	for (size_t i = 0; i < size; ++i)
		res[i] = stringWidth(&data[i * n], &data[(i + 1) * n]);
}

inline void stringWidthConstant(const String & data, UInt64 & res)
{
	res = stringWidth(reinterpret_cast<const UInt8 *>(data.data()), reinterpret_cast<const UInt8 *>(data.data()) + data.size());
}


class FunctionVisibleWidth : public IFunction
{
private:
	template <typename T>
	bool executeConstNumber(Block & block, const ColumnPtr & column, size_t result)
	{
		if (const ColumnConst<T> * col = dynamic_cast<const ColumnConst<T> *>(&*column))
		{
			UInt64 res = 0;
			numWidthConstant(col->getData(), res);
			block.getByPosition(result).column = new ColumnConstUInt64(column->size(), res);
			return true;
		}
		else
			return false;
	}

	template <typename T>
	bool executeNumber(Block & block, const ColumnPtr & column, size_t result)
	{
		if (const ColumnVector<T> * col = dynamic_cast<const ColumnVector<T> *>(&*column))
		{
			ColumnUInt64 * res = new ColumnUInt64(column->size());
			numWidthVector(col->getData(), res->getData());
			block.getByPosition(result).column = res;
			return true;
		}
		else
			return false;
	}
	
public:
	/// Получить имя функции.
	String getName() const
	{
		return "visibleWidth";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;
		const DataTypePtr type = block.getByPosition(arguments[0]).type;
		size_t rows = column->size();

		if (dynamic_cast<const DataTypeDate *>(&*type))
		{
			block.getByPosition(result).column = new ColumnConstUInt64(rows, strlen("0000-00-00"));
		}
		else if (dynamic_cast<const DataTypeDateTime *>(&*type))
		{
			block.getByPosition(result).column = new ColumnConstUInt64(rows, strlen("0000-00-00 00:00:00"));
		}
		else if (executeConstNumber<UInt8>(block, column, result)
			|| executeConstNumber<UInt16>(block, column, result)
			|| executeConstNumber<UInt32>(block, column, result)
			|| executeConstNumber<UInt64>(block, column, result)
			|| executeConstNumber<Int8>(block, column, result)
			|| executeConstNumber<Int16>(block, column, result)
			|| executeConstNumber<Int32>(block, column, result)
			|| executeConstNumber<Int64>(block, column, result)
			|| executeConstNumber<Float32>(block, column, result)	/// TODO: правильная работа с float
			|| executeConstNumber<Float64>(block, column, result)
			|| executeNumber<UInt8>(block, column, result)
			|| executeNumber<UInt16>(block, column, result)
			|| executeNumber<UInt32>(block, column, result)
			|| executeNumber<UInt64>(block, column, result)
			|| executeNumber<Int8>(block, column, result)
			|| executeNumber<Int16>(block, column, result)
			|| executeNumber<Int32>(block, column, result)
			|| executeNumber<Int64>(block, column, result)
			|| executeNumber<Float32>(block, column, result)
			|| executeNumber<Float64>(block, column, result))
		{
		}
		else if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnUInt64 * res = new ColumnUInt64(rows);
			stringWidthVector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getOffsets(), res->getData());
			block.getByPosition(result).column = res;
		}
		else if (const ColumnFixedString * col = dynamic_cast<const ColumnFixedString *>(&*column))
		{
			ColumnUInt64 * res = new ColumnUInt64(rows);
			stringWidthFixedVector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getN(), res->getData());
			block.getByPosition(result).column = res;
		}
		else if (const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*column))
		{
			UInt64 res = 0;
			stringWidthConstant(col->getData(), res);
			block.getByPosition(result).column = new ColumnConstUInt64(rows, res);
		}
		else
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


class FunctionToTypeName : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "toTypeName";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstString(block.getByPosition(0).column->size(), block.getByPosition(arguments[0]).type->getName());
	}
};


class FunctionBlockSize : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "blockSize";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (!arguments.empty())
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		size_t size = block.getByPosition(0).column->size();
		block.getByPosition(result).column = ColumnConstUInt64(size, size).convertToFullColumn();
	}
};


class FunctionMaterialize : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "materialize";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return arguments[0];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn & argument = *block.getByPosition(arguments[0]).column;
		if (!argument.isConst())
			throw Exception("Argument for function 'materialize' must be constant.", ErrorCodes::ILLEGAL_COLUMN);
		
		block.getByPosition(result).column = dynamic_cast<const IColumnConst &>(argument).convertToFullColumn();
	}
};


class FunctionIn : public IFunction
{
private:
	bool negative;
	
public:
	FunctionIn(bool negative_ = false) : negative(negative_) {}
	
	/// Получить имя функции.
	String getName() const
	{
		return negative ? "notIn" : "in";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function '" + getName() + "' doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeUInt8;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Второй аргумент - обязательно ColumnSet.
		const ColumnSet * column_set = dynamic_cast<const ColumnSet *>(&*block.getByPosition(arguments[1]).column);
		if (!column_set)
			throw Exception("Second argument for function '" + getName() + "' must be Set.", ErrorCodes::ILLEGAL_COLUMN);

		/// Столбцы, которые проверяются на принадлежность множеству.
		ColumnNumbers left_arguments;

		/// Первый аргумент может быть tuple или одиночным столбцом.
		const ColumnTuple * tuple = dynamic_cast<const ColumnTuple *>(&*block.getByPosition(arguments[0]).column);
		if (tuple)
		{
			/// Находим в блоке столбцы из tuple.
			const Block & tuple_elems = tuple->getData();
			size_t tuple_size = tuple_elems.columns();
			for (size_t i = 0; i < tuple_size; ++i)
				left_arguments.push_back(block.getPositionByName(tuple_elems.getByPosition(i).name));
		}
		else
			left_arguments.push_back(arguments[0]);

		column_set->getData()->execute(block, left_arguments, result, negative);
	}
};


class FunctionTuple : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "tuple";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() < 2)
			throw Exception("Function tuple requires at least two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeTuple(arguments);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		Block tuple_block;

		for (ColumnNumbers::const_iterator it = arguments.begin(); it != arguments.end(); ++it)
			tuple_block.insert(block.getByPosition(*it));
		
		block.getByPosition(result).column = new ColumnTuple(tuple_block);
	}
};


class FunctionTupleElement : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "tupleElement";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		/** Эта функция особая. Тип результата зависит не только от типов аргументов, но и от значения константы (номера элемента tuple).
		  * Поэтому, тип результата здесь определить нельзя. Это делается, в функции ниже.
		  */
		throw Exception("Cannot get return type of function tupleElement.", ErrorCodes::CANNOT_GET_RETURN_TYPE);
	}

	DataTypePtr getReturnType(const DataTypes & arguments, size_t index) const
	{
		if (arguments.size() != 2)
			throw Exception("Function tupleElement requires exactly two arguments: tuple and element index.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeTuple * tuple = dynamic_cast<const DataTypeTuple *>(&*arguments[0]);
		if (!tuple)
			throw Exception("First argument for function tupleElement must be tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (index == 0)
			throw Exception("Indices in tuples is 1-based.", ErrorCodes::ILLEGAL_INDEX);
			
		const DataTypes & elems = tuple->getElements();

		if (index > elems.size())
			throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

		return elems[index - 1]->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnTuple * tuple_col = dynamic_cast<const ColumnTuple *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConstUInt8 * index_col = dynamic_cast<const ColumnConstUInt8 *>(&*block.getByPosition(arguments[1]).column);

		if (!tuple_col)
			throw Exception("First argument for function tupleElement must be tuple.", ErrorCodes::ILLEGAL_COLUMN);

		if (!index_col)
			throw Exception("Second argument for function tupleElement must be UInt8 constant literal.", ErrorCodes::ILLEGAL_COLUMN);

		size_t index = index_col->getData();
		if (index == 0)
			throw Exception("Indices in tuples is 1-based.", ErrorCodes::ILLEGAL_INDEX);

		const Block & tuple_block = tuple_col->getData();

		if (index > tuple_block.columns())
			throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

		block.getByPosition(result).column = tuple_block.getByPosition(index - 1).column;
	}
};


class FunctionIgnore : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "ignore";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		return new DataTypeUInt8;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstUInt8(block.getByPosition(0).column->size(), 0);
	}
};

}
