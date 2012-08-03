#pragma once

#include <math.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnVector.h>
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
		block.getByPosition(result).column = new ColumnConstUInt64(block.rows(), block.rows());
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

}
