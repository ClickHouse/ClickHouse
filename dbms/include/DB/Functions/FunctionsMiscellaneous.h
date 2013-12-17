#pragma once

#include <Poco/Net/DNS.h>

#include <math.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnReplicated.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Вспомогательные функции:
  *
  * visibleWidth(x)	- вычисляет приблизительную ширину при выводе значения в текстовом (tab-separated) виде на консоль.
  * 
  * toTypeName(x)	- получить имя типа
  * blockSize()		- получить размер блока
  * materialize(x)	- материализовать константу
  * ignore(...)		- функция, принимающая любые аргументы, и всегда возвращающая 0.
  * sleep(seconds)	- спит указанное количество секунд каждый блок.
  * 
  * in(x, set)		- функция для вычисления оператора IN
  * notIn(x, set)	-  и NOT IN.
  *
  * tuple(x, y, ...) - функция, позволяющая сгруппировать несколько столбцов
  * tupleElement(tuple, n) - функция, позволяющая достать столбец из tuple.
  *
  * arrayJoin(arr)	- особая функция - выполнить её напрямую нельзя;
  *                   используется только чтобы получить тип результата соответствующего выражения.
  *
  * replicate(x, arr) - копирует x столько раз, сколько элементов в массиве arr;
  * 					например: replicate(1, ['a', 'b', 'c']) = 1, 1, 1.
  *                     не предназначена для пользователя, а используется только как prerequisites для функций высшего порядка.
  *
  * sleep(n)		- спит n секунд каждый блок.
  */


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

static inline void stringWidthConstant(const String & data, UInt64 & res)
{
	res = stringWidth(reinterpret_cast<const UInt8 *>(data.data()), reinterpret_cast<const UInt8 *>(data.data()) + data.size());
}

/// Получить имя хоста. (Оно - константа, вычисляется один раз за весь запрос.)
class FunctionHostName : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "hostName";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstString(
			block.rowsInFirstColumn(),
			Poco::Net::DNS::hostName());
	}
};

class FunctionVisibleWidth : public IFunction
{
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
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result);
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
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstString(block.rowsInFirstColumn(), block.getByPosition(arguments[0]).type->getName());
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
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		size_t size = block.rowsInFirstColumn();
		block.getByPosition(result).column = ColumnConstUInt64(size, size).convertToFullColumn();
	}
};


class FunctionSleep : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "sleep";
	}
	
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		if (!dynamic_cast<const DataTypeFloat64 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeFloat32 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt64 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt32 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt16 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt8 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected Float64",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return new DataTypeUInt8;
	}
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IColumn * col = &*block.getByPosition(arguments[0]).column;
		double seconds;
		size_t size = col->size();
		
		if (ColumnConst<Float64> * column = dynamic_cast<ColumnConst<Float64> *>(col))
			seconds = column->getData();
		
		else if (ColumnConst<Float32> * column = dynamic_cast<ColumnConst<Float32> *>(col))
			seconds = static_cast<double>(column->getData());
		
		else if (ColumnConst<UInt64> * column = dynamic_cast<ColumnConst<UInt64> *>(col))
			seconds = static_cast<double>(column->getData());
		
		else if (ColumnConst<UInt32> * column = dynamic_cast<ColumnConst<UInt32> *>(col))
			seconds = static_cast<double>(column->getData());
		
		else if (ColumnConst<UInt16> * column = dynamic_cast<ColumnConst<UInt16> *>(col))
			seconds = static_cast<double>(column->getData());
		
		else if (ColumnConst<UInt8> * column = dynamic_cast<ColumnConst<UInt8> *>(col))
			seconds = static_cast<double>(column->getData());
		
		else
			throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);
		
		/// Не спим, если блок пустой.
		if (size > 0)
			usleep(static_cast<unsigned>(seconds * 1e6));
		
		block.getByPosition(result).column = ColumnConst<UInt8>(size, 0).convertToFullColumn();
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
				+ toString(arguments.size()) + ", should be 1.",
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
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeUInt8;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Второй аргумент - обязательно ColumnSet.
		ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
		const ColumnSet * column_set = dynamic_cast<const ColumnSet *>(&*column_set_ptr);
		if (!column_set)
			throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
							ErrorCodes::ILLEGAL_COLUMN);

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

	void getReturnTypeAndPrerequisites(const ColumnsWithNameAndType & arguments,
										DataTypePtr & out_return_type,
										ExpressionActions::Actions & out_prerequisites)
	{
		if (arguments.size() != 2)
			throw Exception("Function tupleElement requires exactly two arguments: tuple and element index.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		const ColumnConstUInt8 * index_col = dynamic_cast<const ColumnConstUInt8 *>(&*arguments[1].column);
		if (!index_col)
			throw Exception("Second argument to tupleElement must be a constant UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		size_t index = index_col->getData();
		
		const DataTypeTuple * tuple = dynamic_cast<const DataTypeTuple *>(&*arguments[0].type);
		if (!tuple)
			throw Exception("First argument for function tupleElement must be tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		if (index == 0)
			throw Exception("Indices in tuples are 1-based.", ErrorCodes::ILLEGAL_INDEX);
		
		const DataTypes & elems = tuple->getElements();
		
		if (index > elems.size())
			throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);
		
		out_return_type = elems[index - 1]->clone();
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
		block.getByPosition(result).column = new ColumnConstUInt8(block.rowsInFirstColumn(), 0);
	}
};


class FunctionArrayJoin : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "arrayJoin";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Function arrayJoin requires exactly one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * arr = dynamic_cast<const DataTypeArray *>(&*arguments[0]);
		if (!arr)
			throw Exception("Argument for function arrayJoin must be Array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return arr->getNestedType()->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		throw Exception("Function arrayJoin must not be executed directly.", ErrorCodes::FUNCTION_IS_SPECIAL);
	}
};


/** Размножает столбец (первый аргумент) по количеству элементов в массиве (втором аргументе).
  * Не предназначена для внешнего использования.
  * Так как возвращаемый столбец будет иметь несовпадающий размер с исходными,
  *  то результат не может быть потом использован в том же блоке, что и аргументы.
  * Используется только в качестве prerequisites для функций высшего порядка.
  */
class FunctionReplicate : public IFunction
{
	/// Получить имя функции.
	String getName() const
	{
		return "replicate";
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
							+ toString(arguments.size()) + ", should be 2.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = dynamic_cast<const DataTypeArray *>(&*arguments[1]);
		if (!array_type)
			throw Exception("Second argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[0]->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnPtr first_column = block.getByPosition(arguments[0]).column;

		ColumnArray * array_column = dynamic_cast<ColumnArray *>(&*block.getByPosition(arguments[1]).column);
		ColumnPtr temp_column;

		if (!array_column)
		{
			ColumnConstArray * const_array_column = dynamic_cast<ColumnConstArray *>(&*block.getByPosition(arguments[1]).column);
			if (!const_array_column)
				throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
			temp_column = const_array_column->convertToFullColumn();
			array_column = dynamic_cast<ColumnArray *>(&*temp_column);
		}

		block.getByPosition(result).column = new ColumnReplicated(first_column->size(), first_column->replicate(array_column->getOffsets()));
	}
};

}
