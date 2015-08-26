#pragma once

#include <Poco/Net/DNS.h>
#include <Yandex/Revision.h>

#include <DB/Core/Defines.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Common/UnicodeBar.h>
#include <DB/Functions/IFunction.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <statdaemons/ext/range.hpp>

#include <cmath>

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
  * replicate(x, arr) - создаёт массив такого же размера как arr, все элементы которого равны x;
  * 					например: replicate(1, ['a', 'b', 'c']) = [1, 1, 1].
  *
  * sleep(n)		- спит n секунд каждый блок.
  *
  * bar(x, min, max, width) - рисует полосу из количества символов, пропорционального (x - min) и равного width при x == max.
  *
  * version()       - возвращает текущую версию сервера в строке.
  *
  * finalizeAggregation(agg_state) - по состоянию агрегации получить результат.
  *
  * runningAccumulate(agg_state) - принимает состояния агрегатной функции и возвращает столбец со значениями,
  *  являющимися результатом накопления этих состояний для множества строк блока, от первой до текущей строки.
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

class FunctionCurrentDatabase : public IFunction
{
	const String db_name;

public:
	static constexpr auto name = "currentDatabase";
	static IFunction * create(const Context & context) { return new FunctionCurrentDatabase{context.getCurrentDatabase()}; }

	explicit FunctionCurrentDatabase(const String & db_name) : db_name{db_name} {}

	String getName() const {
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeString;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		block.getByPosition(result).column = new ColumnConstString{
			block.rowsInFirstColumn(), db_name
		};
	}
};


/// Получить имя хоста. (Оно - константа, вычисляется один раз за весь запрос.)
class FunctionHostName : public IFunction
{
public:
	static constexpr auto name = "hostName";
	static IFunction * create(const Context & context) { return new FunctionHostName; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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

	/** Выполнить функцию над блоком. convertToFullColumn вызывается для того, чтобы в случае
	 *	распределенного выполнения запроса каждый сервер возвращал свое имя хоста. */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = ColumnConstString(
			block.rowsInFirstColumn(),
			Poco::Net::DNS::hostName()).convertToFullColumn();
	}
};


class FunctionVisibleWidth : public IFunction
{
public:
	static constexpr auto name = "visibleWidth";
	static IFunction * create(const Context & context) { return new FunctionVisibleWidth; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
	static constexpr auto name = "toTypeName";
	static IFunction * create(const Context & context) { return new FunctionToTypeName; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
	static constexpr auto name = "blockSize";
	static IFunction * create(const Context & context) { return new FunctionBlockSize; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
	static constexpr auto name = "sleep";
	static IFunction * create(const Context & context) { return new FunctionSleep; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeFloat64 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeFloat32 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt64 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt32 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt16 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt8 *>(&*arguments[0]))
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

		if (ColumnConst<Float64> * column = typeid_cast<ColumnConst<Float64> *>(col))
			seconds = column->getData();

		else if (ColumnConst<Float32> * column = typeid_cast<ColumnConst<Float32> *>(col))
			seconds = static_cast<double>(column->getData());

		else if (ColumnConst<UInt64> * column = typeid_cast<ColumnConst<UInt64> *>(col))
			seconds = static_cast<double>(column->getData());

		else if (ColumnConst<UInt32> * column = typeid_cast<ColumnConst<UInt32> *>(col))
			seconds = static_cast<double>(column->getData());

		else if (ColumnConst<UInt16> * column = typeid_cast<ColumnConst<UInt16> *>(col))
			seconds = static_cast<double>(column->getData());

		else if (ColumnConst<UInt8> * column = typeid_cast<ColumnConst<UInt8> *>(col))
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
	static constexpr auto name = "materialize";
	static IFunction * create(const Context & context) { return new FunctionMaterialize; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
			throw Exception("Argument for function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

		block.getByPosition(result).column = dynamic_cast<const IColumnConst &>(argument).convertToFullColumn();
	}
};

template <bool negative, bool global> struct FunctionInName;
template <> struct FunctionInName<false, false>	{ static constexpr auto name = "in"; };
template <> struct FunctionInName<false, true>	{ static constexpr auto name = "globalIn"; };
template <> struct FunctionInName<true, false>	{ static constexpr auto name = "notIn"; };
template <> struct FunctionInName<true, true>	{ static constexpr auto name = "globalNotIn"; };

template <bool negative, bool global>
class FunctionIn : public IFunction
{
public:
	static constexpr auto name = FunctionInName<negative, global>::name;
	static IFunction * create(const Context & context) { return new FunctionIn; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
		const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
		if (!column_set)
			throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
							ErrorCodes::ILLEGAL_COLUMN);

		/// Столбцы, которые проверяются на принадлежность множеству.
		ColumnNumbers left_arguments;

		/// Первый аргумент может быть tuple или одиночным столбцом.
		const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(&*block.getByPosition(arguments[0]).column);
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
	static constexpr auto name = "tuple";
	static IFunction * create(const Context & context) { return new FunctionTuple; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() < 2)
			throw Exception("Function " + getName() + " requires at least two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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
	static constexpr auto name = "tupleElement";
	static IFunction * create(const Context & context) { return new FunctionTupleElement; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	void getReturnTypeAndPrerequisites(const ColumnsWithTypeAndName & arguments,
										DataTypePtr & out_return_type,
										ExpressionActions::Actions & out_prerequisites)
	{
		if (arguments.size() != 2)
			throw Exception("Function " + getName() + " requires exactly two arguments: tuple and element index.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const ColumnConstUInt8 * index_col = typeid_cast<const ColumnConstUInt8 *>(&*arguments[1].column);
		if (!index_col)
			throw Exception("Second argument to " + getName() + " must be a constant UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		size_t index = index_col->getData();

		const DataTypeTuple * tuple = typeid_cast<const DataTypeTuple *>(&*arguments[0].type);
		if (!tuple)
			throw Exception("First argument for function " + getName() + " must be tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

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
		const ColumnTuple * tuple_col = typeid_cast<const ColumnTuple *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConstUInt8 * index_col = typeid_cast<const ColumnConstUInt8 *>(&*block.getByPosition(arguments[1]).column);

		if (!tuple_col)
			throw Exception("First argument for function " + getName() + " must be tuple.", ErrorCodes::ILLEGAL_COLUMN);

		if (!index_col)
			throw Exception("Second argument for function " + getName() + " must be UInt8 constant literal.", ErrorCodes::ILLEGAL_COLUMN);

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
	static constexpr auto name = "ignore";
	static IFunction * create(const Context & context) { return new FunctionIgnore; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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


class FunctionIdentity : public IFunction
{
public:
	static constexpr auto name = "identity";
	static IFunction * create(const Context & context) { return new FunctionIdentity; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Function " + getName() + " requires exactly one argument.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return arguments.front()->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
	}
};


class FunctionArrayJoin : public IFunction
{
public:
	static constexpr auto name = "arrayJoin";
	static IFunction * create(const Context & context) { return new FunctionArrayJoin; }


	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Function " + getName() + " requires exactly one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * arr = typeid_cast<const DataTypeArray *>(&*arguments[0]);
		if (!arr)
			throw Exception("Argument for function " + getName() + " must be Array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arr->getNestedType()->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		throw Exception("Function " + getName() + " must not be executed directly.", ErrorCodes::FUNCTION_IS_SPECIAL);
	}
};


/** Создаёт массив, размножая столбец (первый аргумент) по количеству элементов в массиве (втором аргументе).
  * Используется только в качестве prerequisites для функций высшего порядка.
  */
class FunctionReplicate : public IFunction
{
public:
	static constexpr auto name = "replicate";
	static IFunction * create(const Context & context) { return new FunctionReplicate; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
							+ toString(arguments.size()) + ", should be 2.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[1]);
		if (!array_type)
			throw Exception("Second argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(arguments[0]->clone());
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnPtr first_column = block.getByPosition(arguments[0]).column;

		ColumnArray * array_column = typeid_cast<ColumnArray *>(&*block.getByPosition(arguments[1]).column);
		ColumnPtr temp_column;

		if (!array_column)
		{
			ColumnConstArray * const_array_column = typeid_cast<ColumnConstArray *>(&*block.getByPosition(arguments[1]).column);
			if (!const_array_column)
				throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
			temp_column = const_array_column->convertToFullColumn();
			array_column = typeid_cast<ColumnArray *>(&*temp_column);
		}

		block.getByPosition(result).column = new ColumnArray(
			first_column->replicate(array_column->getOffsets()),
			array_column->getOffsetsColumn());
	}
};


class FunctionBar : public IFunction
{
public:
	static constexpr auto name = "bar";
	static IFunction * create(const Context & context) { return new FunctionBar; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 3 && arguments.size() != 4)
			throw Exception("Function " + getName() + " requires from 3 or 4 parameters: value, min_value, max_value, [max_width_of_bar = 80]. Passed "
				+ toString(arguments.size()) + ".",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!arguments[0]->isNumeric() || !arguments[1]->isNumeric() || !arguments[2]->isNumeric()
			|| (arguments.size() == 4 && !arguments[3]->isNumeric()))
			throw Exception("All arguments for function " + getName() + " must be numeric.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		Int64 min = extractConstant<Int64>(block, arguments, 1, "Second");	/// Уровень значения, при котором полоска имеет нулевую длину.
		Int64 max = extractConstant<Int64>(block, arguments, 2, "Third");	/// Уровень значения, при котором полоска имеет максимальную длину.

		/// Максимальная ширина полоски в символах, по-умолчанию.
		Float64 max_width = arguments.size() == 4
			? extractConstant<Float64>(block, arguments, 3, "Fourth")
			: 80;

		if (max_width < 1)
			throw Exception("Max_width argument must be >= 1.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		if (max_width > 1000)
			throw Exception("Too large max_width.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		const auto & src = *block.getByPosition(arguments[0]).column;

		if (src.isConst())
		{
			auto res_column = new ColumnConstString(block.rowsInFirstColumn(), "");
			block.getByPosition(result).column = res_column;

			if (   executeConstNumber<UInt8>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<UInt16>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<UInt32>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<UInt64>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<Int8>		(src, *res_column, min, max, max_width)
				|| executeConstNumber<Int16>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<Int32>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<Int64>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<Float32>	(src, *res_column, min, max, max_width)
				|| executeConstNumber<Float64>	(src, *res_column, min, max, max_width))
			{
			}
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else
		{
			auto res_column = new ColumnString;
			block.getByPosition(result).column = res_column;

			if (   executeNumber<UInt8>		(src, *res_column, min, max, max_width)
				|| executeNumber<UInt16>	(src, *res_column, min, max, max_width)
				|| executeNumber<UInt32>	(src, *res_column, min, max, max_width)
				|| executeNumber<UInt64>	(src, *res_column, min, max, max_width)
				|| executeNumber<Int8>		(src, *res_column, min, max, max_width)
				|| executeNumber<Int16>		(src, *res_column, min, max, max_width)
				|| executeNumber<Int32>		(src, *res_column, min, max, max_width)
				|| executeNumber<Int64>		(src, *res_column, min, max, max_width)
				|| executeNumber<Float32>	(src, *res_column, min, max, max_width)
				|| executeNumber<Float64>	(src, *res_column, min, max, max_width))
			{
			}
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
	}

private:
	template <typename T>
	T extractConstant(Block & block, const ColumnNumbers & arguments, size_t argument_pos, const char * which_argument) const
	{
		const auto & column = *block.getByPosition(arguments[argument_pos]).column;

		if (!column.isConst())
			throw Exception(which_argument + String(" argument for function ") + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

		return apply_visitor(FieldVisitorConvertToNumber<T>(), column[0]);
	}

	template <typename T>
	static void fill(const PODArray<T> & src, ColumnString::Chars_t & dst_chars, ColumnString::Offsets_t & dst_offsets,
		Int64 min, Int64 max, Float64 max_width)
	{
		size_t size = src.size();
		size_t current_offset = 0;

		dst_offsets.resize(size);
		dst_chars.reserve(size * (UnicodeBar::getWidthInBytes(max_width) + 1));	/// строки 0-terminated.

		for (size_t i = 0; i < size; ++i)
		{
			Float64 width = UnicodeBar::getWidth(src[i], min, max, max_width);
			size_t next_size = current_offset + UnicodeBar::getWidthInBytes(width) + 1;
			dst_chars.resize(next_size);
			UnicodeBar::render(width, reinterpret_cast<char *>(&dst_chars[current_offset]));
			current_offset = next_size;
			dst_offsets[i] = current_offset;
		}
	}

	template <typename T>
	static void fill(T src, String & dst_chars,
		Int64 min, Int64 max, Float64 max_width)
	{
		Float64 width = UnicodeBar::getWidth(src, min, max, max_width);
		dst_chars.resize(UnicodeBar::getWidthInBytes(width));
		UnicodeBar::render(width, &dst_chars[0]);
	}

	template <typename T>
	static bool executeNumber(const IColumn & src, ColumnString & dst, Int64 min, Int64 max, Float64 max_width)
	{
		if (const ColumnVector<T> * col = typeid_cast<const ColumnVector<T> *>(&src))
		{
			fill(col->getData(), dst.getChars(), dst.getOffsets(), min, max, max_width);
			return true;
		}
		else
			return false;
	}

	template <typename T>
	static bool executeConstNumber(const IColumn & src, ColumnConstString & dst, Int64 min, Int64 max, Float64 max_width)
	{
		if (const ColumnConst<T> * col = typeid_cast<const ColumnConst<T> *>(&src))
		{
			fill(col->getData(), dst.getData(), min, max, max_width);
			return true;
		}
		else
			return false;
	}
};


template <typename Impl>
class FunctionNumericPredicate : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static IFunction * create(const Context &) { return new FunctionNumericPredicate; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		const auto args_size = arguments.size();
		if (args_size != 1)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed " +
					toString(args_size) + ", should be 1",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		const auto arg = arguments.front().get();
		if (!typeid_cast<const DataTypeUInt8 *>(arg) &&
			!typeid_cast<const DataTypeUInt16 *>(arg) &&
			!typeid_cast<const DataTypeUInt32 *>(arg) &&
			!typeid_cast<const DataTypeUInt64 *>(arg) &&
			!typeid_cast<const DataTypeInt8 *>(arg) &&
			!typeid_cast<const DataTypeInt16 *>(arg) &&
			!typeid_cast<const DataTypeInt32 *>(arg) &&
			!typeid_cast<const DataTypeInt64 *>(arg) &&
			!typeid_cast<const DataTypeFloat32 *>(arg) &&
			!typeid_cast<const DataTypeFloat64 *>(arg))
			throw Exception{
				"Argument for function " + getName() + " must be numeric",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};

		return new DataTypeUInt8;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto in = block.getByPosition(arguments.front()).column.get();

		if (!execute<UInt8>(block, in, result) &&
			!execute<UInt16>(block, in, result) &&
			!execute<UInt32>(block, in, result) &&
			!execute<UInt64>(block, in, result) &&
			!execute<Int8>(block, in, result) &&
			!execute<Int16>(block, in, result) &&
			!execute<Int32>(block, in, result) &&
			!execute<Int64>(block, in, result) &&
			!execute<Float32>(block, in, result)  &&
			!execute<Float64>(block, in, result))
			throw Exception{
				"Illegal column " + in->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
	}

	template <typename T>
	bool execute(Block & block, const IColumn * in_untyped, const size_t result) override
	{
		if (const auto in = typeid_cast<const ColumnVector<T> *>(in_untyped))
		{
			const auto size = in->size();

			const auto out = new ColumnVector<UInt8>{size};
			block.getByPosition(result).column = out;

			const auto & in_data = in->getData();
			auto & out_data = out->getData();

			for (const auto i : ext::range(0, size))
				out_data[i] = Impl::execute(in_data[i]);

			return true;
		}
		else if (const auto in = typeid_cast<const ColumnConst<T> *>(in_untyped))
		{
			block.getByPosition(result).column = new ColumnConstUInt8{
				in->size(),
				Impl::execute(in->getData())
			};

			return true;
		}

		return false;
	}
};

struct IsFiniteImpl
{
	static constexpr auto name = "isFinite";
	template <typename T> static bool execute(const T t) { return std::isfinite(t); }
};

struct IsInfiniteImpl
{
	static constexpr auto name = "isInfinite";
	template <typename T> static bool execute(const T t) { return std::isinf(t); }
};

struct IsNaNImpl
{
	static constexpr auto name = "isNaN";
	template <typename T> static bool execute(const T t) { return std::isnan(t); }
};

using FunctionIsFinite = FunctionNumericPredicate<IsFiniteImpl>;
using FunctionIsInfinite = FunctionNumericPredicate<IsInfiniteImpl>;
using FunctionIsNaN = FunctionNumericPredicate<IsNaNImpl>;


class FunctionVersion : public IFunction
{
public:
	static constexpr auto name = "version";
	static IFunction * create(const Context & context) { return new FunctionVersion; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Function " + getName() + " must be called without arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		return new DataTypeString;
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		static const std::string version = getVersion();
		block.getByPosition(result).column = new ColumnConstString(version.length(), version);
	}

private:
	std::string getVersion() const
	{
		std::ostringstream os;
		os << DBMS_VERSION_MAJOR << "." << DBMS_VERSION_MINOR << "." << Revision::get();
		return os.str();
	}
};


/** Весьма необычная функция.
  * Принимает состояние агрегатной функции (например runningAccumulate(uniqState(UserID))),
  *  и для каждой строки блока, возвращает результат агрегатной функции по объединению состояний от всех предыдущих строк блока и текущей строки.
  *
  * То есть, функция зависит от разбиения данных на блоки и от порядка строк в блоке.
  */
class FunctionRunningAccumulate : public IFunction
{
public:
	static constexpr auto name = "runningAccumulate";
	static IFunction * create(const Context & context) { return new FunctionRunningAccumulate; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Function " + getName() + " requires exactly one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeAggregateFunction * type = typeid_cast<const DataTypeAggregateFunction *>(&*arguments[0]);
		if (!type)
			throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return type->getReturnType()->clone();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnAggregateFunction * column_with_states = typeid_cast<const ColumnAggregateFunction *>(&*block.getByPosition(arguments.at(0)).column);
		if (!column_with_states)
			throw Exception(
				"Illegal column " + block.getByPosition(arguments.at(0)).column->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		AggregateFunctionPtr aggregate_function_ptr = column_with_states->getAggregateFunction();
		const IAggregateFunction & agg_func = *aggregate_function_ptr;

		auto deleter = [&agg_func] (char * ptr) { agg_func.destroy(ptr); free(ptr); };
		std::unique_ptr<char, decltype(deleter)> place { reinterpret_cast<char *>(malloc(agg_func.sizeOfData())), deleter };

		agg_func.create(place.get());	/// Немного не exception-safe. Если здесь выкинется исключение, то зря вызовется destroy.

		ColumnPtr result_column_ptr = agg_func.getReturnType()->createColumn();
		block.getByPosition(result).column = result_column_ptr;
		IColumn & result_column = *result_column_ptr;
		result_column.reserve(column_with_states->size());

		const auto & states = column_with_states->getData();
		for (const auto & state_to_add : states)
		{
			agg_func.merge(place.get(), state_to_add);
			agg_func.insertResultInto(place.get(), result_column);
		}
	}
};


/** Принимает состояние агрегатной функции. Возвращает результат агрегации.
  */
class FunctionFinalizeAggregation : public IFunction
{
public:
	static constexpr auto name = "finalizeAggregation";
	static IFunction * create(const Context & context) { return new FunctionFinalizeAggregation; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Function " + getName() + " requires exactly one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeAggregateFunction * type = typeid_cast<const DataTypeAggregateFunction *>(&*arguments[0]);
		if (!type)
			throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return type->getReturnType()->clone();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		ColumnAggregateFunction * column_with_states = typeid_cast<ColumnAggregateFunction *>(&*block.getByPosition(arguments.at(0)).column);
		if (!column_with_states)
			throw Exception(
				"Illegal column " + block.getByPosition(arguments.at(0)).column->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		block.getByPosition(result).column = column_with_states->convertToValues();
	}
};

}
