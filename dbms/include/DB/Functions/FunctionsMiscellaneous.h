#pragma once

#include <Poco/Net/DNS.h>

#include <math.h>
#include <mutex>

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
#include <DB/Common/UnicodeBar.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Functions/IFunction.h>
#include <statdaemons/ext/range.hpp>


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
  *
  * bar(x, min, max, width) - рисует полосу из количества символов, пропорционального (x - min) и равного width при x == max.
  *
  * transform(x, from_array, to_array[, default]) - преобразовать x согласно переданному явным образом соответствию.
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

	void getReturnTypeAndPrerequisites(const ColumnsWithNameAndType & arguments,
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


/** Размножает столбец (первый аргумент) по количеству элементов в массиве (втором аргументе).
  * Не предназначена для внешнего использования.
  * Так как возвращаемый столбец будет иметь несовпадающий размер с исходными,
  *  то результат не может быть потом использован в том же блоке, что и аргументы.
  * Используется только в качестве prerequisites для функций высшего порядка.
  */
class FunctionReplicate : public IFunction
{
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

		return arguments[0]->clone();
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

		block.getByPosition(result).column = new ColumnReplicated(first_column->size(), first_column->replicate(array_column->getOffsets()));
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


DataTypePtr getSmallestCommonNumericType(const IDataType & t1, const IDataType & t2);

/** transform(x, [from...], [to...], default)
  * - преобразует значения согласно явно указанному отображению.
  *
  * x - что преобразовывать.
  * from - константный массив значений для преобразования.
  * to - константный массив значений, в которые должны быть преобразованы значения из from.
  * default - константа, какое значение использовать, если x не равен ни одному из значений во from.
  * from и to - массивы одинаковых размеров.
  *
  * Типы:
  * transform(T, Array(T), Array(U), U) -> U
  *
  * transform(x, [from...], [to...])
  * - eсли default не указан, то для значений x, для которых нет соответствующего элемента во from, возвращается не изменённое значение x.
  *
  * Типы:
  * transform(T, Array(T), Array(T)) -> T
  *
  * Замечание: реализация довольно громоздкая.
  */
class FunctionTransform : public IFunction
{
public:
	static constexpr auto name = "transform";
	static IFunction * create(const Context &) { return new FunctionTransform; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		const auto args_size = arguments.size();
		if (args_size != 3 && args_size != 4)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed " +
					toString(args_size) + ", should be 3 or 4",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

		const IDataType * type_x = arguments[0].get();

		if (!type_x->isNumeric() && !typeid_cast<const DataTypeString *>(type_x))
			throw Exception("Unsupported type " + type_x->getName()
				+ " of first argument of function " + getName()
				+ ", must be numeric type or Date/DateTime or String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const DataTypeArray * type_arr_from = typeid_cast<const DataTypeArray *>(arguments[1].get());

		if (!type_arr_from)
			throw Exception("Second argument of function " + getName()
				+ ", must be array of source values to transform from.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const auto type_arr_from_nested = type_arr_from->getNestedType();

		if ((type_x->isNumeric() != type_arr_from_nested->isNumeric())
			|| (!!typeid_cast<const DataTypeString *>(type_x) != !!typeid_cast<const DataTypeString *>(type_arr_from_nested.get())))
			throw Exception("First argument and elements of array of second argument of function " + getName()
				+ " must have compatible types: both numeric or both strings.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const DataTypeArray * type_arr_to = typeid_cast<const DataTypeArray *>(arguments[2].get());

		if (!type_arr_to)
			throw Exception("Third argument of function " + getName()
				+ ", must be array of destination values to transform to.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const auto type_arr_to_nested = type_arr_to->getNestedType();

		if (args_size == 3)
		{
			if ((type_x->isNumeric() != type_arr_to_nested->isNumeric())
				|| (!!typeid_cast<const DataTypeString *>(type_x) != !!typeid_cast<const DataTypeString *>(type_arr_to_nested.get())))
				throw Exception("Function " + getName()
					+ " have signature: transform(T, Array(T), Array(U), U) -> U; or transform(T, Array(T), Array(T)) -> T; where T and U are types.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			return type_x->clone();
		}
		else
		{
			const IDataType * type_default = arguments[3].get();

			if (!type_default->isNumeric() && !typeid_cast<const DataTypeString *>(type_default))
				throw Exception("Unsupported type " + type_default->getName()
					+ " of fourth argument (default value) of function " + getName()
					+ ", must be numeric type or Date/DateTime or String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			if ((type_default->isNumeric() != type_arr_to_nested->isNumeric())
				|| (!!typeid_cast<const DataTypeString *>(type_default) != !!typeid_cast<const DataTypeString *>(type_arr_to_nested.get())))
				throw Exception("Function " + getName()
					+ " have signature: transform(T, Array(T), Array(U), U) -> U; or transform(T, Array(T), Array(T)) -> T; where T and U are types.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			if (type_arr_to_nested->behavesAsNumber() && type_default->behavesAsNumber())
			{
				/// Берём наименьший общий тип для элементов массива значений to и для default-а.
				return getSmallestCommonNumericType(*type_arr_to_nested, *type_default);
			}

			/// TODO Больше проверок.
			return type_arr_to_nested->clone();
		}
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const ColumnConstArray * array_from = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[1]).column);
		const ColumnConstArray * array_to = typeid_cast<const ColumnConstArray *>(&*block.getByPosition(arguments[2]).column);

		if (!array_from && !array_to)
			throw Exception("Second and third arguments of function " + getName() + " must be constant arrays.", ErrorCodes::ILLEGAL_COLUMN);

		prepare(array_from->getData(), array_to->getData(), block, arguments);

		const auto in = block.getByPosition(arguments.front()).column.get();

		if (in->isConst())
		{
			executeConst(block, arguments, result);
			return;
		}

		auto column_result = block.getByPosition(result).type->createColumn();
		auto out = column_result.get();

		if (!executeNum<UInt8>(in, out)
			&& !executeNum<UInt16>(in, out)
			&& !executeNum<UInt32>(in, out)
			&& !executeNum<UInt64>(in, out)
			&& !executeNum<Int8>(in, out)
			&& !executeNum<Int16>(in, out)
			&& !executeNum<Int32>(in, out)
			&& !executeNum<Int64>(in, out)
			&& !executeNum<Float32>(in, out)
			&& !executeNum<Float64>(in, out)
			&& !executeString(in, out))
			throw Exception(
				"Illegal column " + in->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		block.getByPosition(result).column = column_result;
	}

private:
	void executeConst(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		/// Составим блок из полноценных столбцов размера 1 и вычислим функцию как обычно.

		Block tmp_block;
		ColumnNumbers tmp_arguments;

		tmp_block.insert(block.getByPosition(arguments[0]));
		tmp_block.getByPosition(0).column = static_cast<IColumnConst *>(tmp_block.getByPosition(0).column->cloneResized(1).get())->convertToFullColumn();
		tmp_arguments.push_back(0);

		for (size_t i = 1; i < arguments.size(); ++i)
		{
			tmp_block.insert(block.getByPosition(arguments[i]));
			tmp_arguments.push_back(i);
		}

		tmp_block.insert(block.getByPosition(result));
		size_t tmp_result = arguments.size();

		execute(tmp_block, tmp_arguments, tmp_result);

		block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
			block.rowsInFirstColumn(),
			(*tmp_block.getByPosition(tmp_result).column)[0]);
	}

	template <typename T>
	bool executeNum(const IColumn * in_untyped, IColumn * out_untyped)
	{
		if (const auto in = typeid_cast<const ColumnVector<T> *>(in_untyped))
		{
			if (default_value.isNull())
			{
				auto out = typeid_cast<ColumnVector<T> *>(out_untyped);
				if (!out)
					throw Exception(
						"Illegal column " + out_untyped->getName() + " of elements of array of third argument of function " + getName()
						+ ", must be " + in->getName(),
						ErrorCodes::ILLEGAL_COLUMN);

				executeImplNumToNum<T>(in->getData(), out->getData());
			}
			else
			{
				if (!executeNumToNumWithDefault<T, UInt8>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, UInt16>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, UInt32>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, UInt64>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, Int8>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, Int16>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, Int32>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, Int64>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, Float32>(in, out_untyped)
					&& !executeNumToNumWithDefault<T, Float64>(in, out_untyped)
					&& !executeNumToString<T>(in, out_untyped))
				throw Exception(
					"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
			}

			return true;
		}

		return false;
	}

	bool executeString(const IColumn * in_untyped, IColumn * out_untyped)
	{
		if (const auto in = typeid_cast<const ColumnString *>(in_untyped))
		{
			if (!executeStringToNum<UInt8>(in, out_untyped)
				&& !executeStringToNum<UInt16>(in, out_untyped)
				&& !executeStringToNum<UInt32>(in, out_untyped)
				&& !executeStringToNum<UInt64>(in, out_untyped)
				&& !executeStringToNum<Int8>(in, out_untyped)
				&& !executeStringToNum<Int16>(in, out_untyped)
				&& !executeStringToNum<Int32>(in, out_untyped)
				&& !executeStringToNum<Int64>(in, out_untyped)
				&& !executeStringToNum<Float32>(in, out_untyped)
				&& !executeStringToNum<Float64>(in, out_untyped)
				&& !executeStringToString(in, out_untyped))
			throw Exception(
				"Illegal column " + in->getName() + " of elements of array of second argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);

			return true;
		}

		return false;
	}

	template <typename T, typename U>
	bool executeNumToNumWithDefault(const ColumnVector<T> * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnVector<U> *>(out_untyped);
		if (!out)
			return false;

		executeImplNumToNumWithDefault<T, U>(in->getData(), out->getData(), default_value.get<U>());
		return true;
	}

	template <typename T>
	bool executeNumToString(const ColumnVector<T> * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		const String & default_str = default_value.get<const String &>();
		StringRef default_string_ref{default_str.data(), default_str.size() + 1};
		executeImplNumToStringWithDefault<T>(in->getData(), out->getChars(), out->getOffsets(), default_string_ref);
		return true;
	}

	template <typename U>
	bool executeStringToNum(const ColumnString * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnVector<U> *>(out_untyped);
		if (!out)
			return false;

		executeImplStringToNumWithDefault<U>(in->getChars(), in->getOffsets(), out->getData(), default_value.get<U>());
		return true;
	}

	bool executeStringToString(const ColumnString * in, IColumn * out_untyped)
	{
		auto out = typeid_cast<ColumnString *>(out_untyped);
		if (!out)
			return false;

		if (default_value.isNull())
			executeImplStringToString<false>(in->getChars(), in->getOffsets(), out->getChars(), out->getOffsets(), {});
		else
		{
			const String & default_str = default_value.get<const String &>();
			StringRef default_string_ref{default_str.data(), default_str.size() + 1};
			executeImplStringToString<true>(in->getChars(), in->getOffsets(), out->getChars(), out->getOffsets(), default_string_ref);
		}

		return true;
	}


	template <typename T, typename U>
	void executeImplNumToNumWithDefault(const PODArray<T> & src, PODArray<U> & dst, U dst_default)
	{
		const auto & table = *table_num_to_num;
		size_t size = src.size();
		dst.resize(size);
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));	/// little endian.
			else
				dst[i] = dst_default;
		}
	}

	template <typename T>
	void executeImplNumToNum(const PODArray<T> & src, PODArray<T> & dst)
	{
		const auto & table = *table_num_to_num;
		size_t size = src.size();
		dst.resize(size);
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));
			else
				dst[i] = src[i];
		}
	}

	template <typename T>
	void executeImplNumToStringWithDefault(const PODArray<T> & src,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets, StringRef dst_default)
	{
		const auto & table = *table_num_to_string;
		size_t size = src.size();
		dst_offsets.resize(size);
		ColumnString::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			auto it = table.find(src[i]);
			StringRef ref = it != table.end() ? it->second : dst_default;
			dst_data.resize(current_offset + ref.size);
			memcpy(&dst_data[current_offset], ref.data, ref.size);
			current_offset += ref.size;
			dst_offsets[i] = current_offset;
		}
	}

	template <typename U>
	void executeImplStringToNumWithDefault(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		PODArray<U> & dst, U dst_default)
	{
		const auto & table = *table_string_to_num;
		size_t size = src_offsets.size();
		dst.resize(size);
		ColumnString::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			StringRef ref{&src_data[current_offset], src_offsets[i] - current_offset};
			current_offset = src_offsets[i];
			auto it = table.find(ref);
			if (it != table.end())
				memcpy(&dst[i], &it->second, sizeof(dst[i]));
			else
				dst[i] = dst_default;
		}
	}

	template <bool with_default>
	void executeImplStringToString(
		const ColumnString::Chars_t & src_data, const ColumnString::Offsets_t & src_offsets,
		ColumnString::Chars_t & dst_data, ColumnString::Offsets_t & dst_offsets, StringRef dst_default)
	{
		const auto & table = *table_string_to_string;
		size_t size = src_offsets.size();
		dst_offsets.resize(size);
		ColumnString::Offset_t current_src_offset = 0;
		ColumnString::Offset_t current_dst_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			StringRef src_ref{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
			current_src_offset = src_offsets[i];

			auto it = table.find(src_ref);

			StringRef dst_ref = it != table.end() ? it->second : (with_default ? dst_default : src_ref);
			dst_data.resize(current_dst_offset + dst_ref.size);
			memcpy(&dst_data[current_dst_offset], dst_ref.data, dst_ref.size);
			current_dst_offset += dst_ref.size;
			dst_offsets[i] = current_dst_offset;
		}
	}


	/// Разные варианты хэш-таблиц для реализации отображения.

	using NumToNum = HashMap<UInt64, UInt64, HashCRC32<UInt64>>;
	using NumToString = HashMap<UInt64, StringRef, HashCRC32<UInt64>>;		/// Везде StringRef-ы с завершающим нулём.
	using StringToNum = HashMap<StringRef, UInt64>;
	using StringToString = HashMap<StringRef, StringRef>;

	std::unique_ptr<NumToNum> table_num_to_num;
	std::unique_ptr<NumToString> table_num_to_string;
	std::unique_ptr<StringToNum> table_string_to_num;
	std::unique_ptr<StringToString> table_string_to_string;

	Arena string_pool;

	Field default_value;	/// Null, если не задано.

	bool prepared = false;
	std::mutex mutex;

	/// Может вызываться из разных потоков. Срабатывает только при первом вызове.
	void prepare(const Array & from, const Array & to, Block & block, const ColumnNumbers & arguments)
	{
		if (prepared)
			return;

		const size_t size = from.size();
		if (0 == size)
			throw Exception("Empty arrays are illegal in function " + getName(), ErrorCodes::BAD_ARGUMENTS);

		std::lock_guard<std::mutex> lock(mutex);

		if (prepared)
			return;

		if (from.size() != to.size())
			throw Exception("Second and third arguments of function " + getName() + " must be arrays of same size", ErrorCodes::BAD_ARGUMENTS);

		Array converted_to;
		const Array * used_to = &to;

		/// Задано ли значение по-умолчанию.

		if (arguments.size() == 4)
		{
			const IColumnConst * default_col = dynamic_cast<const IColumnConst *>(&*block.getByPosition(arguments[3]).column);

			if (!default_col)
				throw Exception("Fourth argument of function " + getName() + " (default value) must be constant", ErrorCodes::ILLEGAL_COLUMN);

			default_value = (*default_col)[0];

			/// Нужно ли преобразовать элементы to и default_value к наименьшему общему типу, который является Float64?
			if (default_value.getType() == Field::Types::Float64 && to[0].getType() != Field::Types::Float64)
			{
				converted_to.resize(to.size());
				for (size_t i = 0, size = to.size(); i < size; ++i)
					converted_to[i] = apply_visitor(FieldVisitorConvertToNumber<Float64>(), to[i]);
				used_to = &converted_to;
			}
			else if (default_value.getType() != Field::Types::Float64 && to[0].getType() == Field::Types::Float64)
			{
				default_value = apply_visitor(FieldVisitorConvertToNumber<Float64>(), default_value);
			}
		}

		/// Замечание: не делается проверка дубликатов в массиве from.

		if (from[0].getType() != Field::Types::String && to[0].getType() != Field::Types::String)
		{
			table_num_to_num.reset(new NumToNum);
			auto & table = *table_num_to_num;
			for (size_t i = 0; i < size; ++i)
				table[from[i].get<UInt64>()] = (*used_to)[i].get<UInt64>();
		}
		else if (from[0].getType() != Field::Types::String && to[0].getType() == Field::Types::String)
		{
			table_num_to_string.reset(new NumToString);
			auto & table = *table_num_to_string;
			for (size_t i = 0; i < size; ++i)
			{
				const String & str_to = to[i].get<const String &>();
				StringRef ref{string_pool.insert(str_to.data(), str_to.size() + 1), str_to.size() + 1};
				table[from[i].get<UInt64>()] = ref;
			}
		}
		else if (from[0].getType() == Field::Types::String && to[0].getType() != Field::Types::String)
		{
			table_string_to_num.reset(new StringToNum);
			auto & table = *table_string_to_num;
			for (size_t i = 0; i < size; ++i)
			{
				const String & str_from = from[i].get<const String &>();
				StringRef ref{string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};
				table[ref] = (*used_to)[i].get<UInt64>();
			}
		}
		else if (from[0].getType() == Field::Types::String && to[0].getType() == Field::Types::String)
		{
			table_string_to_string.reset(new StringToString);
			auto & table = *table_string_to_string;
			for (size_t i = 0; i < size; ++i)
			{
				const String & str_from = from[i].get<const String &>();
				const String & str_to = to[i].get<const String &>();
				StringRef ref_from{string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};
				StringRef ref_to{string_pool.insert(str_to.data(), str_to.size() + 1), str_to.size() + 1};
				table[ref_from] = ref_to;
			}
		}

		prepared = true;
	}
};

}
