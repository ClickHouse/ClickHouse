#pragma once

#include <Poco/Net/DNS.h>
#include <common/ClickHouseRevision.h>

#include <DB/Core/Defines.h>
#include <DB/Core/FieldVisitors.h>
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
#include <ext/range.hpp>

#include <cmath>

namespace DB
{

namespace ErrorCodes
{
	extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
	extern const int ILLEGAL_INDEX;
	extern const int FUNCTION_IS_SPECIAL;
}

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
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionCurrentDatabase>(context.getCurrentDatabase()); }

	explicit FunctionCurrentDatabase(const String & db_name) : db_name{db_name} {}

	String getName() const override {
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstString>(
			block.rowsInFirstColumn(), db_name);
	}
};


/// Get the host name. Is is constant on single server, but is not constant in distributed queries.
class FunctionHostName : public IFunction
{
public:
	static constexpr auto name = "hostName";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionHostName>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeString>();
	}

	/** convertToFullColumn needed because in distributed query processing,
	  *	each server returns its own value.
	  */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionVisibleWidth>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt64>();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/// Returns name of IDataType instance (name of data type).
class FunctionToTypeName : public IFunction
{
public:
	static constexpr auto name = "toTypeName";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionToTypeName>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstString>(
			block.rowsInFirstColumn(), block.getByPosition(arguments[0]).type->getName());
	}
};


/// Returns name of IColumn instance.
class FunctionToColumnTypeName : public IFunction
{
public:
	static constexpr auto name = "toColumnTypeName";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionToColumnTypeName>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstString>(
			block.rowsInFirstColumn(), block.getByPosition(arguments[0]).column->getName());
	}
};


class FunctionBlockSize : public IFunction
{
public:
	static constexpr auto name = "blockSize";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBlockSize>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt64>();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		size_t size = block.rowsInFirstColumn();
		block.getByPosition(result).column = ColumnConstUInt64(size, size).convertToFullColumn();
	}
};


class FunctionRowNumberInBlock : public IFunction
{
public:
	static constexpr auto name = "rowNumberInBlock";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRowNumberInBlock>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt64>();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		size_t size = block.rowsInFirstColumn();
		auto column = std::make_shared<ColumnUInt64>();
		auto & data = column->getData();
		data.resize(size);
		for (size_t i = 0; i < size; ++i)
			data[i] = i;

		block.getByPosition(result).column = column;
	}
};


/** Инкрементальный номер блока среди вызовов этой функции. */
class FunctionBlockNumber : public IFunction
{
private:
	std::atomic<size_t> block_number {0};

public:
	static constexpr auto name = "blockNumber";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBlockNumber>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt64>();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		size_t current_block_number = block_number++;
		block.getByPosition(result).column = ColumnConstUInt64(block.rowsInFirstColumn(), current_block_number).convertToFullColumn();
	}
};


/** Incremental number of row within all blocks passed to this function. */
class FunctionRowNumberInAllBlocks : public IFunction
{
private:
	std::atomic<size_t> rows {0};

public:
	static constexpr auto name = "rowNumberInAllBlocks";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRowNumberInAllBlocks>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt64>();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		size_t rows_in_block = block.rowsInFirstColumn();
		size_t current_row_number = rows.fetch_add(rows_in_block);

		auto column = std::make_shared<ColumnUInt64>();
		auto & data = column->getData();
		data.resize(rows_in_block);
		for (size_t i = 0; i < rows_in_block; ++i)
			data[i] = current_row_number + i;

		block.getByPosition(result).column = column;
	}
};


class FunctionSleep : public IFunction
{
public:
	static constexpr auto name = "sleep";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionSleep>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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

		return std::make_shared<DataTypeUInt8>();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		IColumn * col = block.getByPosition(arguments[0]).column.get();
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

		/// convertToFullColumn needed, because otherwise (constant expression case) function will not get called on each block.
		block.getByPosition(result).column = ColumnConst<UInt8>(size, 0).convertToFullColumn();
	}
};


class FunctionMaterialize : public IFunction
{
public:
	static constexpr auto name = "materialize";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionMaterialize>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return arguments[0];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const auto & src = block.getByPosition(arguments[0]).column;
		if (auto converted = src->convertToFullColumnIfConst())
			block.getByPosition(result).column = converted;
		else
			block.getByPosition(result).column = src;
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
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIn>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function '" + getName() + "' doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt8>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		/// Second argument must be ColumnSet.
		ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
		const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
		if (!column_set)
			throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		Block block_of_key_columns;

		/// First argument may be tuple or single column.
		const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(block.getByPosition(arguments[0]).column.get());
		const ColumnConstTuple * const_tuple = typeid_cast<const ColumnConstTuple *>(block.getByPosition(arguments[0]).column.get());

		if (tuple)
			block_of_key_columns = tuple->getData();
		else if (const_tuple)
			block_of_key_columns = static_cast<const ColumnTuple &>(*const_tuple->convertToFullColumn()).getData();
		else
			block_of_key_columns.insert(block.getByPosition(arguments[0]));

		block.getByPosition(result).column = column_set->getData()->execute(block_of_key_columns, negative);
	}
};


class FunctionTuple : public IFunction
{
public:
	static constexpr auto name = "tuple";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTuple>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() < 1)
			throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeTuple>(arguments);
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		Block tuple_block;

		size_t num_constants = 0;
		for (auto column_number : arguments)
		{
			const auto & elem = block.getByPosition(column_number);
			if (elem.column->isConst())
				++num_constants;

			tuple_block.insert(elem);
		}

		if (num_constants == arguments.size())
		{
			/** Return ColumnConstTuple rather than ColumnTuple of nested const columns.
			  * (otherwise, ColumnTuple will not be understanded as constant in many places in code).
			  */

			TupleBackend tuple(arguments.size());
			for (size_t i = 0, size = arguments.size(); i < size; ++i)
				tuple_block.unsafeGetByPosition(i).column->get(0, tuple[i]);

			block.getByPosition(result).column = std::make_shared<ColumnConstTuple>(
				block.rowsInFirstColumn(), Tuple(tuple), block.getByPosition(result).type);
		}
		else
		{
			ColumnPtr res = std::make_shared<ColumnTuple>(tuple_block);

			/** If tuple is mixed of constant and not constant columns,
			  *  convert all to non-constant columns,
			  *  because many places in code expect all non-constant columns in non-constant tuple.
			  */
			if (num_constants != 0)
				if (auto converted = res->convertToFullColumnIfConst())
					res = converted;

			block.getByPosition(result).column = res;
		}
	}
};


/** Extract element of tuple by constant index. The operation is essentially free.
  */
class FunctionTupleElement : public IFunction
{
public:
	static constexpr auto name = "tupleElement";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTupleElement>(); }

	String getName() const override
	{
		return name;
	}

	void getReturnTypeAndPrerequisites(const ColumnsWithTypeAndName & arguments,
										DataTypePtr & out_return_type,
										ExpressionActions::Actions & out_prerequisites) override
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

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnTuple * tuple_col = typeid_cast<const ColumnTuple *>(block.getByPosition(arguments[0]).column.get());
		const ColumnConstTuple * const_tuple_col = typeid_cast<const ColumnConstTuple *>(block.getByPosition(arguments[0]).column.get());
		const ColumnConstUInt8 * index_col = typeid_cast<const ColumnConstUInt8 *>(block.getByPosition(arguments[1]).column.get());

		if (!tuple_col && !const_tuple_col)
			throw Exception("First argument for function " + getName() + " must be tuple.", ErrorCodes::ILLEGAL_COLUMN);

		if (!index_col)
			throw Exception("Second argument for function " + getName() + " must be UInt8 constant literal.", ErrorCodes::ILLEGAL_COLUMN);

		size_t index = index_col->getData();
		if (index == 0)
			throw Exception("Indices in tuples is 1-based.", ErrorCodes::ILLEGAL_INDEX);

		if (tuple_col)
		{
			const Block & tuple_block = tuple_col->getData();

			if (index > tuple_block.columns())
				throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

			block.getByPosition(result).column = tuple_block.getByPosition(index - 1).column;
		}
		else
		{
			const TupleBackend & data = const_tuple_col->getData();
			block.getByPosition(result).column = static_cast<const DataTypeTuple &>(*block.getByPosition(arguments[0]).type)
				.getElements()[index - 1]->createConstColumn(block.rowsInFirstColumn(), data[index - 1]);
		}
	}
};


class FunctionIgnore : public IFunction
{
public:
	static constexpr auto name = "ignore";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIgnore>(); }

	String getName() const override { return name; }
	DataTypePtr getReturnType(const DataTypes & arguments) const override { return std::make_shared<DataTypeUInt8>(); }

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(block.rowsInFirstColumn(), 0);
	}
};


/** Функция indexHint принимает любое количество любых аргументов и всегда возвращает единицу.
  *
  * Эта функция имеет особый смысл (см. ExpressionAnalyzer, PKCondition):
  * - расположенные внутри неё выражения не вычисляются;
  * - но при анализе индекса (выбора диапазонов для чтения), эта функция воспринимается так же,
  *   как если бы вместо её применения было бы само выражение.
  *
  * Пример: WHERE something AND indexHint(CounterID = 34)
  * - не читать и не вычислять CounterID = 34, но выбрать диапазоны, в которых выражение CounterID = 34 может быть истинным.
  *
  * Функция может использоваться в отладочных целях, а также для (скрытых от пользователя) преобразований запроса.
  */
class FunctionIndexHint : public IFunction
{
public:
	static constexpr auto name = "indexHint";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIndexHint>(); }

	String getName() const override	{ return name; }
	DataTypePtr getReturnType(const DataTypes & arguments) const override { return std::make_shared<DataTypeUInt8>(); }

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(block.rowsInFirstColumn(), 1);
	}
};


class FunctionIdentity : public IFunction
{
public:
	static constexpr auto name = "identity";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIdentity>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Function " + getName() + " requires exactly one argument.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return arguments.front()->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
	}
};


class FunctionArrayJoin : public IFunction
{
public:
	static constexpr auto name = "arrayJoin";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayJoin>(); }


	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Function " + getName() + " requires exactly one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * arr = typeid_cast<const DataTypeArray *>(&*arguments[0]);
		if (!arr)
			throw Exception("Argument for function " + getName() + " must be Array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arr->getNestedType()->clone();
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionReplicate>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
							+ toString(arguments.size()) + ", should be 2.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*arguments[1]);
		if (!array_type)
			throw Exception("Second argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeArray>(arguments[0]->clone());
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		ColumnPtr first_column = block.getByPosition(arguments[0]).column;

		ColumnArray * array_column = typeid_cast<ColumnArray *>(block.getByPosition(arguments[1]).column.get());
		ColumnPtr temp_column;

		if (!array_column)
		{
			ColumnConstArray * const_array_column = typeid_cast<ColumnConstArray *>(block.getByPosition(arguments[1]).column.get());
			if (!const_array_column)
				throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
			temp_column = const_array_column->convertToFullColumn();
			array_column = typeid_cast<ColumnArray *>(&*temp_column);
		}

		block.getByPosition(result).column = std::make_shared<ColumnArray>(
			first_column->replicate(array_column->getOffsets()),
			array_column->getOffsetsColumn());
	}
};


/** Returns a string with nice Unicode-art bar with resolution of 1/8 part of symbol.
  */
class FunctionBar : public IFunction
{
public:
	static constexpr auto name = "bar";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBar>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 3 && arguments.size() != 4)
			throw Exception("Function " + getName() + " requires from 3 or 4 parameters: value, min_value, max_value, [max_width_of_bar = 80]. Passed "
				+ toString(arguments.size()) + ".",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!arguments[0]->isNumeric() || !arguments[1]->isNumeric() || !arguments[2]->isNumeric()
			|| (arguments.size() == 4 && !arguments[3]->isNumeric()))
			throw Exception("All arguments for function " + getName() + " must be numeric.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
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
			auto res_column = std::make_shared<ColumnConstString>(block.rowsInFirstColumn(), "");
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
			auto res_column = std::make_shared<ColumnString>();
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
	static void fill(const PaddedPODArray<T> & src, ColumnString::Chars_t & dst_chars, ColumnString::Offsets_t & dst_offsets,
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
	static FunctionPtr create(const Context &) { return std::make_shared<FunctionNumericPredicate>(); }

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

		return std::make_shared<DataTypeUInt8>();
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
	bool execute(Block & block, const IColumn * in_untyped, const size_t result)
	{
		if (const auto in = typeid_cast<const ColumnVector<T> *>(in_untyped))
		{
			const auto size = in->size();

			const auto out = std::make_shared<ColumnUInt8>(size);
			block.getByPosition(result).column = out;

			const auto & in_data = in->getData();
			auto & out_data = out->getData();

			for (const auto i : ext::range(0, size))
				out_data[i] = Impl::execute(in_data[i]);

			return true;
		}
		else if (const auto in = typeid_cast<const ColumnConst<T> *>(in_untyped))
		{
			block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(
				in->size(),
				Impl::execute(in->getData()));

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


/** Returns server version (constant).
  */
class FunctionVersion : public IFunction
{
public:
	static constexpr auto name = "version";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionVersion>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Function " + getName() + " must be called without arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		static const std::string version = getVersion();
		block.getByPosition(result).column = std::make_shared<ColumnConstString>(block.rowsInFirstColumn(), version);
	}

private:
	std::string getVersion() const
	{
		std::ostringstream os;
		os << DBMS_VERSION_MAJOR << "." << DBMS_VERSION_MINOR << "." << ClickHouseRevision::get();
		return os.str();
	}
};


/** Returns server uptime in seconds.
  */
class FunctionUptime : public IFunction
{
public:
	static constexpr auto name = "uptime";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionUptime>(context.getUptimeSeconds()); }

	FunctionUptime(time_t uptime_) : uptime(uptime_) {}

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (!arguments.empty())
			throw Exception("Function " + getName() + " must be called without arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		return std::make_shared<DataTypeUInt32>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt32>(block.rowsInFirstColumn(), uptime);
	}

private:
	time_t uptime;
};


/** Quite unusual function.
  * Takes state of aggregate function (example runningAccumulate(uniqState(UserID))),
  *  and for each row of block, return result of aggregate function on merge of states of all previous rows and current row.
  *
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
class FunctionRunningAccumulate : public IFunction
{
public:
	static constexpr auto name = "runningAccumulate";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRunningAccumulate>(); }

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


/** Takes state of aggregate function. Returns result of aggregation (finalized state).
  */
class FunctionFinalizeAggregation : public IFunction
{
public:
	static constexpr auto name = "finalizeAggregation";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionFinalizeAggregation>(); }

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
