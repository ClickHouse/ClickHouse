#pragma once

#include <Poco/Net/DNS.h>

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
#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/ObjectPool.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Set.h>
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


class FunctionCurrentDatabase : public IFunction
{
	const String db_name;

public:
	static constexpr auto name = "currentDatabase";
	static FunctionPtr create(const Context & context);

	explicit FunctionCurrentDatabase(const String & db_name) : db_name{db_name} {}

	String getName() const override { return name; }
	size_t getNumberOfArguments() const override { return 0; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override;
};


/// Get the host name. Is is constant on single server, but is not constant in distributed queries.
class FunctionHostName : public IFunction
{
public:
	static constexpr auto name = "hostName";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	bool isDeterministicInScopeOfQuery() override { return false; }

	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/** convertToFullColumn needed because in distributed query processing,
	  *	each server returns its own value.
	  */
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionVisibleWidth : public IFunction
{
public:
	static constexpr auto name = "visibleWidth";
	static FunctionPtr create(const Context & context);

	bool hasSpecialSupportForNulls() const override
	{
		return true;
	}

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/// Returns name of IDataType instance (name of data type).
class FunctionToTypeName : public IFunction
{
public:
	static constexpr auto name = "toTypeName";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	bool hasSpecialSupportForNulls() const override
	{
		return true;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/// Returns name of IColumn instance.
class FunctionToColumnTypeName : public IFunction
{
public:
	static constexpr auto name = "toColumnTypeName";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	bool hasSpecialSupportForNulls() const override
	{
		return true;
	}

	size_t getNumberOfArguments() const override { return 1; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionBlockSize : public IFunction
{
public:
	static constexpr auto name = "blockSize";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isDeterministicInScopeOfQuery() override { return false; }

	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionRowNumberInBlock : public IFunction
{
public:
	static constexpr auto name = "rowNumberInBlock";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 0; }

	bool isDeterministicInScopeOfQuery() override { return false; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Инкрементальный номер блока среди вызовов этой функции. */
class FunctionBlockNumber : public IFunction
{
private:
	std::atomic<size_t> block_number {0};

public:
	static constexpr auto name = "blockNumber";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 0; }

	bool isDeterministicInScopeOfQuery() override { return false; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Incremental number of row within all blocks passed to this function. */
class FunctionRowNumberInAllBlocks : public IFunction
{
private:
	std::atomic<size_t> rows {0};

public:
	static constexpr auto name = "rowNumberInAllBlocks";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 0; }

	bool isDeterministicInScopeOfQuery() override { return false; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionSleep : public IFunction
{
public:
	static constexpr auto name = "sleep";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Do not sleep during query analysis.
	bool isSuitableForConstantFolding() const override { return false; }

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionMaterialize : public IFunction
{
public:
	static constexpr auto name = "materialize";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override;

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
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
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 2; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionTuple : public IFunction
{
public:
	static constexpr auto name = "tuple";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }
	bool isInjective(const Block &) override { return true; }

	bool hasSpecialSupportForNulls() const override { return true; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Extract element of tuple by constant index. The operation is essentially free.
  */
class FunctionTupleElement : public IFunction
{
public:
	static constexpr auto name = "tupleElement";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 2; }

	void getReturnTypeAndPrerequisitesImpl(const ColumnsWithTypeAndName & arguments,
										DataTypePtr & out_return_type,
										ExpressionActions::Actions & out_prerequisites) override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionIgnore : public IFunction
{
public:
	static constexpr auto name = "ignore";
	static FunctionPtr create(const Context & context);

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	bool hasSpecialSupportForNulls() const override { return true; }

	String getName() const override { return name; }
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
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
	static FunctionPtr create(const Context & context);

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	bool hasSpecialSupportForNulls() const override { return true; }

	String getName() const override	{ return name; }
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionIdentity : public IFunction
{
public:
	static constexpr auto name = "identity";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionArrayJoin : public IFunction
{
public:
	static constexpr auto name = "arrayJoin";
	static FunctionPtr create(const Context & context);


	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/** It could return many different values for single argument. */
	bool isDeterministicInScopeOfQuery() override { return false; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

	/// Because of function cannot be executed directly.
	bool isSuitableForConstantFolding() const override { return false; }
};


/** Создаёт массив, размножая столбец (первый аргумент) по количеству элементов в массиве (втором аргументе).
  * Используется только в качестве prerequisites для функций высшего порядка.
  */
class FunctionReplicate : public IFunction
{
public:
	static constexpr auto name = "replicate";
	static FunctionPtr create(const Context & context);

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 2; }

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Returns a string with nice Unicode-art bar with resolution of 1/8 part of symbol.
  */
class FunctionBar : public IFunction
{
public:
	static constexpr auto name = "bar";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
	template <typename T>
	T extractConstant(Block & block, const ColumnNumbers & arguments, size_t argument_pos, const char * which_argument) const
	{
		const auto & column = *block.safeGetByPosition(arguments[argument_pos]).column;

		if (!column.isConst())
			throw Exception(which_argument + String(" argument for function ") + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

		return applyVisitor(FieldVisitorConvertToNumber<T>(), column[0]);
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

	size_t getNumberOfArguments() const override { return 1; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
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

	void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto in = block.safeGetByPosition(arguments.front()).column.get();

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
			block.safeGetByPosition(result).column = out;

			const auto & in_data = in->getData();
			auto & out_data = out->getData();

			for (const auto i : ext::range(0, size))
				out_data[i] = Impl::execute(in_data[i]);

			return true;
		}
		else if (const auto in = typeid_cast<const ColumnConst<T> *>(in_untyped))
		{
			block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt8>(
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
	static FunctionPtr create(const Context & context);

	String getName() const override { return name; }

	size_t getNumberOfArguments() const override { return 0; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
	std::string getVersion() const;
};


/** Returns server uptime in seconds.
  */
class FunctionUptime : public IFunction
{
public:
	static constexpr auto name = "uptime";
	static FunctionPtr create(const Context & context);

	FunctionUptime(time_t uptime_) : uptime(uptime_) {}

	String getName() const override { return name; }

	size_t getNumberOfArguments() const override { return 0; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
	time_t uptime;
};


/** Returns the server time zone.
  */
class FunctionTimeZone : public IFunction
{
public:
	static constexpr auto name = "timezone";
	static FunctionPtr create(const Context & context);

	String getName() const override { return name; }
	size_t getNumberOfArguments() const override { return 0; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
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
	static FunctionPtr create(const Context & context);

	String getName() const override { return name; }

	size_t getNumberOfArguments() const override { return 1; }

	bool isDeterministicInScopeOfQuery() override { return false; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Calculate difference of consecutive values in block.
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
class FunctionRunningDifference : public IFunction
{
private:
	/// It is possible to track value from previous block, to calculate continuously across all blocks. Not implemented.

	template <typename Src, typename Dst>
	static void process(const PaddedPODArray<Src> & src, PaddedPODArray<Dst> & dst)
	{
		size_t size = src.size();
		dst.resize(size);

		if (size == 0)
			return;

		/// It is possible to SIMD optimize this loop. By no need for that in practice.

		dst[0] = 0;
		Src prev = src[0];
		for (size_t i = 1; i < size; ++i)
		{
			auto cur = src[i];
			dst[i] = static_cast<Dst>(cur) - prev;
			prev = cur;
		}
	}

	/// Result type is same as result of subtraction of argument types.
	template <typename SrcFieldType>
	using DstFieldType = typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;

	/// Call polymorphic lambda with tag argument of concrete field type of src_type.
	template <typename F>
	void dispatchForSourceType(const IDataType & src_type, F && f) const
	{
			 if (typeid_cast<const DataTypeUInt8  *>(&src_type)) f(UInt8());
		else if (typeid_cast<const DataTypeUInt16 *>(&src_type)) f(UInt16());
		else if (typeid_cast<const DataTypeUInt32 *>(&src_type)) f(UInt32());
		else if (typeid_cast<const DataTypeUInt64 *>(&src_type)) f(UInt64());
		else if (typeid_cast<const DataTypeInt8 *>(&src_type)) f(Int8());
		else if (typeid_cast<const DataTypeInt16 *>(&src_type)) f(Int16());
		else if (typeid_cast<const DataTypeInt32 *>(&src_type)) f(Int32());
		else if (typeid_cast<const DataTypeInt64 *>(&src_type)) f(Int64());
		else if (typeid_cast<const DataTypeFloat32 *>(&src_type)) f(Float32());
		else if (typeid_cast<const DataTypeFloat64 *>(&src_type)) f(Float64());
		else if (typeid_cast<const DataTypeDate *>(&src_type)) f(DataTypeDate::FieldType());
		else if (typeid_cast<const DataTypeDateTime *>(&src_type)) f(DataTypeDateTime::FieldType());
		else
			throw Exception("Argument for function " + getName() + " must have numeric type.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

public:
	static constexpr auto name = "runningDifference";
	static FunctionPtr create(const Context & context);

	String getName() const override { return name; }

	size_t getNumberOfArguments() const override { return 1; }

	bool isDeterministicInScopeOfQuery() override { return false; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Takes state of aggregate function. Returns result of aggregation (finalized state).
  */
class FunctionFinalizeAggregation : public IFunction
{
public:
	static constexpr auto name = "finalizeAggregation";
	static FunctionPtr create(const Context & context);

	String getName() const override { return name; }

	size_t getNumberOfArguments() const override { return 1; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Usage:
 *  hasColumnInTable('database', 'table', 'column')
 */
class FunctionHasColumnInTable : public IFunction
{
public:
	static constexpr auto name = "hasColumnInTable";

	size_t getNumberOfArguments() const override { return 3; }

	static FunctionPtr create(const Context & context);

	FunctionHasColumnInTable(const Context & global_context_)
		: global_context(global_context_)
	{
	}

	String getName() const override { return name; }

	void getReturnTypeAndPrerequisitesImpl(const ColumnsWithTypeAndName & arguments,
		DataTypePtr & out_return_type,
		ExpressionActions::Actions & out_prerequisites) override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
	const Context & global_context;
};

}
