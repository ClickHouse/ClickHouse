#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/Functions/IFunction.h>

#include <type_traits>

namespace DB
{

/** Функции работы с датой и временем.
  *
  * toYear, toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond,
  * toMonday, toStartOfMonth, toStartOfYear, toStartOfMinute, toStartOfFiveMinute
  * toStartOfHour, toTime,
  * now
  * TODO: makeDate, makeDateTime
  *
  * (toDate - расположена в файле FunctionsConversion.h)
  *
  * Возвращаемые типы:
  *  toYear -> UInt16
  *  toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond -> UInt8
  *  toMonday, toStartOfMonth, toStartOfYear -> Date
  *  toStartOfMinute, toStartOfHour, toTime, now -> DateTime
  *
  * А также:
  *
  * timeSlot(EventTime)
  * - округляет время до получаса.
  *
  * timeSlots(StartTime, Duration)
  * - для интервала времени, начинающегося в StartTime и продолжающегося Duration секунд,
  *   возвращает массив моментов времени, состоящий из округлений вниз до получаса точек из этого интервала.
  *  Например, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  Это нужно для поиска хитов, входящих в соответствующий визит.
  */


#define TIME_SLOT_SIZE 1800

/** Всевозможные преобразования.
  * Представляют собой две функции - от даты-с-временем (UInt32) и от даты (UInt16).
  *
  * Также для преобразования T определяется "фактор-преобразование" F.
  * Это такое преобразование F, что его значение идентифицирует область монотонности T
  *  (при фиксированном значении F, преобразование T является монотонным).
  *
  * Или, образно, если T аналогично взятию остатка от деления, то F аналогично делению.
  *
  * Пример: для преобразования T "получить номер дня в месяце" (2015-02-03 -> 3),
  *  фактор-преобразованием F является "округлить до месяца" (2015-02-03 -> 2015-02-01).
  */

/// Это фактор-преобразование будет говорить, что функция монотонна всюду.
struct ZeroTransform
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone) { return 0; }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone) { return 0; }
};

struct ToDateImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return UInt16(time_zone.toDayNum(t));
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return d;
	}

	using FactorTransform = ZeroTransform;
};

struct ToStartOfDayImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toDate(t);
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return d;
	}

	using FactorTransform = ZeroTransform;
};

struct ToMondayImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfWeek(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToStartOfMonthImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfMonth(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToStartOfQuarterImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfQuarter(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToStartOfYearImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toFirstDayNumOfYear(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};


struct ToTimeImpl
{
	/// При переводе во время, дату будем приравнивать к 1970-01-02.
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toTime(t) + 86400;
	}

	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ToDateImpl;
};

struct ToStartOfMinuteImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toStartOfMinuteInaccurate(t);
	}
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toStartOfMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinuteImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toStartOfFiveMinuteInaccurate(t);
	}
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toStartOfFiveMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toStartOfHourInaccurate(t);
	}
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ZeroTransform;
};

struct ToYearImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toYear(t);
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toYear(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToMonthImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toMonth(t);
	}
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toMonth(DayNum_t(d));
	}

	using FactorTransform = ToStartOfYearImpl;
};

struct ToDayOfMonthImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toDayOfMonth(t);
	}
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toDayOfMonth(DayNum_t(d));
	}

	using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toDayOfWeek(t);
	}
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toDayOfWeek(DayNum_t(d));
	}

	using FactorTransform = ToMondayImpl;
};

struct ToHourImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toHour(t);
	}
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ToDateImpl;
};

struct ToMinuteImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toMinuteInaccurate(t);
	}
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ToStartOfHourImpl;
};

struct ToSecondImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toSecondInaccurate(t);
	}
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ToStartOfMinuteImpl;
};

struct ToRelativeYearNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toYear(t);
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toYear(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToRelativeMonthNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toRelativeMonthNum(t);
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toRelativeMonthNum(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToRelativeWeekNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toRelativeWeekNum(t);
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return time_zone.toRelativeWeekNum(DayNum_t(d));
	}

	using FactorTransform = ZeroTransform;
};

struct ToRelativeDayNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toDayNum(t);
	}
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		return static_cast<DayNum_t>(d);
	}

	using FactorTransform = ZeroTransform;
};


struct ToRelativeHourNumImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toRelativeHourNum(t);
	}
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toRelativeHourNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ZeroTransform;
};

struct ToRelativeMinuteNumImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return time_zone.toRelativeMinuteNum(t);
	}
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toRelativeMinuteNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ZeroTransform;
};

struct ToRelativeSecondNumImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
	{
		return t;
	}
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
	{
		throw Exception("Illegal type Date of argument for function toRelativeSecondNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	using FactorTransform = ZeroTransform;
};


template<typename FromType, typename ToType, typename Transform>
struct Transformer
{
	static void vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const DateLUTImpl & time_zone)
	{
		size_t size = vec_from.size();
		vec_to.resize(size);

		for (size_t i = 0; i < size; ++i)
			vec_to[i] = Transform::execute(vec_from[i], time_zone);
	}

	static void constant(const FromType & from, ToType & to, const DateLUTImpl & time_zone)
	{
		to = Transform::execute(from, time_zone);
	}
};


template <typename FromType, typename ToType, typename Transform, typename Name>
struct DateTimeTransformImpl
{
	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		using Op = Transformer<FromType, ToType, Transform>;

		const ColumnPtr source_col = block.safeGetByPosition(arguments[0]).column;
		const auto * sources = typeid_cast<const ColumnVector<FromType> *>(source_col.get());
		const auto * const_source = typeid_cast<const ColumnConst<FromType> *>(source_col.get());

		const ColumnConstString * time_zone_column = nullptr;

		if (arguments.size() == 2)
		{
			time_zone_column = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[1]).column.get());

			if (!time_zone_column)
				throw Exception("Illegal column " + block.safeGetByPosition(arguments[1]).column->getName()
					+ " of second (time zone) argument of function " + Name::name + ", must be constant string",
					ErrorCodes::ILLEGAL_COLUMN);
		}

		const DateLUTImpl & time_zone = time_zone_column
			? DateLUT::instance(time_zone_column->getData())
			: DateLUT::instance();

		if (sources)
		{
			auto col_to = std::make_shared<ColumnVector<ToType>>();
			block.safeGetByPosition(result).column = col_to;
			Op::vector(sources->getData(), col_to->getData(), time_zone);
		}
		else if (const_source)
		{
			ToType res;
			Op::constant(const_source->getData(), res, time_zone);
			block.safeGetByPosition(result).column = std::make_shared<ColumnConst<ToType>>(const_source->size(), res);
		}
		else
		{
			throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
		}
	}
};


template <typename ToDataType, typename Transform, typename Name>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionDateOrDateTimeToSomething>(); };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() == 1)
		{
			if (!typeid_cast<const DataTypeDate *>(arguments[0].get())
				&& !typeid_cast<const DataTypeDateTime *>(arguments[0].get()))
				throw Exception{
					"Illegal type " + arguments[0]->getName() + " of argument of function " + getName() +
					". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		}
		else if (arguments.size() == 2)
		{
			if (!typeid_cast<const DataTypeDateTime *>(arguments[0].get())
				|| !typeid_cast<const DataTypeString *>(arguments[1].get()))
				throw Exception{
					"Function " + getName() + " supports 1 or 2 arguments. The 1st argument "
					"must be of type Date or DateTime. The 2nd argument (optional) must be "
					"a constant string with timezone name. The timezone argument is allowed "
					"only when the 1st argument has the type DateTime",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		}
		else
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1 or 2",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<ToDataType>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		IDataType * from_type = block.safeGetByPosition(arguments[0]).type.get();

		if (typeid_cast<const DataTypeDate *>(from_type))
			DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeDateTime * >(from_type))
			DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else
			throw Exception("Illegal type " + block.safeGetByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}


	bool hasInformationAboutMonotonicity() const override
	{
		return true;
	}

	Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
	{
		IFunction::Monotonicity is_monotonic { true };
		IFunction::Monotonicity is_not_monotonic;

		if (std::is_same<typename Transform::FactorTransform, ZeroTransform>::value)
			return is_monotonic;

		/// Этот метод вызывается только если у функции один аргумент. Поэтому, нас пока не волнует не-локальная тайм-зона.
		const DateLUTImpl & date_lut = DateLUT::instance();

		if (left.isNull() || right.isNull())
			return is_not_monotonic;

		/// Функция монотонна на отрезке [left, right], если фактор-преобразование возвращает для них одинаковые значения.

		if (typeid_cast<const DataTypeDate *>(&type))
		{
			return Transform::FactorTransform::execute(UInt16(left.get<UInt64>()), date_lut)
				== Transform::FactorTransform::execute(UInt16(right.get<UInt64>()), date_lut)
				? is_monotonic : is_not_monotonic;
		}
		else
		{
			return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), date_lut)
				== Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), date_lut)
				? is_monotonic : is_not_monotonic;
		}
	}
};


/// Получить текущее время. (Оно - константа, вычисляется один раз за весь запрос.)
class FunctionNow : public IFunction
{
public:
	static constexpr auto name = "now";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionNow>(); };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		return std::make_shared<DataTypeDateTime>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt32>(
			block.rows(),
			time(0));
	}
};


class FunctionToday : public IFunction
{
public:
	static constexpr auto name = "today";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionToday>(); };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		return std::make_shared<DataTypeDate>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt16>(
			block.rows(),
			DateLUT::instance().toDayNum(time(0)));
	}
};


class FunctionYesterday : public IFunction
{
public:
	static constexpr auto name = "yesterday";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionYesterday>(); };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		return std::make_shared<DataTypeDate>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt16>(
			block.rows(),
			DateLUT::instance().toDayNum(time(0)) - 1);
	}
};


class FunctionTimeSlot : public IFunction
{
public:
	static constexpr auto name = "timeSlot";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTimeSlot>(); };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (!typeid_cast<const DataTypeDateTime *>(arguments[0].get()))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeDateTime>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (const ColumnUInt32 * times = typeid_cast<const ColumnUInt32 *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			auto res = std::make_shared<ColumnUInt32>();
			ColumnPtr res_holder = res;
			ColumnUInt32::Container_t & res_vec = res->getData();
			const ColumnUInt32::Container_t & vec = times->getData();

			size_t size = vec.size();
			res_vec.resize(size);

			for (size_t i = 0; i < size; ++i)
				res_vec[i] = vec[i] / TIME_SLOT_SIZE * TIME_SLOT_SIZE;

			block.safeGetByPosition(result).column = res_holder;
		}
		else if (const ColumnConstUInt32 * const_times = typeid_cast<const ColumnConstUInt32 *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt32>(block.rows(), const_times->getData() / TIME_SLOT_SIZE * TIME_SLOT_SIZE);
		}
		else
			throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
					+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename DurationType>
struct TimeSlotsImpl
{
	static void vector_vector(
		const PaddedPODArray<UInt32> & starts, const PaddedPODArray<DurationType> & durations,
		PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = starts.size();

		result_offsets.resize(size);
		result_values.reserve(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + durations[i]) / TIME_SLOT_SIZE; ++value)
			{
				result_values.push_back(value * TIME_SLOT_SIZE);
				++current_offset;
			}

			result_offsets[i] = current_offset;
		}
	}

	static void vector_constant(
		const PaddedPODArray<UInt32> & starts, DurationType duration,
		PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = starts.size();

		result_offsets.resize(size);
		result_values.reserve(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + duration) / TIME_SLOT_SIZE; ++value)
			{
				result_values.push_back(value * TIME_SLOT_SIZE);
				++current_offset;
			}

			result_offsets[i] = current_offset;
		}
	}

	static void constant_vector(
		UInt32 start, const PaddedPODArray<DurationType> & durations,
		PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = durations.size();

		result_offsets.resize(size);
		result_values.reserve(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + durations[i]) / TIME_SLOT_SIZE; ++value)
			{
				result_values.push_back(value * TIME_SLOT_SIZE);
				++current_offset;
			}

			result_offsets[i] = current_offset;
		}
	}

	static void constant_constant(
		UInt32 start, DurationType duration,
		Array & result)
	{
		for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + duration) / TIME_SLOT_SIZE; ++value)
			result.push_back(static_cast<UInt64>(value * TIME_SLOT_SIZE));
	}
};


class FunctionTimeSlots : public IFunction
{
public:
	static constexpr auto name = "timeSlots";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTimeSlots>(); };

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 2; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (!typeid_cast<const DataTypeDateTime *>(arguments[0].get()))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeUInt32 *>(arguments[1].get()))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>());
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnUInt32 * starts = typeid_cast<const ColumnUInt32 *>(block.safeGetByPosition(arguments[0]).column.get());
		const ColumnConstUInt32 * const_starts = typeid_cast<const ColumnConstUInt32 *>(block.safeGetByPosition(arguments[0]).column.get());

		const ColumnUInt32 * durations = typeid_cast<const ColumnUInt32 *>(block.safeGetByPosition(arguments[1]).column.get());
		const ColumnConstUInt32 * const_durations = typeid_cast<const ColumnConstUInt32 *>(block.safeGetByPosition(arguments[1]).column.get());

		auto res = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt32>());
		ColumnPtr res_holder = res;
		ColumnUInt32::Container_t & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

		if (starts && durations)
		{
			TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), res_values, res->getOffsets());
			block.safeGetByPosition(result).column = res_holder;
		}
		else if (starts && const_durations)
		{
			TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getData(), res_values, res->getOffsets());
			block.safeGetByPosition(result).column = res_holder;
		}
		else if (const_starts && durations)
		{
			TimeSlotsImpl<UInt32>::constant_vector(const_starts->getData(), durations->getData(), res_values, res->getOffsets());
			block.safeGetByPosition(result).column = res_holder;
		}
		else if (const_starts && const_durations)
		{
			Array const_res;
			TimeSlotsImpl<UInt32>::constant_constant(const_starts->getData(), const_durations->getData(), const_res);
			block.safeGetByPosition(result).column = std::make_shared<ColumnConstArray>(block.rows(), const_res, std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()));
		}
		else
			throw Exception("Illegal columns " + block.safeGetByPosition(arguments[0]).column->getName()
					+ ", " + block.safeGetByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameToYear	 			{ static constexpr auto name = "toYear"; };
struct NameToMonth	 			{ static constexpr auto name = "toMonth"; };
struct NameToDayOfMonth			{ static constexpr auto name = "toDayOfMonth"; };
struct NameToDayOfWeek			{ static constexpr auto name = "toDayOfWeek"; };
struct NameToHour	 			{ static constexpr auto name = "toHour"; };
struct NameToMinute				{ static constexpr auto name = "toMinute"; };
struct NameToSecond				{ static constexpr auto name = "toSecond"; };
struct NameToStartOfDay			{ static constexpr auto name = "toStartOfDay"; };
struct NameToMonday				{ static constexpr auto name = "toMonday"; };
struct NameToStartOfMonth		{ static constexpr auto name = "toStartOfMonth"; };
struct NameToStartOfQuarter		{ static constexpr auto name = "toStartOfQuarter"; };
struct NameToStartOfYear		{ static constexpr auto name = "toStartOfYear"; };
struct NameToStartOfMinute		{ static constexpr auto name = "toStartOfMinute"; };
struct NameToStartOfFiveMinute	{ static constexpr auto name = "toStartOfFiveMinute"; };
struct NameToStartOfHour		{ static constexpr auto name = "toStartOfHour"; };
struct NameToTime	 			{ static constexpr auto name = "toTime"; };
struct NameToRelativeYearNum	{ static constexpr auto name = "toRelativeYearNum"; };
struct NameToRelativeMonthNum	{ static constexpr auto name = "toRelativeMonthNum"; };
struct NameToRelativeWeekNum	{ static constexpr auto name = "toRelativeWeekNum"; };
struct NameToRelativeDayNum		{ static constexpr auto name = "toRelativeDayNum"; };
struct NameToRelativeHourNum	{ static constexpr auto name = "toRelativeHourNum"; };
struct NameToRelativeMinuteNum	{ static constexpr auto name = "toRelativeMinuteNum"; };
struct NameToRelativeSecondNum	{ static constexpr auto name = "toRelativeSecondNum"; };


using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToYearImpl, 		NameToYear> 	;
using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMonthImpl, 		NameToMonth> 	;
using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfMonthImpl, 	NameToDayOfMonth> ;
using FunctionToDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfWeekImpl, 	NameToDayOfWeek> ;
using FunctionToHour = FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToHourImpl, 		NameToHour> 	;
using FunctionToMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMinuteImpl, 		NameToMinute> 	;
using FunctionToSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToSecondImpl, 		NameToSecond> 	;
using FunctionToStartOfDay = FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfDayImpl, NameToStartOfDay>;
using FunctionToMonday = FunctionDateOrDateTimeToSomething<DataTypeDate,		ToMondayImpl, 		NameToMonday> 	;
using FunctionToStartOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate,		ToStartOfMonthImpl, NameToStartOfMonth>;
using FunctionToStartOfQuarter = FunctionDateOrDateTimeToSomething<DataTypeDate,	ToStartOfQuarterImpl, 	NameToStartOfQuarter> ;
using FunctionToStartOfYear = FunctionDateOrDateTimeToSomething<DataTypeDate,		ToStartOfYearImpl, 	NameToStartOfYear> ;
using FunctionToStartOfMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfMinuteImpl, NameToStartOfMinute>;
using FunctionToStartOfFiveMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfFiveMinuteImpl, NameToStartOfFiveMinute>;
using FunctionToStartOfHour = FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfHourImpl, 	NameToStartOfHour> ;
using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToTimeImpl, 		NameToTime> 	;

using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToRelativeYearNumImpl, 		NameToRelativeYearNum> 	;
using FunctionToRelativeMonthNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeMonthNumImpl, 	NameToRelativeMonthNum> ;
using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeWeekNumImpl, 		NameToRelativeWeekNum> 	;
using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeDayNumImpl, 		NameToRelativeDayNum> 	;

using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeHourNumImpl, 		NameToRelativeHourNum> 	;
using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeMinuteNumImpl, 	NameToRelativeMinuteNum> ;
using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeSecondNumImpl, 	NameToRelativeSecondNum> ;


}
