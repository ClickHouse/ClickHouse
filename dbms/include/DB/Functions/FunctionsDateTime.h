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


struct ToYearImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toYear(t); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toYear(DayNum_t(d)); }
};

struct ToMonthImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toMonth(t); }
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toMonth(DayNum_t(d)); }
};

struct ToDayOfMonthImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toDayOfMonth(t); }
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toDayOfMonth(DayNum_t(d)); }
};

struct ToDayOfWeekImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toDayOfWeek(t); }
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toDayOfWeek(DayNum_t(d)); }
};

struct ToHourImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toHourInaccurate(t); }
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToMinuteImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toMinuteInaccurate(t); }
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToSecondImpl
{
	static inline UInt8 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toSecondInaccurate(t); }
	static inline UInt8 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToMondayImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfWeek(remote_date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfWeek(DayNum_t(d)); }
};

struct ToStartOfMonthImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfMonth(remote_date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfMonth(DayNum_t(d)); }
};

struct ToStartOfQuarterImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfQuarter(remote_date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfQuarter(DayNum_t(d)); }
};

struct ToStartOfYearImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfYear(remote_date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toFirstDayNumOfYear(DayNum_t(d)); }
};


struct ToTimeImpl
{
	/// При переводе во время, дату будем приравнивать к 1970-01-02.
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		time_t remote_ts = remote_date_lut.toTimeInaccurate(t) + 86400;

		if (&remote_date_lut == &local_date_lut)
			return remote_ts;
		else
		{
			const auto & values = remote_date_lut.getValues(remote_ts);
			return local_date_lut.makeDateTime(values.year, values.month, values.day_of_month,
											remote_date_lut.toHourInaccurate(remote_ts),
											remote_date_lut.toMinuteInaccurate(remote_ts),
											remote_date_lut.toSecondInaccurate(remote_ts));
		}
	}

	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToStartOfMinuteImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toStartOfMinuteInaccurate(t); }
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToStartOfFiveMinuteImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toStartOfFiveMinuteInaccurate(t); }
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfFiveMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToStartOfHourImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toStartOfHourInaccurate(t); }
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToRelativeYearNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toYear(t); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toYear(DayNum_t(d)); }
};

struct ToRelativeMonthNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toRelativeMonthNum(t); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toRelativeMonthNum(DayNum_t(d)); }
};

struct ToRelativeWeekNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toRelativeWeekNum(t); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toRelativeWeekNum(DayNum_t(d)); }
};

struct ToRelativeDayNumImpl
{
	static inline UInt16 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toDayNum(t); }
	static inline UInt16 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return static_cast<DayNum_t>(d); }
};


struct ToRelativeHourNumImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toRelativeHourNum(t); }
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toRelativeHourNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToRelativeMinuteNumImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return remote_date_lut.toRelativeMinuteNum(t); }
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toRelativeMinuteNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToRelativeSecondNumImpl
{
	static inline UInt32 execute(UInt32 t, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut) { return t; }
	static inline UInt32 execute(UInt16 d, const DateLUTImpl & remote_date_lut, const DateLUTImpl & local_date_lut)
	{
		throw Exception("Illegal type Date of argument for function toRelativeSecondNum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

template<typename FromType, typename ToType, typename Transform>
struct Transformer
{
	static void vector_vector(const PODArray<FromType> & vec_from, const ColumnString::Chars_t & data,
							  const ColumnString::Offsets_t & offsets, PODArray<ToType> & vec_to)
	{
		const auto & local_date_lut = DateLUT::instance();
		ColumnString::Offset_t prev_offset = 0;

		for (size_t i = 0; i < vec_from.size(); ++i)
		{
			ColumnString::Offset_t cur_offset = offsets[i];
			const std::string time_zone(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset - 1);
			const auto & remote_date_lut = DateLUT::instance(time_zone);
			vec_to[i] = Transform::execute(vec_from[i], remote_date_lut, local_date_lut);
			prev_offset = cur_offset;
		}
	}

	static void vector_constant(const PODArray<FromType> & vec_from, const std::string & data,
								PODArray<ToType> & vec_to)
	{
		const auto & local_date_lut = DateLUT::instance();
		const auto & remote_date_lut = DateLUT::instance(data);
		for (size_t i = 0; i < vec_from.size(); ++i)
			vec_to[i] = Transform::execute(vec_from[i], remote_date_lut, local_date_lut);
	}

	static void vector_constant(const PODArray<FromType> & vec_from, PODArray<ToType> & vec_to)
	{
		const auto & local_date_lut = DateLUT::instance();
		for (size_t i = 0; i < vec_from.size(); ++i)
			vec_to[i] = Transform::execute(vec_from[i], local_date_lut, local_date_lut);
	}

	static void constant_vector(const FromType & from, const ColumnString::Chars_t & data,
								const ColumnString::Offsets_t & offsets, PODArray<ToType> & vec_to)
	{
		const auto & local_date_lut = DateLUT::instance();
		ColumnString::Offset_t prev_offset = 0;

		for (size_t i = 0; i < offsets.size(); ++i)
		{
			ColumnString::Offset_t cur_offset = offsets[i];
			const std::string time_zone(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset - 1);
			const auto & remote_date_lut = DateLUT::instance(time_zone);
			vec_to[i] = Transform::execute(from, remote_date_lut, local_date_lut);
			prev_offset = cur_offset;
		}
	}

	static void constant_constant(const FromType & from, const std::string & data, ToType & to)
	{
		const auto & local_date_lut = DateLUT::instance();
		const auto & remote_date_lut = DateLUT::instance(data);
		to = Transform::execute(from, remote_date_lut, local_date_lut);
	}

	static void constant_constant(const FromType & from, ToType & to)
	{
		const auto & local_date_lut = DateLUT::instance();
		to = Transform::execute(from, local_date_lut, local_date_lut);
	}
};

template <typename FromType, typename ToType, typename Transform, typename Name>
struct DateTimeTransformImpl
{
	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		using Op = Transformer<FromType, ToType, Transform>;

		const ColumnPtr source_col = block.getByPosition(arguments[0]).column;
		const auto * sources = typeid_cast<const ColumnVector<FromType> *>(&*source_col);
		const auto * const_source = typeid_cast<const ColumnConst<FromType> *>(&*source_col);

		if (arguments.size() == 1)
		{
			if (sources)
			{
				auto * col_to = new ColumnVector<ToType>;
				block.getByPosition(result).column = col_to;

				auto & vec_from = sources->getData();
				auto & vec_to = col_to->getData();
				size_t size = vec_from.size();
				vec_to.resize(size);

				Op::vector_constant(vec_from, vec_to);
			}
			else if (const_source)
			{
				ToType res;
				Op::constant_constant(const_source->getData(), res);
				block.getByPosition(result).column = new ColumnConst<ToType>(const_source->size(), res);
			}
			else
			{
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of first argument of function " + Name::name,
					ErrorCodes::ILLEGAL_COLUMN);
			}
		}
		else if (arguments.size() == 2)
		{
			const ColumnPtr time_zone_col = block.getByPosition(arguments[1]).column;
			const auto * time_zones = typeid_cast<const ColumnString *>(&*time_zone_col);
			const auto * const_time_zone = typeid_cast<const ColumnConstString *>(&*time_zone_col);

			if (sources)
			{
				auto * col_to = new ColumnVector<ToType>;
				block.getByPosition(result).column = col_to;

				auto & vec_from = sources->getData();
				auto & vec_to = col_to->getData();
				vec_to.resize(vec_from.size());

				if (time_zones)
					Op::vector_vector(vec_from, time_zones->getChars(), time_zones->getOffsets(), vec_to);
				else if (const_time_zone)
					Op::vector_constant(vec_from, const_time_zone->getData(), vec_to);
				else
					throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
							+ " of second argument of function " + Name::name,
						ErrorCodes::ILLEGAL_COLUMN);
			}
			else if (const_source)
			{
				if (time_zones)
				{
					auto * col_to = new ColumnVector<ToType>;
					block.getByPosition(result).column = col_to;

					auto & vec_to = col_to->getData();
					vec_to.resize(time_zones->getOffsets().size());

					Op::constant_vector(const_source->getData(), time_zones->getChars(), time_zones->getOffsets(), vec_to);
				}
				else if (const_time_zone)
				{
					ToType res;
					Op::constant_constant(const_source->getData(), const_time_zone->getData(), res);
					block.getByPosition(result).column = new ColumnConst<ToType>(const_source->size(), res);
				}
				else
					throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
							+ " of second argument of function " + Name::name,
						ErrorCodes::ILLEGAL_COLUMN);
			}
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of first argument of function " + Name::name,
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else
			throw Exception("Internal error.", ErrorCodes::LOGICAL_ERROR);
	}
};

template <typename ToDataType, typename Transform, typename Name>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionDateOrDateTimeToSomething; };

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		return getReturnTypeImpl(arguments);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IDataType * from_type = &*block.getByPosition(arguments[0]).type;

		if (typeid_cast<const DataTypeDate *>(from_type))
			DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeDateTime * >(from_type))
			DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else
			throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

private:
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.

	template<typename ToDataType2 = ToDataType, typename Transform2 = Transform>
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments,
		typename std::enable_if<
			!(std::is_same<ToDataType2, DataTypeDate>::value
			|| (std::is_same<ToDataType2, DataTypeDateTime>::value && std::is_same<Transform2, ToTimeImpl>::value))
		, void>::type * = nullptr) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new ToDataType;
	}

	template<typename ToDataType2 = ToDataType, typename Transform2 = Transform>
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments,
		typename std::enable_if<
			std::is_same<ToDataType2, DataTypeDate>::value
			|| (std::is_same<ToDataType2, DataTypeDateTime>::value && std::is_same<Transform2, ToTimeImpl>::value)
		, void>::type * = nullptr) const
	{
		if ((arguments.size() < 1) || (arguments.size() > 2))
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1 or 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (typeid_cast<const DataTypeDateTime *>(&*arguments[0]) == nullptr)
		{
			if (arguments.size() != 1)
				throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
					+ toString(arguments.size()) + ", should be 1.",
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		}
		else if ((arguments.size()) == 2 && typeid_cast<const DataTypeString *>(&*arguments[1]) == nullptr)
		{
			throw Exception{
				"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};
		}

		return new ToDataType;
	}
};


/// Получить текущее время. (Оно - константа, вычисляется один раз за весь запрос.)
class FunctionNow : public IFunction
{
public:
	static constexpr auto name = "now";
	static IFunction * create(const Context & context) { return new FunctionNow; };

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

		return new DataTypeDateTime;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstUInt32(
			block.rowsInFirstColumn(),
			time(0));
	}
};


class FunctionToday : public IFunction
{
public:
	static constexpr auto name = "today";
	static IFunction * create(const Context & context) { return new FunctionToday; };

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

		return new DataTypeDate;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstUInt16(
			block.rowsInFirstColumn(),
			DateLUT::instance().toDayNum(time(0)));
	}
};


class FunctionYesterday : public IFunction
{
public:
	static constexpr auto name = "yesterday";
	static IFunction * create(const Context & context) { return new FunctionYesterday; };

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

		return new DataTypeDate;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstUInt16(
			block.rowsInFirstColumn(),
			DateLUT::instance().toDayNum(time(0)) - 1);
	}
};


class FunctionTimeSlot : public IFunction
{
public:
	static constexpr auto name = "timeSlot";
	static IFunction * create(const Context & context) { return new FunctionTimeSlot; };

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

		if (!typeid_cast<const DataTypeDateTime *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeDateTime;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnUInt32 * times = typeid_cast<const ColumnUInt32 *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnUInt32 * res = new ColumnUInt32;
			ColumnPtr res_holder = res;
			ColumnUInt32::Container_t & res_vec = res->getData();
			const ColumnUInt32::Container_t & vec = times->getData();

			size_t size = vec.size();
			res_vec.resize(size);

			for (size_t i = 0; i < size; ++i)
				res_vec[i] = vec[i] / TIME_SLOT_SIZE * TIME_SLOT_SIZE;

			block.getByPosition(result).column = res_holder;
		}
		else if (const ColumnConstUInt32 * const_times = typeid_cast<const ColumnConstUInt32 *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConstUInt32(block.rowsInFirstColumn(), const_times->getData() / TIME_SLOT_SIZE * TIME_SLOT_SIZE);
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename DurationType>
struct TimeSlotsImpl
{
	static void vector_vector(
		const PODArray<UInt32> & starts, const PODArray<DurationType> & durations,
		PODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
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
		const PODArray<UInt32> & starts, DurationType duration,
		PODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
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
		UInt32 start, const PODArray<DurationType> & durations,
		PODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
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
	static IFunction * create(const Context & context) { return new FunctionTimeSlots; };

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeDateTime *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeUInt32 *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(new DataTypeDateTime);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnUInt32 * starts = typeid_cast<const ColumnUInt32 *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConstUInt32 * const_starts = typeid_cast<const ColumnConstUInt32 *>(&*block.getByPosition(arguments[0]).column);

		const ColumnUInt32 * durations = typeid_cast<const ColumnUInt32 *>(&*block.getByPosition(arguments[1]).column);
		const ColumnConstUInt32 * const_durations = typeid_cast<const ColumnConstUInt32 *>(&*block.getByPosition(arguments[1]).column);

		ColumnArray * res = new ColumnArray(new ColumnUInt32);
		ColumnPtr res_holder = res;
		ColumnUInt32::Container_t & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

		if (starts && durations)
		{
			TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), res_values, res->getOffsets());
			block.getByPosition(result).column = res_holder;
		}
		else if (starts && const_durations)
		{
			TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getData(), res_values, res->getOffsets());
			block.getByPosition(result).column = res_holder;
		}
		else if (const_starts && durations)
		{
			TimeSlotsImpl<UInt32>::constant_vector(const_starts->getData(), durations->getData(), res_values, res->getOffsets());
			block.getByPosition(result).column = res_holder;
		}
		else if (const_starts && const_durations)
		{
			Array const_res;
			TimeSlotsImpl<UInt32>::constant_constant(const_starts->getData(), const_durations->getData(), const_res);
			block.getByPosition(result).column = new ColumnConstArray(block.rowsInFirstColumn(), const_res, new DataTypeArray(new DataTypeDateTime));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
					+ ", " + block.getByPosition(arguments[1]).column->getName()
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


typedef FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToYearImpl, 		NameToYear> 		FunctionToYear;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMonthImpl, 		NameToMonth> 		FunctionToMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfMonthImpl, 	NameToDayOfMonth> 	FunctionToDayOfMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfWeekImpl, 	NameToDayOfWeek> 	FunctionToDayOfWeek;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToHourImpl, 		NameToHour> 		FunctionToHour;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMinuteImpl, 		NameToMinute> 		FunctionToMinute;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToSecondImpl, 		NameToSecond> 		FunctionToSecond;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,		ToMondayImpl, 		NameToMonday> 		FunctionToMonday;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,		ToStartOfMonthImpl, NameToStartOfMonth> FunctionToStartOfMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,	ToStartOfQuarterImpl, 	NameToStartOfQuarter> 	FunctionToStartOfQuarter;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,		ToStartOfYearImpl, 	NameToStartOfYear> 	FunctionToStartOfYear;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfMinuteImpl, NameToStartOfMinute> FunctionToStartOfMinute;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfFiveMinuteImpl, NameToStartOfFiveMinute> FunctionToStartOfFiveMinute;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfHourImpl, 	NameToStartOfHour> 	FunctionToStartOfHour;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToTimeImpl, 		NameToTime> 		FunctionToTime;

typedef FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToRelativeYearNumImpl, 		NameToRelativeYearNum> 		FunctionToRelativeYearNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeMonthNumImpl, 	NameToRelativeMonthNum> 	FunctionToRelativeMonthNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeWeekNumImpl, 		NameToRelativeWeekNum> 		FunctionToRelativeWeekNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeDayNumImpl, 		NameToRelativeDayNum> 		FunctionToRelativeDayNum;

typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeHourNumImpl, 		NameToRelativeHourNum> 		FunctionToRelativeHourNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeMinuteNumImpl, 	NameToRelativeMinuteNum> 	FunctionToRelativeMinuteNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeSecondNumImpl, 	NameToRelativeSecondNum> 	FunctionToRelativeSecondNum;


}
