#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции работы с датой и временем.
  *
  * toYear, toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond,
  * toMonday, toStartOfMonth,
  * toTime,
  * TODO: makeDate, makeDateTime
  * 
  * (toDate - расположена в файле FunctionsConversion.h)
  *
  * Возвращаемые типы:
  *  toYear -> UInt16
  *  toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond -> UInt8
  *  toMonday, toStartOfMonth -> Date
  *  toTime -> DateTime
  */

struct ToYearImpl
{
	static inline UInt16 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toYear(t); }
	static inline UInt16 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut) { return date_lut.toYear(Yandex::DayNum_t(d)); }
};

struct ToMonthImpl
{
	static inline UInt8 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toMonth(t); }
	static inline UInt8 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut) { return date_lut.toMonth(Yandex::DayNum_t(d)); }
};

struct ToDayOfMonthImpl
{
	static inline UInt8 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toDayOfMonth(t); }
	static inline UInt8 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut) { return date_lut.toDayOfMonth(Yandex::DayNum_t(d)); }
};

struct ToDayOfWeekImpl
{
	static inline UInt8 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toDayOfWeek(t); }
	static inline UInt8 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut) { return date_lut.toDayOfWeek(Yandex::DayNum_t(d)); }
};

struct ToHourImpl
{
	static inline UInt8 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toHourInaccurate(t); }
	static inline UInt8 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToMinuteImpl
{
	static inline UInt8 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toMinute(t); }
	static inline UInt8 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToSecondImpl
{
	static inline UInt8 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toSecond(t); }
	static inline UInt8 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToMondayImpl
{
	static inline UInt32 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toWeek(t); }
	static inline UInt32 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut) { return date_lut.toWeek(Yandex::DayNum_t(d)); }
};

struct ToStartOfMonthImpl
{
	static inline UInt32 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return date_lut.toFirstDayOfMonth(t); }
	static inline UInt32 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut) { return date_lut.toFirstDayOfMonth(Yandex::DayNum_t(d)); }
};

struct ToTimeImpl
{
	static inline UInt32 execute(UInt32 t, Yandex::DateLUTSingleton & date_lut) { return t - date_lut.toDate(t) - 10800; }
	static inline UInt32 execute(UInt16 d, Yandex::DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


template <typename FromType, typename ToType, typename Transform, typename Name>
struct DateTimeTransformImpl
{
	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();

		if (const ColumnVector<FromType> * col_from = dynamic_cast<const ColumnVector<FromType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToType> * col_to = new ColumnVector<ToType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::execute(vec_from[i], date_lut);
		}
		else if (const ColumnConst<FromType> * col_from = dynamic_cast<const ColumnConst<FromType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToType>(col_from->size(), Transform::execute(col_from->getData(), date_lut));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename ToDataType, typename Transform, typename Name>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new ToDataType;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IDataType * from_type = &*block.getByPosition(arguments[0]).type;
		
		if (dynamic_cast<const DataTypeDate *>(from_type))
			DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else if (dynamic_cast<const DataTypeDateTime * >(from_type))
			DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else
			throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


struct NameToYear 			{ static const char * get() { return "toYear"; } };
struct NameToMonth 			{ static const char * get() { return "toMonth"; } };
struct NameToDayOfMonth		{ static const char * get() { return "toDayOfMonth"; } };
struct NameToDayOfWeek		{ static const char * get() { return "toDayOfWeek"; } };
struct NameToHour 			{ static const char * get() { return "toHour"; } };
struct NameToMinute			{ static const char * get() { return "toMinute"; } };
struct NameToSecond			{ static const char * get() { return "toSecond"; } };
struct NameToMonday			{ static const char * get() { return "toMonday"; } };
struct NameToStartOfMonth	{ static const char * get() { return "toStartOfMonth"; } };
struct NameToTime 			{ static const char * get() { return "toTime"; } };

typedef FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToYearImpl, 		NameToYear> 		FunctionToYear;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMonthImpl, 		NameToMonth> 		FunctionToMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfMonthImpl, 	NameToDayOfMonth> 	FunctionToDayOfMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfWeekImpl, 	NameToDayOfWeek> 	FunctionToDayOfWeek;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToHourImpl, 		NameToHour> 		FunctionToHour;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMinuteImpl, 		NameToMinute> 		FunctionToMinute;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToSecondImpl, 		NameToSecond> 		FunctionToSecond;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToMondayImpl, 		NameToMonday> 		FunctionToMonday;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfMonthImpl, NameToStartOfMonth> FunctionToStartOfMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToTimeImpl, 		NameToTime> 		FunctionToTime;

}
