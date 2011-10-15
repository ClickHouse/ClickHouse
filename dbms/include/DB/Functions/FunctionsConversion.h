#pragma once

#include <tr1/type_traits>

#include <Poco/NumberFormatter.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции преобразования типов.
  *
  * Бывают двух видов:
  * - toType - преобразование "естественным образом";
  * - reinterpretAsType - преобразования чисел и дат в строки, содержащие тот же набор байт в машинном представлении, и наоборот.
  */

/** Преобразование чисел в строки и наоборот: через форматирование и парсинг.
  * Преобразование дат и дат с временем в строки и наоборот: то же, что и выше, но форматировать и парсить надо по другому.
  */

/** Преобразование чисел друг в друга, дат/дат-с-временем в числа и наоборот: делается обычным присваиванием.
  *  (дата внутри хранится как количество дней с какого-то, дата-с-временем - как unix timestamp)
  * Преобразование даты в дату-с-временем и наоборот: добавление нулевого времени или вырезание только даты.
  */
template <typename ToDataType, typename Name>
class FunctionConvert : public IFunction
{
private:
	typedef typename ToDataType::FieldType ToFieldType;
	
	template <typename FromFieldType>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<FromFieldType> * col_from = dynamic_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = vec_from[i];

			return true;
		}
		else if (const ColumnConst<FromFieldType> * col_from = dynamic_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), col_from->getData());
			
			return true;
		}
		else
			return false;
	}

	void executeDateToDateTime(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		typedef DataTypeDate::FieldType FromFieldType;
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		
		if (const ColumnVector<FromFieldType> * col_from = dynamic_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = date_lut.fromDayNum(Yandex::DayNum_t(vec_from[i]));
		}
		else if (const ColumnConst<FromFieldType> * col_from = dynamic_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), date_lut.fromDayNum(Yandex::DayNum_t(col_from->getData())));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

	void executeDateTimeToDate(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		typedef DataTypeDateTime::FieldType FromFieldType;
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();

		if (const ColumnVector<FromFieldType> * col_from = dynamic_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = date_lut.toDayNum(vec_from[i]);
		}
		else if (const ColumnConst<FromFieldType> * col_from = dynamic_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), date_lut.toDayNum(col_from->getData()));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
	
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

		if (!arguments[0]->isNumeric()
			&& !dynamic_cast<const DataTypeDate *>(&*arguments[0])
			&& !dynamic_cast<const DataTypeDateTime *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new ToDataType;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (dynamic_cast<const DataTypeDate *>(&*block.getByPosition(arguments[0]).type)
			&& std::tr1::is_same<ToDataType, DataTypeDateTime>::value)
		{
			/// Преобразование даты в дату с временем
			executeDateToDateTime(block, arguments, result);
		}
		else if (dynamic_cast<const DataTypeDateTime *>(&*block.getByPosition(arguments[0]).type)
			&& std::tr1::is_same<ToDataType, DataTypeDate>::value)
		{
			/// Преобразование даты с временем в дату
			executeDateTimeToDate(block, arguments, result);
		}
		else if (!(	executeType<UInt8>(block, arguments, result)
			||	executeType<UInt16>(block, arguments, result)
			||	executeType<UInt32>(block, arguments, result)
			||	executeType<UInt64>(block, arguments, result)
			||	executeType<Int8>(block, arguments, result)
			||	executeType<Int16>(block, arguments, result)
			||	executeType<Int32>(block, arguments, result)
			||	executeType<Int64>(block, arguments, result)
			||	executeType<Float32>(block, arguments, result)
			||	executeType<Float64>(block, arguments, result)))
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameToUInt8 			{ static const char * get() { return "toUInt8"; } };
struct NameToUInt16 		{ static const char * get() { return "toUInt16"; } };
struct NameToUInt32 		{ static const char * get() { return "toUInt32"; } };
struct NameToUInt64 		{ static const char * get() { return "toUInt64"; } };
struct NameToInt8 			{ static const char * get() { return "toInt8"; } };
struct NameToInt16	 		{ static const char * get() { return "toInt16"; } };
struct NameToInt32			{ static const char * get() { return "toInt32"; } };
struct NameToInt64			{ static const char * get() { return "toInt64"; } };
struct NameToFloat32		{ static const char * get() { return "toFloat32"; } };
struct NameToFloat64		{ static const char * get() { return "toFloat64"; } };
struct NameToVarUInt		{ static const char * get() { return "toVarUInt"; } };
struct NameToVatInt			{ static const char * get() { return "toVarInt"; } };
struct NameToDate			{ static const char * get() { return "toDate"; } };
struct NameToDateTime		{ static const char * get() { return "toDateTime"; } };

typedef FunctionConvert<DataTypeUInt8,		NameToUInt8> 		FunctionToUInt8;
typedef FunctionConvert<DataTypeUInt16,		NameToUInt16> 		FunctionToUInt16;
typedef FunctionConvert<DataTypeUInt32,		NameToUInt32> 		FunctionToUInt32;
typedef FunctionConvert<DataTypeUInt64,		NameToUInt64> 		FunctionToUInt64;
typedef FunctionConvert<DataTypeInt8,		NameToInt8> 		FunctionToInt8;
typedef FunctionConvert<DataTypeInt16,		NameToInt16> 		FunctionToInt16;
typedef FunctionConvert<DataTypeInt32,		NameToInt32> 		FunctionToInt32;
typedef FunctionConvert<DataTypeInt64,		NameToInt64> 		FunctionToInt64;
typedef FunctionConvert<DataTypeFloat32,	NameToFloat32> 		FunctionToFloat32;
typedef FunctionConvert<DataTypeFloat64,	NameToFloat64> 		FunctionToFloat64;
typedef FunctionConvert<DataTypeVarUInt,	NameToVarUInt> 		FunctionToVarUInt;
typedef FunctionConvert<DataTypeVarInt,		NameToVatInt> 		FunctionToVarInt;
typedef FunctionConvert<DataTypeDate,		NameToDate> 		FunctionToDate;
typedef FunctionConvert<DataTypeDateTime,	NameToDateTime> 	FunctionToDateTime;


}
