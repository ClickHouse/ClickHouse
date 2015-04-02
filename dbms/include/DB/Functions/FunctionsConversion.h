#pragma once

#include <DB/IO/WriteBufferFromVector.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>
#include <statdaemons/ext/range.hpp>


namespace DB
{

/** Функции преобразования типов.
  * toType - преобразование "естественным образом";
  */


/** Преобразование чисел друг в друга, дат/дат-с-временем в числа и наоборот: делается обычным присваиванием.
  *  (дата внутри хранится как количество дней с какого-то, дата-с-временем - как unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImpl
{
	typedef typename FromDataType::FieldType FromFieldType;
	typedef typename ToDataType::FieldType ToFieldType;

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<FromFieldType> * col_from = typeid_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = vec_from[i];
		}
		else if (const ColumnConst<FromFieldType> * col_from = typeid_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), col_from->getData());
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/** Преобразование даты в дату-с-временем: добавление нулевого времени.
  */
template <typename Name>
struct ConvertImpl<DataTypeDate, DataTypeDateTime, Name>
{
	typedef DataTypeDate::FieldType FromFieldType;
	typedef DataTypeDateTime::FieldType ToFieldType;

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		typedef DataTypeDate::FieldType FromFieldType;
		DateLUT & date_lut = DateLUT::instance();

		if (const ColumnVector<FromFieldType> * col_from = typeid_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
			{
				vec_to[i] = date_lut.fromDayNum(DayNum_t(vec_from[i]));
			}
		}
		else if (const ColumnConst<FromFieldType> * col_from = typeid_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), date_lut.fromDayNum(DayNum_t(col_from->getData())));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/** Преобразование даты-с-временем в дату: отбрасывание времени.
  */
template <typename Name>
struct ConvertImpl<DataTypeDateTime, DataTypeDate, Name>
{
	typedef DataTypeDateTime::FieldType FromFieldType;
	typedef DataTypeDate::FieldType ToFieldType;

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		DateLUT & date_lut = DateLUT::instance();

		if (const ColumnVector<FromFieldType> * col_from = typeid_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
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
		else if (const ColumnConst<FromFieldType> * col_from = typeid_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), date_lut.toDayNum(col_from->getData()));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/** Отдельный случай для преобразования (U)Int32 или (U)Int64 в Date.
  * Если число меньше 65536, то оно понимается, как DayNum, а если больше - как unix timestamp.
  * Немного нелогично, что мы, по сути, помещаем две разные функции в одну.
  * Но зато это позволяет поддержать распространённый случай,
  *  когда пользователь пишет toDate(UInt32), ожидая, что это - перевод unix timestamp в дату
  *  (иначе такое использование было бы распространённой ошибкой).
  */
template <typename FromDataType, typename Name>
struct ConvertImpl32Or64ToDate
{
	typedef typename FromDataType::FieldType FromFieldType;
	typedef DataTypeDate::FieldType ToFieldType;

	template <typename To, typename From>
	static To convert(const From & from, const DateLUT & date_lut)
	{
		return from < 0xFFFF
			? from
			: date_lut.toDayNum(from);
	}

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		DateLUT & date_lut = DateLUT::instance();

		if (const ColumnVector<FromFieldType> * col_from
			= typeid_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = convert<ToFieldType>(vec_from[i], date_lut);
		}
		else if (const ColumnConst<FromFieldType> * col_from
			= typeid_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(),
				convert<ToFieldType>(col_from->getData(), date_lut));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};

template <typename Name> struct ConvertImpl<DataTypeUInt32, DataTypeDate, Name> : ConvertImpl32Or64ToDate<DataTypeUInt32, Name> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDate, Name> : ConvertImpl32Or64ToDate<DataTypeUInt64, Name> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDate, Name> : ConvertImpl32Or64ToDate<DataTypeInt32, Name> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDate, Name> : ConvertImpl32Or64ToDate<DataTypeInt64, Name> {};


/** Преобразование чисел, дат, дат-с-временем в строки: через форматирование.
  */
template <typename DataType> void formatImpl(typename DataType::FieldType x, WriteBuffer & wb) { writeText(x, wb); }
template <> inline void formatImpl<DataTypeDate>(DataTypeDate::FieldType x, WriteBuffer & wb) { writeDateText(DayNum_t(x), wb); }
template <> inline void formatImpl<DataTypeDateTime>(DataTypeDateTime::FieldType x, WriteBuffer & wb) { writeDateTimeText(x, wb); }

template <typename FromDataType, typename Name>
struct ConvertImpl<FromDataType, DataTypeString, Name>
{
	typedef typename FromDataType::FieldType FromFieldType;

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<FromFieldType> * col_from = typeid_cast<const ColumnVector<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnString * col_to = new ColumnString;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
			ColumnString::Chars_t & data_to = col_to->getChars();
			ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
			size_t size = vec_from.size();
			data_to.resize(size * 2);
			offsets_to.resize(size);

			WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

			for (size_t i = 0; i < size; ++i)
			{
				formatImpl<FromDataType>(vec_from[i], write_buffer);
				writeChar(0, write_buffer);
				offsets_to[i] = write_buffer.count();
			}
			data_to.resize(write_buffer.count());
		}
		else if (const ColumnConst<FromFieldType> * col_from = typeid_cast<const ColumnConst<FromFieldType> *>(&*block.getByPosition(arguments[0]).column))
		{
			std::vector<char> buf;
			WriteBufferFromVector<std::vector<char> > write_buffer(buf);
			formatImpl<FromDataType>(col_from->getData(), write_buffer);
			block.getByPosition(result).column = new ColumnConstString(col_from->size(), std::string(&buf[0], write_buffer.count()));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/** Преобразование строк в числа, даты, даты-с-временем: через парсинг.
  */
template <typename DataType> void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb) { readText(x,rb); }

template <> inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb)
{
	DayNum_t tmp(0);
	readDateText(tmp, rb);
	x = tmp;
}

template <> inline void parseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb)
{
	time_t tmp = 0;
	readDateTimeText(tmp, rb);
	x = tmp;
}

template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeString, ToDataType, Name>
{
	typedef typename ToDataType::FieldType ToFieldType;

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnString * col_from = typeid_cast<const ColumnString *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const ColumnString::Chars_t & data_from = col_from->getChars();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = col_from->size();
			vec_to.resize(size);

			ReadBuffer read_buffer(const_cast<char *>(reinterpret_cast<const char *>(&data_from[0])), data_from.size(), 0);

			char zero = 0;
			for (size_t i = 0; i < size; ++i)
			{
				parseImpl<ToDataType>(vec_to[i], read_buffer);
				readChar(zero, read_buffer);
				if (zero != 0)
					throw Exception("Cannot parse from string.", ErrorCodes::CANNOT_PARSE_NUMBER);
			}
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column))
		{
			const String & s = col_from->getData();
			ReadBufferFromString read_buffer(s);
			ToFieldType x = 0;
			parseImpl<ToDataType>(x, read_buffer);
			block.getByPosition(result).column = new ColumnConst<ToFieldType>(col_from->size(), x);
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};

/** Если типы совпадают - просто скопируем ссылку на столбец.
  */
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeString, Name>
{
	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
	}
};


/** Преобразование из FixedString.
  */
template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeFixedString, ToDataType, Name>
{
	typedef typename ToDataType::FieldType ToFieldType;

	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_to = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_to;

			const ColumnFixedString::Chars_t & data_from = col_from->getChars();
			size_t n = col_from->getN();
			typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
			size_t size = col_from->size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
			{
				char * begin = const_cast<char *>(reinterpret_cast<const char *>(&data_from[i * n]));
				char * end = begin + n;
				ReadBuffer read_buffer(begin, n, 0);
				parseImpl<ToDataType>(vec_to[i], read_buffer);

				if (!read_buffer.eof())
				{
					while (read_buffer.position() < end && *read_buffer.position() == 0)
						++read_buffer.position();

					if (read_buffer.position() < end)
						throw Exception("Cannot parse from fixed string.", ErrorCodes::CANNOT_PARSE_NUMBER);
				}
			}
		}
		else if (typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column))
		{
			ConvertImpl<DataTypeString, ToDataType, Name>::execute(block, arguments, result);
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};

/** Преобразование из FixedString в String.
  * При этом, вырезаются последовательности нулевых байт с конца строк.
  */
template <typename Name>
struct ConvertImpl<DataTypeFixedString, DataTypeString, Name>
{
	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnString * col_to = new ColumnString;
			block.getByPosition(result).column = col_to;

			const ColumnFixedString::Chars_t & data_from = col_from->getChars();
			ColumnString::Chars_t & data_to = col_to->getChars();
			ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
			size_t size = col_from->size();
			size_t n = col_from->getN();
			data_to.resize(size * (n + 1));		/// + 1 - нулевой байт
			offsets_to.resize(size);

			size_t offset_from = 0;
			size_t offset_to = 0;
			for (size_t i = 0; i < size; ++i)
			{
				size_t bytes_to_copy = n;
				while (bytes_to_copy > 0 && data_from[offset_from + bytes_to_copy - 1] == 0)
					--bytes_to_copy;

				memcpy(&data_to[offset_to], &data_from[offset_from], bytes_to_copy);
				offset_from += n;
				offset_to += bytes_to_copy;
				data_to[offset_to] = 0;
				++offset_to;
				offsets_to[i] = offset_to;
			}

			data_to.resize(offset_to);
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column))
		{
			const String & s = col_from->getData();

			size_t bytes_to_copy = s.size();
			while (bytes_to_copy > 0 && s[bytes_to_copy - 1] == 0)
				--bytes_to_copy;

			block.getByPosition(result).column = new ColumnConstString(col_from->size(), s.substr(0, bytes_to_copy));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename ToDataType, typename Name>
class FunctionConvert : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionConvert; }

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

		return new ToDataType;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IDataType * from_type = &*block.getByPosition(arguments[0]).type;

		if      (typeid_cast<const DataTypeUInt8 *		>(from_type)) ConvertImpl<DataTypeUInt8, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeUInt16 *		>(from_type)) ConvertImpl<DataTypeUInt16, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeUInt32 *		>(from_type)) ConvertImpl<DataTypeUInt32, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeUInt64 *		>(from_type)) ConvertImpl<DataTypeUInt64, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeInt8 *		>(from_type)) ConvertImpl<DataTypeInt8, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeInt16 *		>(from_type)) ConvertImpl<DataTypeInt16, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeInt32 *		>(from_type)) ConvertImpl<DataTypeInt32, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeInt64 *		>(from_type)) ConvertImpl<DataTypeInt64, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeFloat32 *	>(from_type)) ConvertImpl<DataTypeFloat32, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeFloat64 *	>(from_type)) ConvertImpl<DataTypeFloat64, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeDate *		>(from_type)) ConvertImpl<DataTypeDate, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeDateTime *	>(from_type)) ConvertImpl<DataTypeDateTime,	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeString *		>(from_type)) ConvertImpl<DataTypeString, 	ToDataType, Name>::execute(block, arguments, result);
		else if (typeid_cast<const DataTypeFixedString *>(from_type)) ConvertImpl<DataTypeFixedString, ToDataType, Name>::execute(block, arguments, result);
		else
			throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


/** Преобразование в строку фиксированной длины реализовано только из строк.
  */
class FunctionToFixedString : public IFunction
{
public:
	static constexpr auto name = "toFixedString";
	static IFunction * create(const Context & context) { return new FunctionToFixedString; };

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/** Получить тип результата по типам аргументов и значениям константных аргументов.
	  * Если функция неприменима для данных аргументов - кинуть исключение.
	  * Для неконстантных столбцов arguments[i].column = nullptr.
	  */
	void getReturnTypeAndPrerequisites(const ColumnsWithNameAndType & arguments,
												DataTypePtr & out_return_type,
												ExpressionActions::Actions & out_prerequisites)
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		if (!arguments[1].column)
			throw Exception("Second argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
		if (!typeid_cast<const DataTypeString *>(arguments[0].type.get()) &&
			!typeid_cast<const DataTypeFixedString *>(arguments[0].type.get()))
			throw Exception(getName() + " is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED);

		const size_t n = getSize(arguments[1]);

		out_return_type = new DataTypeFixedString(n);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnPtr column = block.getByPosition(arguments[0]).column;
		size_t n = getSize(block.getByPosition(arguments[1]));

		if (const ColumnConstString * column_const = typeid_cast<const ColumnConstString *>(&*column))
		{
			if (column_const->getData().size() > n)
				throw Exception("String too long for type FixedString(" + toString(n) + ")",
					ErrorCodes::TOO_LARGE_STRING_SIZE);

			auto resized_string = column_const->getData();
			resized_string.resize(n);

			block.getByPosition(result).column = new ColumnConst<String>(column_const->size(), std::move(resized_string), new DataTypeFixedString(n));
		}
		else if (const ColumnString * column_string = typeid_cast<const ColumnString *>(&*column))
		{
			ColumnFixedString * column_fixed = new ColumnFixedString(n);
			ColumnPtr result_ptr = column_fixed;
			ColumnFixedString::Chars_t & out_chars = column_fixed->getChars();
			const ColumnString::Chars_t & in_chars = column_string->getChars();
			const ColumnString::Offsets_t & in_offsets = column_string->getOffsets();
			out_chars.resize_fill(in_offsets.size() * n);
			for (size_t i = 0; i < in_offsets.size(); ++i)
			{
				size_t off = i ? in_offsets[i - 1] : 0;
				size_t len = in_offsets[i] - off - 1;
				if (len > n)
					throw Exception("String too long for type FixedString(" + toString(n) + ")",
						ErrorCodes::TOO_LARGE_STRING_SIZE);
				memcpy(&out_chars[i * n], &in_chars[off], len);
			}
			block.getByPosition(result).column = result_ptr;
		}
		else if (const auto column_fixed_string = typeid_cast<const ColumnFixedString *>(column.get()))
		{
			const auto src_n = column_fixed_string->getN();
			if (src_n > n)
				throw Exception{
					"String too long for type FixedString(" + toString(n) + ")",
					ErrorCodes::TOO_LARGE_STRING_SIZE
				};

			const auto column_fixed = new ColumnFixedString{n};
			block.getByPosition(result).column = column_fixed;

			auto & out_chars = column_fixed->getChars();
			const auto & in_chars = column_fixed_string->getChars();
			const auto size = column_fixed_string->size();
			out_chars.resize_fill(size * n);

			for (const auto i : ext::range(0, size))
				memcpy(&out_chars[i * n], &in_chars[i * src_n], src_n);
		}
		else
			throw Exception("Unexpected column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	template <typename T>
	bool getSizeTyped(const ColumnWithNameAndType & column, size_t & out_size)
	{
		if (!typeid_cast<const typename DataTypeFromFieldType<T>::Type *>(&*column.type))
			return false;
		const ColumnConst<T> * column_const = typeid_cast<const ColumnConst<T> *>(&*column.column);
		if (!column_const)
			throw Exception("Unexpected type of column for FixedString length: " + column.column->getName(), ErrorCodes::ILLEGAL_COLUMN);
		T s = column_const->getData();
		if (s <= 0)
			throw Exception("FixedString length must be positive (unlike " + toString(s) + ")", ErrorCodes::ILLEGAL_COLUMN);
		out_size = static_cast<size_t>(s);
		return true;
	}

	size_t getSize(const ColumnWithNameAndType & column)
	{
		size_t res;
		if (getSizeTyped<UInt8>(column, res) ||
			getSizeTyped<UInt16>(column, res) ||
			getSizeTyped<UInt32>(column, res) ||
			getSizeTyped<UInt64>(column, res) ||
			getSizeTyped< Int8 >(column, res) ||
			getSizeTyped< Int16>(column, res) ||
			getSizeTyped< Int32>(column, res) ||
			getSizeTyped< Int64>(column, res))
			return res;
		throw Exception("Length of FixedString must be integer; got " + column.type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


struct NameToUInt8 			{ static constexpr auto name = "toUInt8"; };
struct NameToUInt16 		{ static constexpr auto name = "toUInt16"; };
struct NameToUInt32 		{ static constexpr auto name = "toUInt32"; };
struct NameToUInt64 		{ static constexpr auto name = "toUInt64"; };
struct NameToInt8 			{ static constexpr auto name = "toInt8"; };
struct NameToInt16	 		{ static constexpr auto name = "toInt16"; };
struct NameToInt32			{ static constexpr auto name = "toInt32"; };
struct NameToInt64			{ static constexpr auto name = "toInt64"; };
struct NameToFloat32		{ static constexpr auto name = "toFloat32"; };
struct NameToFloat64		{ static constexpr auto name = "toFloat64"; };
struct NameToDate			{ static constexpr auto name = "toDate"; };
struct NameToDateTime		{ static constexpr auto name = "toDateTime"; };
struct NameToString			{ static constexpr auto name = "toString"; };

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
typedef FunctionConvert<DataTypeDate,		NameToDate> 		FunctionToDate;
typedef FunctionConvert<DataTypeDateTime,	NameToDateTime> 	FunctionToDateTime;
typedef FunctionConvert<DataTypeString,		NameToString> 		FunctionToString;


}
