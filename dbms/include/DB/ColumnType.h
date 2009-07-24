#ifndef DBMS_COLUMN_TYPE_H
#define DBMS_COLUMN_TYPE_H

#include <ostream>
#include <sstream>

#include <boost/variant/apply_visitor.hpp>

#include <Poco/SharedPtr.h>
#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>
#include <Poco/NumberParser.h>
#include <Poco/NumberFormatter.h>
#include <Poco/RegularExpression.h>

#include <strconvert/escape_manip.h>

#include <DB/Field.h>
#include <DB/Exception.h>
#include <DB/ErrorCodes.h>
#include <DB/VarInt.h>


namespace DB
{


/** Метаданные типа для хранения (столбца).
  * Содержит методы для сериализации/десериализации.
  */
class IColumnType
{
public:
	/// Основное имя типа (например, BIGINT UNSIGNED).
	virtual std::string getName() const = 0;

	/// Бинарная сериализация - для сохранения на диск / в сеть и т. п.
	virtual void serializeBinary(const DB::Field & field, std::ostream & ostr) const = 0;
	virtual void deserializeBinary(DB::Field & field, std::istream & istr) const = 0;

	/** Текстовая сериализация - для вывода на экран / сохранения в текстовый файл и т. п.
	  * Считается, что разделители, а также escape-инг обязан делать вызывающий.
	  */
	virtual void serializeText(const DB::Field & field, std::ostream & ostr) const = 0;
	virtual void deserializeText(DB::Field & field, std::istream & istr) const = 0;

	virtual ~IColumnType() {}
};



/** Аналог BIGINT UNSIGNED, сериализуется в набор байт фиксированной длины */
class ColumnTypeUInt64 : public IColumnType
{
public:
	std::string getName() const { return "UInt64"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		Poco::BinaryWriter w(ostr);
		w << boost::get<UInt>(field);
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		Poco::BinaryReader r(istr);
		UInt x;
		r >> x;
		field = x;
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<UInt>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		UInt x;
		istr >> x;
		field = x;
	}
};


/** Аналог INT UNSIGNED, сериализуется в набор байт фиксированной длины */
class ColumnTypeUInt32 : public IColumnType
{
public:
	std::string getName() const { return "UInt32"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		Poco::BinaryWriter w(ostr);
		w << static_cast<Poco::UInt32>(boost::get<UInt>(field));
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		Poco::BinaryReader r(istr);
		Poco::UInt32 x;
		r >> x;
		field = UInt(x);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << static_cast<Poco::UInt32>(boost::get<UInt>(field));
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		Poco::UInt32 x;
		istr >> x;
		field = UInt(x);
	}
};


/** Аналог BIGINT, сериализуется в набор байт фиксированной длины */
class ColumnTypeInt64 : public IColumnType
{
public:
	std::string getName() const { return "Int64"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		Poco::BinaryWriter w(ostr);
		w << boost::get<Int>(field);
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		Poco::BinaryReader r(istr);
		Int x;
		r >> x;
		field = x;
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<Int>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		Int x;
		istr >> x;
		field = x;
	}
};


/** Аналог INT, сериализуется в набор байт фиксированной длины */
class ColumnTypeInt32 : public IColumnType
{
public:
	std::string getName() const { return "Int32"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		Poco::BinaryWriter w(ostr);
		w << static_cast<Poco::Int32>(boost::get<Int>(field));
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		Poco::BinaryReader r(istr);
		Poco::Int32 x;
		r >> x;
		field = Int(x);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << static_cast<Poco::Int32>(boost::get<Int>(field));
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		Poco::Int32 x;
		istr >> x;
		field = Int(x);
	}
};


/** Аналог BIGINT UNSIGNED, сериализуемый в набор байт переменной длины (до 9 байт) */
class ColumnTypeVarUInt : public IColumnType
{
public:
	std::string getName() const { return "VarUInt"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		writeVarUInt(boost::get<UInt>(field), ostr);
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		UInt x;
		readVarUInt(x, istr);
		field = x;
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<UInt>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		UInt x;
		istr >> x;
		field = x;
	}
};


/** Аналог BIGINT, сериализуемый в набор байт переменной длины (до 9 байт) */
class ColumnTypeVarInt : public IColumnType
{
public:
	std::string getName() const { return "VarInt"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		writeVarInt(boost::get<Int>(field), ostr);
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		Int x;
		readVarInt(x, istr);
		field = x;
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<Int>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		Int x;
		istr >> x;
		field = x;
	}
};


/** Строки (без явного указания кодировки) произвольной длины */
class ColumnTypeText : public IColumnType
{
public:
	std::string getName() const { return "Text"; }
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		const std::string & str = boost::get<String>(field);
		writeVarUInt(UInt(str.size()), ostr);
		ostr << str;
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		field = String("");
		String & str = boost::get<String>(field);
		UInt size;
		readVarUInt(size, istr);
		if (!istr.good())
			return;
		str.resize(size);
		/// непереносимо, но (действительно) быстрее
		istr.read(const_cast<char*>(str.data()), size);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<String>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		field = std::string("");
		istr >> boost::get<String>(field);
	}
};


/** Строки (без явного указания кодировки) фиксированной длины */
class ColumnTypeFixedText : public IColumnType
{
private:
	UInt size;
	
public:
	ColumnTypeFixedText(UInt size_)
		: size(size_)
	{
	}

	std::string getName() const
	{
		return "FixedText(" + Poco::NumberFormatter::format(size) + ")";
	}
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		const String & str = boost::get<String>(field);
		if (UInt(str.size()) != size)
			throw Exception("Incorrect size of value of type " + getName()
				+ ": " + Poco::NumberFormatter::format(str.size()), ErrorCodes::INCORRECT_SIZE_OF_VALUE);
				
		ostr << str;
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		field = String("");
		std::string & str = boost::get<String>(field);
		str.resize(size);
		/// непереносимо, но (действительно) быстрее
		istr.read(const_cast<char*>(str.data()), size);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		serializeBinary(field, ostr);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		field = String("");
		istr >> boost::get<String>(field);
	}
};


/** Массив заданного типа произвольной длины */
class ColumnTypeArray : public IColumnType
{
private:
	Poco::SharedPtr<IColumnType> nested_type;
	
public:
	ColumnTypeArray(Poco::SharedPtr<IColumnType> nested_type_)
		: nested_type(nested_type_)
	{
	}

	std::string getName() const
	{
		return "Array(" + nested_type->getName() + ")";
	}
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		const FieldVector & vec = boost::get<FieldVector>(field);
		writeVarUInt(UInt(vec.size()), ostr);
		for (UInt i(0); i < vec.size(); ++i)
			nested_type->serializeBinary(vec[i], ostr);
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		field = FieldVector();
		FieldVector & vec = boost::get<FieldVector>(field);
		UInt size;
		readVarUInt(size, istr);
		if (!istr.good())
			return;
		vec.resize(size);
		for (UInt i(0); i < size; ++i)
			nested_type->deserializeBinary(vec[i], istr);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		const FieldVector & vec = boost::get<FieldVector>(field);
		for (UInt i(0); i < vec.size(); ++i)
		{
			std::stringstream stream;
			nested_type->serializeText(vec[i], stream);
			ostr << strconvert::quote_fast << stream.str();
			if (i + 1 != vec.size())
				ostr << ", ";
		}
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		// TODO
		throw Exception("Method ColumnTypeArray::deserializeText not implemented", ErrorCodes::METHOD_NOT_IMPLEMENTED);
	}
};


/** Массив заданного типа фиксированной длины */
class ColumnTypeFixedArray : public IColumnType
{
private:
	Poco::SharedPtr<IColumnType> nested_type;
	UInt size;
	
public:
	ColumnTypeFixedArray(Poco::SharedPtr<IColumnType> nested_type_, UInt size_)
		: nested_type(nested_type_), size(size_)
	{
	}

	std::string getName() const
	{
		return "Array(" + nested_type->getName() + ", " + Poco::NumberFormatter::format(size) + ")";
	}
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		const FieldVector & vec = boost::get<FieldVector>(field);

		if (UInt(vec.size()) != size)
			throw Exception("Incorrect size of value of type " + getName()
				+ ": " + Poco::NumberFormatter::format(vec.size()), ErrorCodes::INCORRECT_SIZE_OF_VALUE);
				
		for (UInt i(0); i < size; ++i)
			nested_type->serializeBinary(vec[i], ostr);
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		field = FieldVector();
		FieldVector & vec = boost::get<FieldVector>(field);
		vec.resize(size);
		for (UInt i(0); i < size; ++i)
			nested_type->deserializeBinary(vec[i], istr);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		const FieldVector & vec = boost::get<FieldVector>(field);
		for (UInt i(0); i < size; ++i)
		{
			std::stringstream stream;
			nested_type->serializeText(vec[i], stream);
			ostr << strconvert::quote_fast << stream.str();
			if (i + 1 != size)
				ostr << ", ";
		}
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		// TODO
		throw Exception("Method ColumnTypeFixedArray::deserializeText not implemented", ErrorCodes::METHOD_NOT_IMPLEMENTED);
	}
};


/** Типа с дополнительным значением NULL */
class ColumnTypeNullable : public IColumnType
{
private:
	Poco::SharedPtr<IColumnType> nested_type;
	
public:
	ColumnTypeNullable(Poco::SharedPtr<IColumnType> nested_type_)
		: nested_type(nested_type_)
	{
	}

	std::string getName() const
	{
		return "Nullable(" + nested_type->getName() + ")";
	}
	
	void serializeBinary(const DB::Field & field, std::ostream & ostr) const
	{
		if (boost::apply_visitor(FieldVisitorIsNull(), field))
			ostr.put(1);
		else
		{
			ostr.put(0);
			nested_type->serializeBinary(field, ostr);
		}
	}

	void deserializeBinary(DB::Field & field, std::istream & istr) const
	{
		bool is_null = istr.get();
		if (is_null)
			field = Null();
		else
			nested_type->deserializeBinary(field, istr);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		if (boost::apply_visitor(FieldVisitorIsNull(), field))
			ostr << "NULL";
		else
		{
			nested_type->serializeText(field, ostr);	/// пока не взаимнооднозначно
		}
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		// TODO
		throw Exception("Method ColumnTypeNullable::deserializeText not implemented", ErrorCodes::METHOD_NOT_IMPLEMENTED);
	}
};


class ColumnTypeFactory
{
public:
	static Poco::SharedPtr<IColumnType> get(const std::string & name)
	{
		if (name == "VarUInt")
			return new ColumnTypeVarUInt;
		if (name == "VarInt")
			return new ColumnTypeVarInt;
		if (name == "UInt32")
			return new ColumnTypeUInt32;
		if (name == "UInt64")
			return new ColumnTypeUInt64;
		if (name == "Int32")
			return new ColumnTypeUInt32;
		if (name == "Int64")
			return new ColumnTypeUInt64;
		if (name == "Text")
			return new ColumnTypeText;

		/// параметризованные типы
		static Poco::RegularExpression one_parameter_regexp("^([^\\(]+)\\((.+)\\)$");
		static Poco::RegularExpression two_parameters_regexp("^([^\\(]+)\\((.+),\\s*(.+)\\)$");

		Poco::RegularExpression::MatchVec matches;

		if (two_parameters_regexp.match(name, 0, matches))
		{
			/// FixedArray(T, N), где T - любой тип, а N - размер
			if (name.substr(matches[0].offset, matches[0].length) == "FixedArray")
			{
				UInt size;
				if (!Poco::NumberParser::tryParseUnsigned64(name.substr(matches[2].offset, matches[2].length), size))
					throw Exception("Incorrect parameter '"
						+ name.substr(matches[2].offset, matches[2].length)
						+ "'for type FixedArray", ErrorCodes::INCORRECT_PARAMETER_FOR_TYPE);

				return new ColumnTypeFixedArray(
					get(name.substr(matches[1].offset, matches[1].length)),
					size);
			}
		}
		
		if (one_parameter_regexp.match(name, 0, matches))
		{
			/// FixedText(N), где N - размер
			if (name.substr(matches[0].offset, matches[0].length) == "FixedText")
			{
				UInt size;
				if (!Poco::NumberParser::tryParseUnsigned64(name.substr(matches[1].offset, matches[1].length), size))
					throw Exception("Incorrect parameter '"
						+ name.substr(matches[1].offset, matches[1].length)
						+ "'for type FixedText", ErrorCodes::INCORRECT_PARAMETER_FOR_TYPE);

				return new ColumnTypeFixedText(size);
			}

			/// Array(T), где T - любой тип
			if (name.substr(matches[0].offset, matches[0].length) == "Array")
				return new ColumnTypeArray(get(name.substr(matches[1].offset, matches[1].length)));
			
			/// Nullable(T), где T - любой тип
			if (name.substr(matches[0].offset, matches[0].length) == "Nullable")
				return new ColumnTypeNullable(get(name.substr(matches[1].offset, matches[1].length)));
		}
		
		/// неизвестный тип
		throw Exception("Unknown column type " + name, ErrorCodes::UNKNOWN_COLUMN_TYPE);
	}
};


}

#endif
