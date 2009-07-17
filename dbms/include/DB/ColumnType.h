#ifndef DBMS_COLUMN_TYPE_H
#define DBMS_COLUMN_TYPE_H

#include <ostream>

#include <Poco/SharedPtr.h>
#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>
#include <Poco/NumberParser.h>
#include <Poco/NumberFormatter.h>

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
	virtual void deserializeBinary(DB::Field & field, std::istream & ostr) const = 0;

	/// Текстовая сериализация - для вывода на экран / сохранения в текстовый файл и т. п.
	virtual void serializeText(const DB::Field & field, std::ostream & ostr) const = 0;
	virtual void deserializeText(DB::Field & field, std::istream & ostr) const = 0;

	/// Шаблонные методы для параметризуемого типа сериализации.
	template <typename SerializationTag> void serialize(const DB::Field & field, std::ostream & ostr) const;
	template <typename SerializationTag> void deserialize(DB::Field & field, std::istream & ostr) const;
};


struct BinarySerializationTag;
struct TextSerializationTag;

template <typename SerializationTag> struct SerializeImpl;

template <typename SerializationTag>
void IColumnType::serialize(const DB::Field & field, std::ostream & ostr) const
{
	SerializeImpl<SerializationTag>::serialize(this, field, ostr);
}

template <typename SerializationTag>
void IColumnType::deserialize(DB::Field & field, std::istream & ostr) const
{
	SerializeImpl<SerializationTag>::deserialize(this, field, ostr);
}


template <> struct SerializeImpl<BinarySerializationTag>
{
	static inline void serialize(const IColumnType * column, const DB::Field & field, std::ostream & ostr)
	{
		column->serializeBinary(field, ostr);
	}

	static inline void deserialize(const IColumnType * column, DB::Field & field, std::istream & ostr)
	{
		column->deserializeBinary(field, ostr);
	}
};

template <> struct SerializeImpl<TextSerializationTag>
{
	static inline void serialize(const IColumnType * column, const DB::Field & field, std::ostream & ostr)
	{
		column->serializeText(field, ostr);
	}

	static inline void deserialize(const IColumnType * column, DB::Field & field, std::istream & ostr)
	{
		column->deserializeText(field, ostr);
	}
};


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
		r >> boost::get<UInt>(field);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<UInt>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		istr >> boost::get<UInt>(field);
	}
};


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
		boost::get<UInt>(field) = x;
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << static_cast<Poco::UInt32>(boost::get<UInt>(field));
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		Poco::UInt32 x;
		istr >> x;
		boost::get<UInt>(field) = x;
	}
};


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
		r >> boost::get<Int>(field);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<Int>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		istr >> boost::get<Int>(field);
	}
};


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
		boost::get<Int>(field) = x;
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << static_cast<Poco::Int32>(boost::get<Int>(field));
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		Poco::Int32 x;
		istr >> x;
		boost::get<Int>(field) = x;
	}
};


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
		readVarUInt(boost::get<UInt>(field), istr);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<UInt>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		istr >> boost::get<UInt>(field);
	}
};


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
		readVarInt(boost::get<Int>(field), istr);
	}

	void serializeText(const DB::Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<Int>(field);
	}

	void deserializeText(DB::Field & field, std::istream & istr) const
	{
		istr >> boost::get<Int>(field);
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

		throw Exception("Unknown column type " + name, ErrorCodes::UNKNOWN_COLUMN_TYPE);
	}
};


}

#endif
