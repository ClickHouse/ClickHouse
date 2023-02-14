//
// Element.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  Element
//
// Definition of the Element class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_Element_INCLUDED
#define MongoDB_Element_INCLUDED


#include "Poco/BinaryReader.h"
#include "Poco/BinaryWriter.h"
#include "Poco/SharedPtr.h"
#include "Poco/Timestamp.h"
#include "Poco/Nullable.h"
#include "Poco/NumberFormatter.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/UTF8String.h"
#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/BSONReader.h"
#include "Poco/MongoDB/BSONWriter.h"
#include <string>
#include <sstream>
#include <iomanip>
#include <list>


namespace Poco {
namespace MongoDB {


class MongoDB_API Element
	/// Represents an Element of a Document or an Array.
{
public:
	typedef Poco::SharedPtr<Element> Ptr;

	explicit Element(const std::string& name);
		/// Creates the Element with the given name.

	virtual ~Element();
		/// Destructor

	const std::string& name() const;
		/// Returns the name of the element.

	virtual std::string toString(int indent = 0) const = 0;
		/// Returns a string representation of the element.

	virtual int type() const = 0;
		/// Returns the MongoDB type of the element.

private:
	virtual void read(BinaryReader& reader) = 0;
	virtual void write(BinaryWriter& writer) = 0;

	friend class Document;
	std::string _name;
};


//
// inlines
//
inline const std::string& Element::name() const
{
	return _name;
}


typedef std::list<Element::Ptr> ElementSet;


template<typename T> 
struct ElementTraits
{
};


// BSON Floating point
// spec: double
template<>
struct ElementTraits<double>
{
	enum { TypeId = 0x01 };

	static std::string toString(const double& value, int indent = 0)
	{
		return Poco::NumberFormatter::format(value);
	}
};


// BSON UTF-8 string
// spec: int32 (byte*) "\x00"
// int32 is the number bytes in byte* + 1 (for trailing "\x00")
template<>
struct ElementTraits<std::string>
{
	enum { TypeId = 0x02 };

	static std::string toString(const std::string& value, int indent = 0)
	{
		std::ostringstream oss;

		oss << '"';

		for (std::string::const_iterator it = value.begin(); it != value.end(); ++it)
		{
			switch (*it)
			{
			case '"':
				oss << "\\\"";
				break;
			case '\\':
				oss << "\\\\";
				break;
			case '\b':
				oss << "\\b";
				break;
			case '\f':
				oss << "\\f";
				break;
			case '\n':
				oss << "\\n";
				break;
			case '\r':
				oss << "\\r";
				break;
			case '\t':
				oss << "\\t";
				break;
			default:
				{
					if ( *it > 0 && *it <= 0x1F )
					{
						oss << "\\u" << std::hex << std::uppercase << std::setfill('0') << std::setw(4) << static_cast<int>(*it);
					}
					else
					{
						oss << *it;
					}
					break;
				}
			}
		}
		oss << '"';
		return oss.str();
	}
};


template<>
inline void BSONReader::read<std::string>(std::string& to)
{
	Poco::Int32 size;
	_reader >> size;
	_reader.readRaw(size, to);
	to.erase(to.end() - 1); // remove terminating 0
}


template<>
inline void BSONWriter::write<std::string>(std::string& from)
{
	_writer << (Poco::Int32) (from.length() + 1);
	writeCString(from);
}


// BSON bool
// spec: "\x00" "\x01"
template<>
struct ElementTraits<bool>
{
	enum { TypeId = 0x08 };

	static std::string toString(const bool& value, int indent = 0)
	{
		return value ? "true" : "false";
	}
};


template<>
inline void BSONReader::read<bool>(bool& to)
{
	unsigned char b;
	_reader >> b;
	to = b != 0;
}


template<>
inline void BSONWriter::write<bool>(bool& from)
{
	unsigned char b = from ? 0x01 : 0x00;
	_writer << b;
}


// BSON 32-bit integer
// spec: int32
template<>
struct ElementTraits<Int32>
{
	enum { TypeId = 0x10 };


	static std::string toString(const Int32& value, int indent = 0)
	{
		return Poco::NumberFormatter::format(value);
	}
};


// BSON UTC datetime
// spec: int64
template<>
struct ElementTraits<Timestamp>
{
	enum { TypeId = 0x09 };

	static std::string toString(const Timestamp& value, int indent = 0)
	{
		std::string result;
		result.append(1, '"');
		result.append(DateTimeFormatter::format(value, "%Y-%m-%dT%H:%M:%s%z"));
		result.append(1, '"');
		return result;
	}
};


template<>
inline void BSONReader::read<Timestamp>(Timestamp& to)
{
	Poco::Int64 value;
	_reader >> value;
	to = Timestamp::fromEpochTime(static_cast<std::time_t>(value / 1000));
	to += (value % 1000 * 1000);
}


template<>
inline void BSONWriter::write<Timestamp>(Timestamp& from)
{
	_writer << (from.epochMicroseconds() / 1000);
}


typedef Nullable<unsigned char> NullValue;


// BSON Null Value
// spec:
template<>
struct ElementTraits<NullValue>
{
	enum { TypeId = 0x0A };

	static std::string toString(const NullValue& value, int indent = 0)
	{
		return "null";
	}
};


template<>
inline void BSONReader::read<NullValue>(NullValue& to)
{
}


template<>
inline void BSONWriter::write<NullValue>(NullValue& from)
{
}


struct BSONTimestamp 
{
	Poco::Timestamp ts;
	Poco::Int32 inc;
};


// BSON Timestamp
// spec: int64
template<>
struct ElementTraits<BSONTimestamp>
{
	enum { TypeId = 0x11 };

	static std::string toString(const BSONTimestamp& value, int indent = 0)
	{
		std::string result;
		result.append(1, '"');
		result.append(DateTimeFormatter::format(value.ts, "%Y-%m-%dT%H:%M:%s%z"));
		result.append(1, ' ');
		result.append(NumberFormatter::format(value.inc));
		result.append(1, '"');
		return result;
	}
};


template<>
inline void BSONReader::read<BSONTimestamp>(BSONTimestamp& to)
{
	Poco::Int64 value;
	_reader >> value;
	to.inc = value & 0xffffffff;
	value >>= 32;
	to.ts = Timestamp::fromEpochTime(static_cast<std::time_t>(value));
}


template<>
inline void BSONWriter::write<BSONTimestamp>(BSONTimestamp& from)
{
	Poco::Int64 value = from.ts.epochMicroseconds() / 1000;
	value <<= 32;
	value += from.inc;
	_writer << value;
}


// BSON 64-bit integer
// spec: int64
template<>
struct ElementTraits<Int64>
{
	enum { TypeId = 0x12 };

	static std::string toString(const Int64& value, int indent = 0)
	{
		return NumberFormatter::format(value);
	}
};


template<typename T>
class ConcreteElement: public Element
{
public:
	ConcreteElement(const std::string& name, const T& init):
		Element(name), 
		_value(init)
	{
	}

	virtual ~ConcreteElement()
	{
	}

	
	T value() const
	{
		return _value;
	}


	std::string toString(int indent = 0) const
	{
		return ElementTraits<T>::toString(_value, indent);
	}

	
	int type() const
	{
		return ElementTraits<T>::TypeId;
	}

	void read(BinaryReader& reader)
	{
		BSONReader(reader).read(_value);
	}

	void write(BinaryWriter& writer)
	{
		BSONWriter(writer).write(_value);
	}

private:
	T _value;
};


} } // namespace Poco::MongoDB


#endif // MongoDB_Element_INCLUDED
