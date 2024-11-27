#pragma once
#include <iomanip>
#include <list>
#include <sstream>
#include <string>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include "Poco/Nullable.h"
#include <Poco/DateTimeFormatter.h>
#include <Poco/DateTimeParser.h>
#include <Poco/NumberFormatter.h>
#include <Poco/SharedPtr.h>
#include <Common/logger_useful.h>
#include "BSONReader.h"
#include "BSONWriter.h"

namespace DB
{
namespace BSON
{

template <typename T>
class ConcreteElement;

class Element
/// Represents an Element of a Document or an Array.
{
public:
    using Key = std::string;
    using Ptr = Poco::SharedPtr<Element>;

    Element(const Key & name_) : name(name_) { }
    /// Creates the Element with the given name.

    virtual ~Element();
    /// Destructor

    const Key & getName() const;
    /// Returns the name of the element.

    virtual std::string toString() const = 0;
    /// Returns a string representation of the element.

    virtual int getType() const = 0;
    /// Returns the MongoDB type of the element.

    template <typename T>
    static Poco::SharedPtr<ConcreteElement<T>> cast(Ptr ptr)
    {
        return ptr.cast<ConcreteElement<T>>();
    }

    static Element::Ptr fromTypeId(UInt8 type, const std::string & name);
    static UInt8 typeIdFromString(const std::string & type);
    static Element::Ptr createElementWithType(const std::string & type, const std::string & name, const std::string & value);

    virtual void read(ReadBuffer & reader) = 0;
    virtual void write(WriteBuffer & writer) const = 0;
    virtual void valueFromString(const std::string & string) = 0;

    virtual Int32 getLength() const = 0;

protected:
    Key name;
};


inline const Element::Key & Element::getName() const
{
    return this->name;
}

using ElementSet = std::vector<Element::Ptr>;


template <typename T>
struct ElementTraits
{
    static T fromString(const std::string & str);
};

template <typename T>
class ConcreteElement : public Element
{
public:
    using Key = Element::Key;
    ConcreteElement(const Key & name_, const T & value_) : Element(name_), value(value_) { }

    ~ConcreteElement() override = default;


    T & getValue() { return value; }

    std::string toString() const override { return ElementTraits<T>::toString(value); }

    int getType() const override { return ElementTraits<T>::TypeId; }

    void read(ReadBuffer & reader) override { value = BSONReader(reader).read<T>(); }

    std::pair<std::string, T> deconstruct()
    {
        std::pair<std::string, T> pair;
        pair.first = std::move(name);
        pair.second = std::move(value);
        return pair;
    }

    Int32 getLength() const override
    {
        Int32 length = sizeof(unsigned char);
        length += name.length() + sizeof('\0');
        return length + BSONWriter::getLength<T>(value);
    }

    void valueFromString(const std::string & str) override { value = ElementTraits<T>::fromString(str); }

    void write(WriteBuffer & writer) const override
    {
        writer.write(static_cast<unsigned char>(this->getType()));
        writeNullTerminatedString(this->name, writer);
        BSONWriter(writer).write<T>(value);
    }

private:
    T value;
};


template <>
struct ElementTraits<std::string>
{
    enum
    {
        TypeId = 0x02
    };

    static std::string toString(const std::string & value)
    {
        WriteBufferFromOwnString writer;

        writeChar('\'', writer);

        for (char it : value)
        {
            switch (it)
            {
                case '"':
                    writeText("\\\"", writer);
                    break;
                case '\\':
                    writeText("\\\\", writer);
                    break;
                case '\b':
                    writeText("\\b", writer);
                    break;
                case '\f':
                    writeText("\\f", writer);
                    break;
                case '\n':
                    writeText("\\n", writer);
                    break;
                case '\r':
                    writeText("\\r", writer);
                    break;
                case '\t':
                    writeText("\\t", writer);
                    break;
                default: {
                    if (it > 0 && it <= 0x1F)
                        writeText(fmt::format("\\u{}", static_cast<int>(it)), writer); // FIXME make string like one line lower
                    // oss << "\\u" << std::hex << std::uppercase << std::setfill('0') << std::setw(4) << static_cast<int>(it);
                    else
                        writeChar(it, writer);
                    break;
                }
            }
        }
        writeChar('\'', writer);
        return writer.str();
    }

    static std::string fromString(const std::string & str) { return str; }
};


// BSON Floating point
// spec: double
template <>
struct ElementTraits<double>
{
    enum
    {
        TypeId = 0x01
    };

    static std::string toString(const double & value) { return Poco::NumberFormatter::format(value); }


    static double fromString(const std::string & str) { return std::stod(str); }
};
template <>
inline double BSONReader::read<double>()
{
    double value;
    readFloatBinary(value, reader);
    return value;
}

template <>
inline void BSONWriter::write<double>(const double & t)
{
    writeFloatBinary(t, writer);
}

template <>
inline Int32 BSONWriter::getLength<double>(const double & from)
{
    return static_cast<Int32>(sizeof(from));
}


// BSON UTF-8 string
// spec: int32 (byte*) "\x00"
// int32 is the number bytes in byte* + 1 (for trailing "\x00")


template <>
inline std::string BSONReader::read<std::string>()
{
    Int32 size;
    readIntBinary(size, reader);
    std::string result;
    readNullTerminated(result, reader);
    return result;
}


template <>
inline void BSONWriter::write<std::string>(const std::string & t)
{
    writeIntBinary(static_cast<Int32>(t.length() + 1), writer);
    writeNullTerminatedString(t, writer);
}

template <>
inline Int32 BSONWriter::getLength<std::string>(const std::string & t)
{
    return static_cast<Int32>(sizeof(Int32) + t.length() + sizeof('\0'));
}


// BSON bool
// spec: "\x00" "\x01"
template <>
struct ElementTraits<bool>
{
    enum
    {
        TypeId = 0x08
    };

    static std::string toString(const bool & value) { return value ? "true" : "false"; }

    static bool fromString(const std::string & str)
    {
        if (str == "True")
            return 1;
        else if (str == "False")
            return 0;
        else
            throw std::runtime_error(fmt::format("Cannot parse boolean from {}", str));
    }
};


template <>
inline bool BSONReader::read<bool>()
{
    char b;
    if (!reader.read(b))
        throw std::exception(); // FIXME

    return b != 0;
}


template <>
inline void BSONWriter::write<bool>(const bool & t)
{
    unsigned char b = t ? 0x01 : 0x00;
    writer.write(b);
}

template <>
inline Int32 BSONWriter::getLength<bool>(const bool & from)
{
    return static_cast<Int32>(sizeof(from));
}

// BSON 32-bit integer
// spec: int32
template <>
struct ElementTraits<Int32>
{
    enum
    {
        TypeId = 0x10
    };


    static std::string toString(const Int32 & value) { return Poco::NumberFormatter::format(value); }

    static Int32 fromString(const std::string & str) { return std::stoi(str); }
};

template <>
inline Int32 BSONReader::read<Int32>()
{
    Int32 value;
    readIntBinary(value, reader);
    return value;
}

template <>
inline void BSONWriter::write<Int32>(const Int32 & t)
{
    writeIntBinary(t, writer);
}

template <>
inline Int32 BSONWriter::getLength<Int32>(const Int32 & from)
{
    return static_cast<Int32>(sizeof(from));
}

// BSON 64-bit integer
// spec: int64
template <>
struct ElementTraits<Int64>
{
    enum
    {
        TypeId = 0x12
    };

    static std::string toString(const Int64 & value) { return Poco::NumberFormatter::format(value); }

    static Int64 fromString(const std::string & str) { return std::stoll(str); }
};

template <>
inline Int64 BSONReader::read<Int64>()
{
    Int64 value;
    readIntBinary(value, reader);
    return value;
}

template <>
inline void BSONWriter::write<Int64>(const Int64 & t)
{
    writeIntBinary(t, writer);
}


template <>
inline Int32 BSONWriter::getLength<Int64>(const Int64 & from)
{
    return static_cast<Int32>(sizeof(from));
}


// BSON UTC datetime
// spec: int64
template <>
struct ElementTraits<Poco::Timestamp>
{
    enum
    {
        TypeId = 0x09
    };

    static std::string toString(const Poco::Timestamp & value)
    {
        std::string result;
        result.append(1, '\'');
        result.append(Poco::DateTimeFormatter::format(value, "%Y-%m-%d %H:%M:%s"));
        result.append(1, '\'');
        return result;
    }

    static Poco::Timestamp fromString(const std::string & str)
    {
        int diff;
        return Poco::DateTimeParser::parse(str, diff).timestamp();
    }
};


template <>
inline Poco::Timestamp BSONReader::read<Poco::Timestamp>()
{
    Int64 value;
    readIntBinary(value, reader);
    auto to = Poco::Timestamp::fromEpochTime(static_cast<std::time_t>(value / 1000));
    to += (value % 1000 * 1000);
    return to;
}


template <>
inline void BSONWriter::write<Poco::Timestamp>(const Poco::Timestamp & t)
{
    writeIntBinary(t.epochMicroseconds() / 1000, writer);
}

template <>
inline Int32 BSONWriter::getLength<Poco::Timestamp>(const Poco::Timestamp & t)
{
    return static_cast<Int32>(sizeof(t.epochMicroseconds() / 1000));
}

using NullValue = Poco::Nullable<unsigned char>;


// BSON Null Value
// spec:
template <>
struct ElementTraits<NullValue>
{
    enum
    {
        TypeId = 0x0A
    };

    static std::string toString(const NullValue &) { return "null"; }

    static NullValue fromString(const std::string &) { return NullValue(); }
};


template <>
inline NullValue BSONReader::read<NullValue>()
{
    return NullValue();
}


template <>
inline void BSONWriter::write<NullValue>(const NullValue &)
{
}

template <>
inline Int32 BSONWriter::getLength<NullValue>(const NullValue &)
{
    return 0;
}


}
}
