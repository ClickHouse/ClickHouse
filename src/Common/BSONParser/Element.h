#pragma once
#include "BSONReader.h"
#include "BSONWriter.h"
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Poco/SharedPtr.h>
#include <Poco/NumberFormatter.h>
#include <Poco/DateTimeFormatter.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#include <iomanip>
#include <list>
#include <sstream>
#include <string>

namespace DB
{
namespace BSON
{


class Element
/// Represents an Element of a Document or an Array.
{
public:
    using Key = std::string;
    using Ptr = Poco::SharedPtr<Element>;

    Element(const Key & name_) : name(name_) {}
    /// Creates the Element with the given name.

    virtual ~Element();
    /// Destructor

    const Key & getName() const;
    /// Returns the name of the element.

    virtual std::string toString() const = 0;
    /// Returns a string representation of the element.

    virtual int type() const = 0;
    /// Returns the MongoDB type of the element.

    virtual void read(ReadBuffer & reader) = 0;
    virtual void write(WriteBuffer & writer) const = 0;

    virtual Int32 getLength() const = 0;

protected:
    Key name;
};

Element::~Element() = default;


inline const Element::Key & Element::getName() const
{
    return this->name;
}


using ElementSet = std::list<Element::Ptr>;


template <typename T>
struct ElementTraits
{
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

    int type() const override { return ElementTraits<T>::TypeId; }

    void read(ReadBuffer & reader) override { value = BSONReader(reader).read<T>(); }

    Int32 getLength() const override {
        Int32 length = sizeof(unsigned char);
        length += name.length() + sizeof('\0');
        return length + BSONWriter::getLength<T>(value);
    }

    void write(WriteBuffer & writer) const override {
		writer.write(static_cast<unsigned char>(this->type()));
		writeNullTerminatedString(this->name, writer);
        BSONWriter(writer).write<T>(value);
    }

private:
    T value;
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
};

template<>
double BSONReader::read<double>() {
    double value;
    readFloatBinary(value, reader);
    return value;
}


template<>
void BSONWriter::write<double>(const double& t) {
    writeFloatBinary(t, writer);
}

// BSON UTF-8 string
// spec: int32 (byte*) "\x00"
// int32 is the number bytes in byte* + 1 (for trailing "\x00")
template <>
struct ElementTraits<std::string>
{
    enum
    {
        TypeId = 0x02
    };

    static std::string toString(const std::string & value)
    {
        std::ostringstream oss;

        oss << '"';

        for (char it : value)
        {
            switch (it)
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
                default: {
                    if (it > 0 && it <= 0x1F)
                        oss << "\\u" << std::hex << std::uppercase << std::setfill('0') << std::setw(4) << static_cast<int>(it);
                    else
                        oss << it;
                    break;
                }
            }
        }
        oss << '"';
        return oss.str();
    }
};

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
Int32 BSONWriter::getLength<std::string>(const std::string & t)
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
};


template <>
inline bool BSONReader::read<bool>()
{
    char b;
    if (!reader.read(b)) {
        throw std::exception(); // FIXME
    }
    return b != 0;
}


template <>
inline void BSONWriter::write<bool>(const bool & t)
{
    unsigned char b = t ? 0x01 : 0x00;
    writer.write(b);
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
};

template<>
inline Int32 BSONReader::read<Int32>() {
    Int32 value;
    readIntBinary(value, reader);
    return value;
}

template<>
inline void BSONWriter::write<Int32>(const Int32& t) {
    writeIntBinary(t, writer);
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
};

template<>
inline Int64 BSONReader::read<Int64>() {
    Int64 value;
    readIntBinary(value, reader);
    return value;
}

template<>
inline void BSONWriter::write<Int64>(const Int64& t) {
    writeIntBinary(t, writer);
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
        result.append(1, '"');
        result.append(Poco::DateTimeFormatter::format(value, "%Y-%m-%dT%H:%M:%s%z"));
        result.append(1, '"');
        return result;
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
inline void BSONWriter::write<Poco::Timestamp>(const Poco::Timestamp& t)
{
    writeIntBinary(t.epochMicroseconds() / 1000, writer);
}


}} // namespace DB::BSON
