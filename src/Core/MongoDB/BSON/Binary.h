#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Base64Encoder.h>
#include <Poco/Buffer.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/UUID.h>
#include "Element.h"


namespace DB
{
namespace BSON
{


class Binary
/// Implements BSON Binary.
{
public:
    using Ptr = Poco::SharedPtr<Binary>;

    Binary();
    /// Creates an empty Binary with subtype 0.

    Binary(Int32 size, unsigned char subtype_);
    /// Creates a Binary with a buffer of the given size and the given subtype.

    Binary(const Poco::UUID & uuid);
    /// Creates a Binary containing an UUID.

    Binary(const std::string & data, unsigned char subtype_ = 0);
    /// Creates a Binary with the contents of the given string and the given subtype.

    Binary(const void * data, Int32 size, unsigned char subtype_ = 0);
    /// Creates a Binary with the contents of the given buffer and the given subtype.

    virtual ~Binary();
    /// Destroys the Binary.

    Poco::Buffer<char> & getBuffer();
    /// Returns a reference to the internal buffer


    const Poco::Buffer<char> & getBuffer() const;
    /// Returns a const reference to the internal buffer

    unsigned char getSubtype() const;
    /// Returns the subtype.

    void setSubtype(unsigned char type);
    /// Sets the subtype.

    std::string toString() const;
    /// Returns the contents of the Binary as Base64-encoded string.

    std::string toRawString() const;
    /// Returns the raw content of the Binary as a string.

    Poco::UUID uuid() const;
    /// Returns the UUID when the binary subtype is 0x04.
    /// Otherwise, throws a Poco::BadCastException.

private:
    Poco::Buffer<char> buffer;
    unsigned char subtype;
};


//
// inlines
//
inline unsigned char Binary::getSubtype() const
{
    return subtype;
}


inline void Binary::setSubtype(unsigned char type)
{
    subtype = type;
}


inline Poco::Buffer<char> & Binary::getBuffer()
{
    return buffer;
}

inline const Poco::Buffer<char> & Binary::getBuffer() const
{
    return buffer;
}


inline std::string Binary::toRawString() const
{
    return std::string(reinterpret_cast<const char *>(buffer.begin()), buffer.size());
}


// BSON Embedded Document
// spec: binary
template <>
struct ElementTraits<Binary::Ptr>
{
    enum
    {
        TypeId = 0x05
    };

    static std::string toString(const Binary::Ptr & value) { return value.isNull() ? "" : value->toString(); }


    static Binary::Ptr fromString(const std::string & str)
    {
        throw Poco::NotImplementedException("Binary from string is not implemented, str: {}", str);
        return nullptr;
    }
};


template <>
inline Binary::Ptr BSONReader::read<Binary::Ptr>()
{
    Int32 size;
    readIntBinary(size, reader);
    char subtype;
    readChar(subtype, reader);
    Binary::Ptr binary = new Binary(size, subtype);
    reader.readStrict(static_cast<char *>(binary->getBuffer().begin()), size);
    return binary;
}


template <>
inline void BSONWriter::write<Binary::Ptr>(const Binary::Ptr & from)
{
    writeIntBinary(Int32(from->getBuffer().size()), writer);
    writeChar(from->getSubtype(), writer);
    writer.write(from->getBuffer().begin(), from->getBuffer().size());
}

template <>
inline Int32 BSONWriter::getLength<Binary::Ptr>(const Binary::Ptr & from)
{
    return static_cast<Int32>(sizeof(Int32) + sizeof(from->getSubtype()) + from->getBuffer().size());
}


}
}
