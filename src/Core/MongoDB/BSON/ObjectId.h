#pragma once

#include <string.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Timestamp.h>
#include "Element.h"
namespace DB
{
namespace BSON
{


class ObjectId
/// ObjectId is a 12-byte BSON type, constructed using:
///
///   - a 4-byte timestamp,
///   - a 3-byte machine identifier,
///   - a 2-byte process id, and
///   - a 3-byte counter, starting with a random value.
///
/// In MongoDB, documents stored in a collection require a unique _id field that acts
/// as a primary key. Because ObjectIds are small, most likely unique, and fast to generate,
/// MongoDB uses ObjectIds as the default value for the _id field if the _id field is not
/// specified; i.e., the mongod adds the _id field and generates a unique ObjectId to assign
/// as its value.
{
public:
    static constexpr int ID_LEN = 12;
    using Ptr = Poco::SharedPtr<ObjectId>;

    ObjectId();

    explicit ObjectId(const std::string & id_);
    /// Creates an ObjectId from a string.
    ///
    /// The string must contain a hexadecimal representation
    /// of an object ID. This means a string of 24 characters.

    ObjectId(const ObjectId & copy);
    /// Creates an ObjectId by copying another one.

    virtual ~ObjectId();
    /// Destroys the ObjectId.

    const std::string & getId() const;


private:
    static int fromHex(char c);
    static char fromHex(const std::string & c);

    std::string id;

    friend class BSONWriter;
    friend class BSONReader;
    friend class Document;
};


//
// inlines
//
inline int ObjectId::fromHex(char c)
{
    if ('0' <= c && c <= '9')
        return c - '0';
    if ('a' <= c && c <= 'f')
        return c - 'a' + 10;
    if ('A' <= c && c <= 'F')
        return c - 'A' + 10;
    return 0xff;
}

inline const std::string & ObjectId::getId() const
{
    return id;
}


inline char ObjectId::fromHex(const std::string & c)
{
    return static_cast<char>(((fromHex(c[0]) << 4) | fromHex(c[1])));
}


// BSON Embedded Document
// spec: ObjectId
template <>
struct ElementTraits<ObjectId::Ptr>
{
    enum
    {
        TypeId = 0x07
    };

    static std::string toString(const ObjectId::Ptr & from) { return from->getId(); }

    static ObjectId::Ptr fromString(const std::string & str)
    {
        throw Poco::NotImplementedException("Not implemented fromString for ObjectId, str: {}", str);
    }
};


template <>
inline ObjectId::Ptr BSONReader::read<ObjectId::Ptr>()
{
    ObjectId::Ptr obj = new ObjectId();
    reader.readStrict(obj->id.data(), ObjectId::ID_LEN);
    return obj;
}


template <>
inline void BSONWriter::write<ObjectId::Ptr>(const ObjectId::Ptr & from)
{
    std::string id = from->id;
    writer.write(id.data(), ObjectId::ID_LEN);
}

template <>
inline Int32 BSONWriter::getLength<ObjectId::Ptr>(const ObjectId::Ptr &)
{
    return static_cast<Int32>(ObjectId::ID_LEN);
}


}
}
