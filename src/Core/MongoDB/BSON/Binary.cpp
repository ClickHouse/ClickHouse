#include "Binary.h"
#include <boost/beast/core/detail/base64.hpp>

namespace DB
{
namespace BSON
{


Binary::Binary() : buffer(0), subtype(0)
{
}


Binary::Binary(Poco::Int32 size, unsigned char subtype_) : buffer(size), subtype(subtype_)
{
}


Binary::Binary(const Poco::UUID & uuid) : buffer(128 / 8), subtype(0x04)
{
    char szUUID[16];
    uuid.copyTo(szUUID);
    buffer.assign(szUUID, 16);
}


Binary::Binary(const std::string & data, unsigned char subtype_) : buffer(data.data(), data.size()), subtype(subtype_)
{
}


Binary::Binary(const void * data, Int32 size, unsigned char subtype_) : buffer(static_cast<const char *>(data), size), subtype(subtype_)
{
}


Binary::~Binary()
{
}


std::string Binary::toString() const
{
    size_t encoded_size_ = encoded_size(buffer.size());
    std::string result(encoded_size);
    assert(encode(result.data(), reinterpret_cast<const char *>(buffer.begin()), buffer.size()) == encoded_size_);
    return result;
}


Poco::UUID Binary::uuid() const
{
    if (subtype == 0x04 && buffer.size() == 16)
    {
        Poco::UUID uuid;
        uuid.copyFrom(reinterpret_cast<const char *>(buffer.begin()));
        return uuid;
    }
    throw Poco::BadCastException("Invalid subtype");
}


}
}
