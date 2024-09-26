#pragma once

#include <string>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteBuffer.h>
#include <base/StringRef.h>


namespace DB
{

/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object or call finalize.
  */
using WriteBufferFromString = AutoFinalizedWriteBuffer<WriteBufferFromVectorImpl<std::string>>;


namespace detail
{
    /// For correct order of initialization.
    class StringHolder
    {
    protected:
        std::string value;
    };
}

/// Creates the string by itself and allows to get it.
class WriteBufferFromOwnStringImpl : public detail::StringHolder, public WriteBufferFromVectorImpl<std::string>
{
    using Base = WriteBufferFromVectorImpl<std::string>;
public:
    WriteBufferFromOwnStringImpl() : Base(value) {}

    std::string_view stringView() const { return isFinalized() ? std::string_view(value) : std::string_view(value.data(), pos - value.data()); }

    std::string & str()
    {
        finalize();
        return value;
    }
};

using WriteBufferFromOwnString = AutoFinalizedWriteBuffer<WriteBufferFromOwnStringImpl>;

}
