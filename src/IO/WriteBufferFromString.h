#pragma once

#include <string>
#include <IO/WriteBufferFromVector.h>
#include <base/StringRef.h>


namespace DB
{

/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object.
  */
using WriteBufferFromString = WriteBufferFromVector<std::string>;


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
class WriteBufferFromOwnString : public detail::StringHolder, public WriteBufferFromString
{
public:
    WriteBufferFromOwnString() : WriteBufferFromString(value) {}

    StringRef stringRef() const { return isFinished() ? StringRef(value) : StringRef(value.data(), pos - value.data()); }

    std::string & str()
    {
        finalize();
        return value;
    }
};

}
