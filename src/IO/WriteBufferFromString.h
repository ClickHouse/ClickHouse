#pragma once

#include <IO/WriteBufferFromVector.h>

#include <string>

namespace DB
{

/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object or call finalize.
  */
class WriteBufferFromString final : public WriteBufferFromVectorImpl<std::string>
{
    using Base = WriteBufferFromVectorImpl;
public:
    explicit WriteBufferFromString(std::string & vector_)
        : Base(vector_)
    {
    }

    WriteBufferFromString(std::string & vector_, AppendModeTag tag_)
        : Base(vector_, tag_)
    {
    }
    ~WriteBufferFromString() override;
};

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
class WriteBufferFromOwnString : public detail::StringHolder, public WriteBufferFromVectorImpl<std::string>
{
    using Base = WriteBufferFromVectorImpl<std::string>;
public:
    WriteBufferFromOwnString()
        : Base(value)
    {
    }
    ~WriteBufferFromOwnString() override;

    std::string_view stringView() const { return isFinalized() ? std::string_view(value) : std::string_view(value.data(), pos - value.data()); }

    std::string & str()
    {
        finalize();
        return value;
    }
};
}
