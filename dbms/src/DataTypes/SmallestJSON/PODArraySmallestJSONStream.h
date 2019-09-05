#pragma once

#include <DataTypes/SmallestJSON/SmallestJSONStreamFactory.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

template <typename PODArrayType, FormatStyle format>
struct PODArraySmallestJSONStream
{
public:
    typedef char Ch;

    PODArraySmallestJSONStream(PODArrayType & array_, const FormatSettings & settings_)
        : array(array_), settings(settings_), offset(0), limit(array.size())
    {}

    void SkipQuoted()
    {}

    void setOffsetAndLimit(size_t new_offset, size_t new_limit)
    {
        cursor = 0;
        limit = new_limit;
        offset = new_offset;

        if (offset + limit > array.size())
            throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
    }

    char Take()
    {
        if constexpr (!std::is_same<PODArrayType, const ColumnString::Chars>::value)
            throw Exception("Method Take is not supported for ColumnString::Chars", ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            if ((offset + cursor) >= array.size() || cursor >= limit)
                throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);

            return array[offset++];
        }
    }

    char Peek() const
    {
        if constexpr (!std::is_same<PODArrayType, const ColumnString::Chars>::value)
            throw Exception("Method Peek is not supported for ColumnString::Chars", ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            if ((offset + cursor) >= array.size() || cursor >= limit)
                throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);

            return array[offset];
        }
    }

    size_t Tell() const
    {
        if constexpr (!std::is_same<PODArrayType, const ColumnString::Chars>::value)
            throw Exception("Method Tell is not supported for ColumnString::Chars", ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            return cursor;
        }
    }

    void Flush()
    {
        /// do nothing
    }

    void Put(char value)
    {
        if constexpr (!std::is_same<PODArrayType, ColumnString::Chars>::value)
            throw Exception("Method Peek is not supported for const ColumnString::Chars Put value:" + toString(value), ErrorCodes::NOT_IMPLEMENTED);
        else
        {
            array[offset + cursor++] = static_cast<UInt8>(value);
        }
    }

    size_t PutEnd(char * /*value*/)
    {
        throw Exception("Method PutEnd is not supported for PODArraySmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
    }

    char * PutBegin()
    {
        throw Exception("Method PutBegin is not supported for PODArraySmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    PODArrayType & array;
    const FormatSettings & settings;

    size_t cursor{0};
    size_t offset{0}, limit{0};
};

}
