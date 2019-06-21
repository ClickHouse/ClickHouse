#pragma once

#include <IO/ReadHelpers.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/SmallestJSON/SmallestJSONStreamFactory.h>
#include <IO/WriteHelpers.h>

namespace DB
{

struct BufferStreamHelper
{
    template <typename BufferType>
    static char Peek(BufferType & /*buffer*/);

    template <typename BufferType>
    static char Take(BufferType & /*buffer*/);

    template <typename BufferType>
    static size_t Tell(BufferType & /*buffer*/);

    template <typename BufferType>
    static void Put(BufferType & /*buffer*/, char /*value*/);

    template <RapidFormat format>
    static char SkipQuoted(ReadBuffer & /*buffer*/, const FormatSettings & /*setting*/, char /*maybe_opening_quoted*/);

    template <RapidFormat format>
    static char SkipQuoted(WriteBuffer & /*buffer*/, const FormatSettings & /*setting*/, char /*maybe_opening_quoted*/);
};

template <typename BufferType, RapidFormat format>
struct BufferSmallestJSONStream
{
public:
    typedef char Ch;

    BufferSmallestJSONStream(BufferType * buffer_, const FormatSettings & settings_)
        : buffer(buffer_), settings(settings_)
    {
    }

    void Flush() {  /* do nothing */ }

    char Take() { return BufferStreamHelper::Take<BufferType>(*buffer); }

    char Peek() const { return BufferStreamHelper::Peek<BufferType>(*buffer); }

    size_t Tell() const { return BufferStreamHelper::Tell<BufferType>(*buffer); }

    void Put(char value) { BufferStreamHelper::Put<BufferType>(*buffer, value); }

    void SkipQuoted() { quote_char = BufferStreamHelper::SkipQuoted<format>(*buffer, settings, quote_char); }

    char * PutBegin() { throw Exception("Method PutBegin is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED); }

    size_t PutEnd(char * /*value*/) { throw Exception("Method PutEnd is not supported for BufferSmallestJSONStream", ErrorCodes::NOT_IMPLEMENTED); }

private:
    char quote_char{0};
    BufferType * buffer;
    const FormatSettings & settings;
};

}
