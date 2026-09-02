#include <Processors/Formats/Framing/FramingFormatFactory.h>

#include <Processors/Formats/Framing/FramingFormatEventStream.h>
#include <Processors/Formats/Framing/FramingFormatJSONEachPacket.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_FORMAT;
}

FramingFormatPtr createFramingFormat(
    const String & name,
    WriteBuffer & out,
    const FormatSettings & format_settings,
    const FramingFormatParameters & parameters)
{
    if (boost::iequals(name, "None"))
        return nullptr;

    if (boost::iequals(name, "EventStream"))
    {
        if (!parameters.is_http)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The EventStream framing format integrates with the HTTP protocol and is not applicable here");
        return std::make_shared<FramingFormatEventStream>(out, format_settings);
    }

    if (boost::iequals(name, "JSONEachPacketBase64"))
        return std::make_shared<FramingFormatJSONEachPacket>(out, format_settings, /*base64_=*/ true);

    if (boost::iequals(name, "JSONEachPacketString"))
        return std::make_shared<FramingFormatJSONEachPacket>(out, format_settings, /*base64_=*/ false);

    throw Exception(ErrorCodes::UNKNOWN_FORMAT,
        "Unknown framing format {}. Supported framing formats: None, EventStream, JSONEachPacketBase64, JSONEachPacketString", name);
}

}
