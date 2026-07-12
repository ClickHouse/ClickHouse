#pragma once

#include <Processors/Formats/Framing/IFramingFormat.h>

namespace DB
{

struct FramingFormatParameters
{
    /// Whether the response is sent over the HTTP protocol. Framing formats that integrate
    /// with the transport protocol (such as `EventStream`) throw an exception when it is not.
    bool is_http = false;

    /// Whether the output format may produce bytes that are not valid UTF-8 text (binary formats
    /// such as `Native` or `RowBinary`, and raw passthrough formats such as `RawBLOB` or `TSVRaw`).
    /// A text framing (`EventStream`) base64-encodes the payloads in this case so that arbitrary
    /// bytes survive the text transport.
    bool binary_payload = false;
};

/// Creates a framing format by name (the value of the `framing_output_format` setting).
/// Returns nullptr for the `None` framing format: everything applicable is transparently routed
/// to the output format, so everything works as it is by default.
/// Throws for unknown framing format names.
FramingFormatPtr createFramingFormat(
    const String & name,
    WriteBuffer & out,
    const FormatSettings & format_settings,
    const FramingFormatParameters & parameters);

}
