#pragma once

#include "config.h"

#if USE_SIMDUTF

#include <Core/Block_fwd.h>
#include <Formats/FormatSettings.h>
#include <Formats/PNGTerminalOutput.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/StringWithMemoryTracking.h>

namespace DB
{

class PNGSerializer;
class PNGWriter;
class WriteBufferFromStringWithMemoryTracking;

/// Output format that renders the result set as a PNG image, or, when the result has a `t` column,
/// as an animation (APNG).
class PNGOutputFormat final : public IOutputFormat
{
public:
    PNGOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);

    String getName() const override { return "PNG"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    /// Append one frame of the animation, starting the datastream if this is the first one.
    void writeFrame(const UInt8 * pixels, UInt16 delay_num, UInt16 delay_den);

    const ImageTerminalMode terminal_mode;
    std::unique_ptr<PNGSerializer> serializer;
    /// Whether the animation is written out frame by frame, which also makes every frame flushed on its own.
    bool streaming = false;

    /// The animated output. An inline terminal image protocol carries the datastream as a single payload, so
    /// in that case it is encoded into memory first; otherwise the frames go straight to `out`.
    StringWithMemoryTracking animation_buffer;
    std::unique_ptr<WriteBufferFromStringWithMemoryTracking> animation_buffer_out;
    std::unique_ptr<PNGWriter> animation_writer;
};

}

#endif
