#pragma once

#include <base/types.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>


namespace DB
{

class Chunk;
class IColumn;
struct FormatSettings;

/// Prints text describing the number in the form of: -- 12.34 million
void writeReadableNumberTip(WriteBuffer & out, const IColumn & column, size_t row, const FormatSettings & settings, bool color, size_t max_width = SIZE_MAX);

/// Underscores digit groups related to thousands using terminal ANSI escape sequences.
String highlightDigitGroups(String source);

/// Highlights and underscores trailing spaces using ANSI escape sequences.
String highlightTrailingSpaces(String source);

/// Replace non-printable control characters (C0 controls and DEL) with the corresponding
/// Unicode "Control Pictures" (U+2400..U+2421), so they become visible instead of being swallowed.
String replaceControlCharactersWithPictures(String source);

/// Streaming counterpart of `replaceControlCharactersWithPictures`: a `WriteBuffer` that forwards
/// everything written to it to the underlying buffer, replacing non-printable control characters
/// with the corresponding Unicode "Control Pictures" on the fly. Using this instead of the `String`
/// helper avoids materializing the whole value in memory, so large values keep streaming.
class WriteBufferReplacingControlCharacters final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferReplacingControlCharacters(WriteBuffer & out_);
    ~WriteBufferReplacingControlCharacters() override;

private:
    /// An instance is constructed per value, so the buffer has to be small: the default 1 MiB
    /// buffer would mean a large allocation for every value written, which is prohibitively
    /// slow (especially under sanitizers) and defeats the purpose of the streaming path.
    static constexpr size_t buffer_size = 4096;

    void nextImpl() override;

    WriteBuffer & out;
};

/// If the visible width of the name is longer than `cut_to` + `hysteresis`,
/// and it isn't a proper identifier, truncate it to `cut_to`
/// by cutting it in the middle and replacing with a single filler character (ascii or unicode).
std::pair<String, size_t> truncateName(String name, size_t cut_to, size_t hysteresis, bool ascii);

}
