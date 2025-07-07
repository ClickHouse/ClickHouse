#pragma once

#include <base/types.h>


namespace DB
{

class Chunk;
class IColumn;
class WriteBuffer;
struct FormatSettings;

/// Prints text describing the number in the form of: -- 12.34 million
void writeReadableNumberTip(WriteBuffer & out, const IColumn & column, size_t row, const FormatSettings & settings, bool color);
void writeReadableNumberTipIfSingleValue(WriteBuffer & out, const Chunk & chunk, const FormatSettings & settings, bool color);

/// Underscores digit groups related to thousands using terminal ANSI escape sequences.
String highlightDigitGroups(String source);

/// Highlights and underscores trailing spaces using ANSI escape sequences.
String highlightTrailingSpaces(String source);

/// If the visible width of the name is longer than `cut_to` + `hysteresis`,
/// and it isn't a proper identifier, truncate it to `cut_to`
/// by cutting it in the middle and replacing with a single filler character (ascii or unicode).
std::pair<String, size_t> truncateName(String name, size_t cut_to, size_t hysteresis, bool ascii);

}
