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

}
