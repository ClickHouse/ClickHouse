#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class PNGWriter;

/** Serializes rows of a result set into an in-memory image of fixed size.
  *
  * The color mode and coordinate mode are determined from the column names and types of the input:
  *
  *   r, g, b               -> RGB
  *   r, g, b, a            -> RGBA
  *   v of integer type     -> 8-bit grayscale
  *   v of floating type    -> 8-bit grayscale (values in [0, 1] mapped to [0, 255])
  *   v of Bool type        -> binary (0 or 255 in 8-bit grayscale)
  *
  *   x, y of integer type  -> explicit pixel coordinates ((0, 0) is top-left).
  *                            Records with coordinates outside the image are silently ignored.
  *                            In case of multiple records with the same coordinates, the last one wins.
  *   no x, y               -> implicit coordinates (each record fills one pixel in scanline order:
  *                            left to right, top to bottom).
  *
  * If the color mode cannot be unambiguously determined from the columns, an exception is thrown.
  *
  * Empty pixels (without data) are filled with black in RGB and grayscale modes, and with
  * transparent black (zero alpha) in RGBA mode.
  *
  * The image is always of the exact size given by `FormatSettings::image::width` x `FormatSettings::image::height`.
  */
class PNGSerializer
{
public:
    PNGSerializer(const Block & header, const FormatSettings & settings, PNGWriter & writer);
    ~PNGSerializer();

    void setColumns(const ColumnPtr * columns, size_t num_columns);
    void writeRow(size_t row_num);
    void finalizeWrite();
    void reset();

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
