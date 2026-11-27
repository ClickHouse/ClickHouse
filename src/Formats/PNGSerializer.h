#pragma once

#include <functional>

#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>

namespace DB
{

/** Serializes rows of a result set into in-memory images of fixed size.
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
  *   t of integer type     -> the relative time offset of the frame this record belongs to, which turns the
  *                            result into an animation. Records are grouped into frames by the value of `t`,
  *                            and each frame is an independent image: the canvas is empty at the start of
  *                            every frame and the implicit coordinate cursor restarts from the top-left.
  *   no t                  -> a single still image.
  *
  * If the color mode cannot be unambiguously determined from the columns, an exception is thrown.
  *
  * Empty pixels (without data) are filled with black in RGB and grayscale modes, and with
  * transparent black (zero alpha) in RGBA mode.
  *
  * The image is always of the exact size given by `FormatSettings::image::width` x `FormatSettings::image::height`.
  *
  * In the animated mode the completed frames are handed to the callback installed by `setFrameCallback`, in
  * ascending order of `t`. When `FormatSettings::image::streaming_animation` is set, a frame is handed over as
  * soon as `t` advances, which requires `t` to be non-decreasing and keeps only one frame in memory; otherwise
  * every frame is buffered until `finalizeFrames` and `t` may come in any order.
  */
class PNGSerializer
{
public:
    PNGSerializer(const Block & header, const FormatSettings & settings);
    ~PNGSerializer();

    /// Whether the input has a `t` column, which turns the result into an animation.
    bool isAnimated() const;

    /// Whether the frames are handed over as soon as they are complete, instead of being buffered until the
    /// whole result has been read.
    bool isStreamingAnimation() const;

    /// Called once per completed frame, in ascending order of `t`. The frame is displayed for
    /// `delay_num / delay_den` seconds. `pixels` is only valid for the duration of the call.
    using FrameCallback = std::function<void(const UInt8 * pixels, UInt16 delay_num, UInt16 delay_den)>;
    void setFrameCallback(FrameCallback callback);

    void setColumns(const ColumnPtr * columns, size_t num_columns);
    void writeRow(size_t row_num);

    /// Hand over every frame that has not been handed over yet, and make the frame count final.
    /// An animation always has at least one frame, even if the result is empty.
    void finalizeFrames();

    void reset();

    /// The frame count to declare in the `acTL` chunk. Exact once all frames are known, which is the case
    /// while `finalizeFrames` is running in the buffered mode; an upper bound in the streaming mode, where
    /// the frames are written out before the last one has been seen.
    UInt32 getDeclaredFrameCount() const;

    /// The fixed image dimensions and the number of channels (1, 3, or 4) determined from the input columns.
    size_t getWidth() const;
    size_t getHeight() const;
    size_t getChannels() const;

    /// The rendered image as a tightly packed buffer of `getWidth() * getHeight() * getChannels()` bytes.
    /// Only meaningful when the result is a still image.
    const UInt8 * getPixels() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
