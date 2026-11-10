#pragma once

#include <cstddef>

#include <base/types.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class WriteBuffer;

/** Minimal PNG encoder: writes 8-bit-per-channel, non-interlaced images to a `WriteBuffer`.
  *
  * The number of channels selects the PNG color type:
  *   1 - grayscale,
  *   3 - RGB,
  *   4 - RGBA.
  *
  * Only what ClickHouse needs to produce `FORMAT PNG` is implemented: 8 bits per channel and no
  * interlacing. Each scanline is filtered with the best of the five standard PNG filters (chosen by
  * the sum-of-absolute-differences heuristic) and the pixel data is compressed with Deflate (zlib), so the
  * result is a standard PNG datastream that any decoder can read.
  *
  * Two kinds of datastream can be produced, and a single writer produces exactly one of them:
  *   - a still image, with `writeImage`;
  *   - an animation (APNG), with `writeAnimationHeader`, then one `writeFrame` per frame, then `writeEnd`.
  */
class PNGWriter : private boost::noncopyable
{
public:
    /// The largest frame count an `acTL` chunk can declare. `num_frames` is a 31-bit value, and decoders
    /// reject anything above this (as well as zero).
    static constexpr UInt32 MAX_DECLARED_FRAMES = 0x7fffffff;

    /// `channels` must be 1 (grayscale), 3 (RGB), or 4 (RGBA).
    PNGWriter(WriteBuffer & out_, size_t width_, size_t height_, size_t channels_);

    /// Encode and write a whole still-image datastream. `pixels` is a tightly packed buffer of
    /// width * height * channels bytes, in row-major order (top-to-bottom, left-to-right),
    /// with `channels` bytes per pixel.
    void writeImage(const unsigned char * pixels);

    /// Begin an animation: write the signature, `IHDR` and `acTL`.
    ///
    /// `declared_num_frames` is the frame count announced by `acTL`. The APNG specification requires it to
    /// equal the number of frames that follow, which a forward-only writer knows only if the frames have
    /// been collected in advance; see `PNGSerializer` for how the two cases are handled.
    /// `num_plays` is the number of times the animation is played, 0 meaning "forever".
    void writeAnimationHeader(UInt32 declared_num_frames, UInt32 num_plays);

    /// Write one animation frame: an `fcTL` chunk followed by the pixel data, as `IDAT` for the first frame
    /// and as `fdAT` for every frame after it. Every frame covers the whole canvas and replaces it, so the
    /// image can be produced without keeping any previous frame around.
    ///
    /// The frame is displayed for `delay_num / delay_den` seconds.
    void writeFrame(const unsigned char * pixels, UInt16 delay_num, UInt16 delay_den);

    /// End an animation by writing `IEND`.
    void writeEnd();

    /// Rewrite the frame count declared by the `acTL` chunk (and the chunk's CRC) of a complete animation
    /// datastream that this writer produced, in place. For when the header had to declare an upper bound
    /// (the streaming mode), but the datastream was buffered in memory anyway (a terminal protocol carries
    /// it as a single payload), so the exact count is known before any byte of it is sent.
    void patchDeclaredFrameCount(char * datastream, size_t size) const;

    /// Flush the underlying buffer.
    void finalize();

private:
    /// Write the 8-byte signature and the `IHDR` chunk.
    void writeSignatureAndHeader();

    /// Filter and Deflate the pixels into bounded `IDAT` chunks, or `fdAT` chunks when `as_fdat` is set.
    void writePixelData(const unsigned char * pixels, bool as_fdat);

    WriteBuffer & out;
    const size_t width;
    const size_t height;
    const size_t channels;

    bool image_written = false;
    bool animation_started = false;
    size_t frames_written = 0;

    /// APNG numbers the `fcTL` and `fdAT` chunks with a single counter, shared across all frames, which
    /// decoders use to detect a reordered or truncated datastream.
    UInt32 sequence_number = 0;
};

}
