#pragma once

#include <cstddef>

#include <boost/noncopyable.hpp>

namespace DB
{

class WriteBuffer;

/** Minimal PNG encoder: writes a single 8-bit-per-channel, non-interlaced image to a `WriteBuffer`.
  *
  * The number of channels selects the PNG color type:
  *   1 - grayscale,
  *   3 - RGB,
  *   4 - RGBA.
  *
  * Only what ClickHouse needs to produce `FORMAT PNG` is implemented: one image, 8 bits per channel,
  * and no interlacing. Each scanline is filtered with the best of the five standard PNG filters (chosen by
  * the sum-of-absolute-differences heuristic) and the pixel data is compressed with Deflate (zlib), so the
  * result is a standard PNG datastream that any decoder can read.
  */
class PNGWriter : private boost::noncopyable
{
public:
    /// `channels` must be 1 (grayscale), 3 (RGB), or 4 (RGBA).
    PNGWriter(WriteBuffer & out_, size_t width_, size_t height_, size_t channels_);

    /// Encode and write the whole image. `pixels` is a tightly packed buffer of width * height * channels bytes,
    /// in row-major order (top-to-bottom, left-to-right), with `channels` bytes per pixel.
    void writeImage(const unsigned char * pixels);

    /// Flush the underlying buffer.
    void finalize();

private:
    WriteBuffer & out;
    const size_t width;
    const size_t height;
    const size_t channels;
    bool written = false;
};

}
