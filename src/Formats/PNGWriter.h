#pragma once

#include <png.h>
#include <boost/noncopyable.hpp>

#include <Common/Exception.h>

namespace DB
{

class WriteBuffer;

/** Thin libpng wrapper that writes an 8-bit-per-channel PNG image of fixed size to a `WriteBuffer`.
  *
  * Number of channels controls the PNG color type:
  *   1 - grayscale,
  *   3 - RGB,
  *   4 - RGBA.
  */
class PNGWriter : private boost::noncopyable
{
public:
    explicit PNGWriter(WriteBuffer & out);
    ~PNGWriter();

    /// Provide image dimensions and color type. Must be called exactly once before `writeImage`.
    void setImage(size_t width, size_t height, size_t channels);

    /// Encode and write the entire image. `pixels` is a tightly packed buffer of width * height * channels bytes.
    void writeImage(const unsigned char * pixels);

    /// Flush the underlying buffer.
    void finalize();

private:
    static void writeDataCallback(png_struct_def * png_ptr, unsigned char * data, size_t length);
    static void flushDataCallback(png_struct_def * png_ptr);
    [[noreturn]] static void errorCallback(png_struct_def * png_ptr, png_const_charp error_msg);
    static void warningCallback(png_struct_def * png_ptr, png_const_charp warning_msg);

    void cleanup();

    WriteBuffer & out;

    png_structp png_ptr = nullptr;
    png_infop info_ptr = nullptr;

    size_t width = 0;
    size_t height = 0;
    size_t channels = 0;
    bool initialized = false;
};

}
