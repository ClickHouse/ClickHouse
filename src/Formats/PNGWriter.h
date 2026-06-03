#pragma once

#include "config.h"

#if USE_LIBPNG

#include <exception>
#include <string>

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
    /// `channels` controls the PNG color type and must be 1 (grayscale), 3 (RGB), or 4 (RGBA).
    PNGWriter(WriteBuffer & out, size_t width, size_t height, size_t channels);
    ~PNGWriter();

    /// Encode and write the entire image. `pixels` is a tightly packed buffer of width * height * channels bytes.
    void writeImage(const unsigned char * pixels);

    /// Flush the underlying buffer.
    void finalize();

private:
    static void writeDataCallback(png_struct_def * png_ptr, unsigned char * data, size_t length);
    static void flushDataCallback(png_struct_def * png_ptr);
    [[noreturn]] static void errorCallback(png_struct_def * png_ptr, png_const_charp error_msg);
    [[noreturn]] static void warningCallback(png_struct_def * png_ptr, png_const_charp warning_msg);

    void cleanup();

    WriteBuffer & out;

    png_structp png_ptr = nullptr;
    png_infop info_ptr = nullptr;

    /// libpng error handling uses `longjmp`, so C++ exceptions must not be thrown through its C frames.
    /// Instead, callbacks save the state here and `longjmp` back to `writeImage`, which rethrows.
    std::exception_ptr saved_exception;
    std::string error_message;

    const size_t width;
    const size_t height;
    const size_t channels;
    bool initialized = false;
};

}

#endif
