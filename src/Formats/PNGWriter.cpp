#include <Formats/PNGWriter.h>

#include <png.h>
#include <vector>

#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr int PNG_COMPRESSION_LEVEL = 6;
}

PNGWriter::PNGWriter(WriteBuffer & out_)
    : out(out_)
{
}

PNGWriter::~PNGWriter()
{
    cleanup();
}

void PNGWriter::cleanup()
{
    if (png_ptr)
        png_destroy_write_struct(&png_ptr, info_ptr ? &info_ptr : nullptr);
    png_ptr = nullptr;
    info_ptr = nullptr;
}

void PNGWriter::setImage(size_t width_, size_t height_, size_t channels_)
{
    width = width_;
    height = height_;
    channels = channels_;

    if (channels != 1 && channels != 3 && channels != 4)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "PNG writer supports only 1, 3, or 4 channels per pixel, got {}", channels);
}

void PNGWriter::writeImage(const unsigned char * pixels)
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PNG writer can encode only one image");

    /// Build the row pointers before `setjmp`, so that this object is in scope (and thus destroyed during
    /// stack unwinding) if a callback longjmps back and we throw from the handler below.
    std::vector<png_bytep> row_pointers(height);
    const size_t row_bytes = width * channels;
    for (size_t y = 0; y < height; ++y)
        row_pointers[y] = const_cast<png_bytep>(pixels + y * row_bytes);

    png_ptr = png_create_write_struct(PNG_LIBPNG_VER_STRING, this, &PNGWriter::errorCallback, &PNGWriter::warningCallback);
    if (!png_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create libpng write struct");

    info_ptr = png_create_info_struct(png_ptr);
    if (!info_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create libpng info struct");

    if (setjmp(png_jmpbuf(png_ptr))) // NOLINT(cert-err52-cpp)
    {
        /// A callback longj'd back here. Either an I/O exception was saved while writing, or libpng raised an error/warning.
        if (saved_exception)
            std::rethrow_exception(saved_exception);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error while encoding image: {}",
            error_message.empty() ? "unknown" : error_message);
    }

    png_set_write_fn(png_ptr, this, &PNGWriter::writeDataCallback, &PNGWriter::flushDataCallback);

    int color_type;
    switch (channels)
    {
        case 1: color_type = PNG_COLOR_TYPE_GRAY; break;
        case 3: color_type = PNG_COLOR_TYPE_RGB; break;
        case 4: color_type = PNG_COLOR_TYPE_RGBA; break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported number of channels");
    }

    png_set_IHDR(png_ptr, info_ptr,
        static_cast<png_uint_32>(width), static_cast<png_uint_32>(height),
        8 /* bit_depth */, color_type,
        PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);

    png_set_compression_level(png_ptr, PNG_COMPRESSION_LEVEL);

    png_write_info(png_ptr, info_ptr);
    png_write_image(png_ptr, row_pointers.data());
    png_write_end(png_ptr, info_ptr);

    initialized = true;
}

void PNGWriter::finalize()
{
    cleanup();
    out.next();
}

void PNGWriter::writeDataCallback(png_struct_def * png_ptr_, unsigned char * data, size_t length)
{
    auto * writer = reinterpret_cast<PNGWriter *>(png_get_io_ptr(png_ptr_));
    try
    {
        writer->out.write(reinterpret_cast<const char *>(data), length);
    }
    catch (...) /// Ok: a C++ exception must not propagate through libpng's C frames; save it and longjmp instead.
    {
        writer->saved_exception = std::current_exception();
        png_longjmp(png_ptr_, 1);
    }
}

void PNGWriter::flushDataCallback(png_struct_def * png_ptr_)
{
    auto * writer = reinterpret_cast<PNGWriter *>(png_get_io_ptr(png_ptr_));
    try
    {
        writer->out.next();
    }
    catch (...) /// Ok: a C++ exception must not propagate through libpng's C frames; save it and longjmp instead.
    {
        writer->saved_exception = std::current_exception();
        png_longjmp(png_ptr_, 1);
    }
}

[[noreturn]] void PNGWriter::errorCallback(png_struct_def * png_ptr_, png_const_charp error_msg)
{
    auto * writer = reinterpret_cast<PNGWriter *>(png_get_error_ptr(png_ptr_));
    if (writer)
        writer->error_message = error_msg ? error_msg : "unknown";
    png_longjmp(png_ptr_, 1);
}

[[noreturn]] void PNGWriter::warningCallback(png_struct_def * png_ptr_, png_const_charp warning_msg)
{
    /// We do not expect any warnings; treat them as errors.
    auto * writer = reinterpret_cast<PNGWriter *>(png_get_error_ptr(png_ptr_));
    if (writer)
        writer->error_message = warning_msg ? warning_msg : "unknown";
    png_longjmp(png_ptr_, 1);
}

}
