#include <png.h>
#include <Formats/PngWriter.h>

#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

PngResourceWrapper::PngResourceWrapper(png_structp png_ptr_, png_infop info_ptr_) noexcept
    : png_ptr(png_ptr_)
    , info_ptr(info_ptr_)
{
}

PngResourceWrapper::~PngResourceWrapper()
{
    /// Checks only png_ptr, because the info pointer is tied to png
    if (png_ptr)
        png_destroy_write_struct(&png_ptr, &info_ptr);
}

PngResourceWrapper::PngResourceWrapper(PngResourceWrapper && other) noexcept
    : png_ptr(other.png_ptr)
    , info_ptr(other.info_ptr)
{
    other.png_ptr = nullptr;
    other.info_ptr = nullptr;
}

PngResourceWrapper & PngResourceWrapper::operator=(PngResourceWrapper && other) noexcept
{
    if (this != &other)
    {
        if (png_ptr)
            png_destroy_write_struct(&png_ptr, &info_ptr);

        png_ptr = other.png_ptr;
        info_ptr = other.info_ptr;
        other.png_ptr = nullptr;
        other.info_ptr = nullptr;
    }

    return *this;
}

PngWriter::PngWriter(WriteBuffer & out_, int bit_depth_, int color_type_, int compression_level_)
    : out(out_)
    , bit_depth(bit_depth_)
    , color_type(color_type_)
    , compression_level(compression_level_)
    , log(::getLogger("PngWriter"))
{
    if (bit_depth != 16 && bit_depth != 8)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid bit depth provided ({}). Only 8 or 16 is supported", bit_depth);
    }

    if (compression_level < -1 || compression_level > 9)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid compression level ({}). Must be between -1 and 9", compression_level);

    switch (color_type)
    {
        case PNG_COLOR_TYPE_GRAY:
        case PNG_COLOR_TYPE_RGB:
        case PNG_COLOR_TYPE_RGBA:
            break;

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported PNG color type ({}) requested", color_type);
    }
}

template <typename Func>
void PngWriter::executePngOperation(Func && func, const char * error_context)
{
    if (!handle_)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PngWriter internal error. png resources is null");
    }

    if (setjmp(png_jmpbuf(handle_->getPngPtr()))) // NOLINT(cert-err52-cpp)
    {
        /// TODO:
        /// If we are here it means longjmp occurred.
        /// Libpng erroCcallback should have already thrown a C++ exception with libpng error message
        /// If errorCallback couldnt't throw or something else went wrong we throw a generic error here
        /// This path might not even be reachable, but we have a fallback:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Setjmp triggered an error during {}", error_context);
    }
    else
    {
        /// If func() throws C++ exception
        /// It would be propagated noramly by RAII
        std::forward<Func>(func)();
    }
}

void PngWriter::writeDataCallback(png_struct_def * png_ptr_, unsigned char * data, size_t length)
{
    auto * writer = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr_));
    if (!writer)
        return;
    try
    {
        writer->out.write(reinterpret_cast<const char *>(data), length);
    }
    catch (...)
    {
        /**
         * If WriteBuffer throws, signal png error. libpng should longjmp
         * We rely on main error handler to convert this
         * Avoid throwing directly from a C callback if possible using png_error 
         * **/
        png_error(png_ptr_, "Error writing png image via WriteBuffer");
    }
}

void PngWriter::flushDataCallback(png_struct_def * png_ptr_)
{
    auto * writer = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr_));
    if (!writer)
        return;
    try
    {
        writer->out.next();
    }
    catch (...)
    {
        png_error(png_ptr_, "Error flushing WriteBuffer while writing png image");
    }
}

[[noreturn]] void PngWriter::errorCallback(png_structp /* png_ptr */, png_const_charp error_msg)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PngWriter: libpng error: {}", std::string(error_msg ? error_msg : "Unknown libpng error"));
}

void PngWriter::warningCallback(png_struct_def * png_ptr, png_const_charp warning_msg)
{
    auto * writer = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr));
    if (!writer)
        return;

    LOG_WARNING(writer->getLogger(), "Libpng warning: {}", std::string(warning_msg ? warning_msg : "Unknown libpng warning"));
}

void PngWriter::startImage(size_t width_, size_t height_)
{
    LOG_INFO(log, "Starting new png image with resolution {}x{}", width_, height_);

    width = width_;
    height = height_;

    png_structp png_ptr = nullptr;
    png_infop info_ptr = nullptr;

    png_ptr
        = png_create_write_struct(PNG_LIBPNG_VER_STRING, static_cast<void *>(this), &PngWriter::errorCallback, &PngWriter::warningCallback);

    if (!png_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create libpng write struct");

    info_ptr = png_create_info_struct(png_ptr);

    if (!info_ptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create libpng info struct");
    }

    handle_ = std::make_unique<PngResourceWrapper>(png_ptr, info_ptr);

    executePngOperation(
        [&]()
        {
            png_set_write_fn(
                handle_->getPngPtr(), reinterpret_cast<void *>(this), PngWriter::writeDataCallback, PngWriter::flushDataCallback);

            png_set_IHDR(
                handle_->getPngPtr(),
                handle_->getInfoPtr(),
                static_cast<png_uint_32>(width_),
                static_cast<png_uint_32>(height_),
                bit_depth,
                color_type,
                PNG_INTERLACE_NONE,
                PNG_COMPRESSION_TYPE_DEFAULT,
                PNG_FILTER_TYPE_DEFAULT);

            /// Libpng assumes that the data is in big-endian format
            if (bit_depth == 16)
                png_set_swap(handle_->getPngPtr());

            /* TODO: Allow to control filters for better image compression */
            png_set_filter(handle_->getPngPtr(), PNG_FILTER_TYPE_BASE, PNG_FILTER_NONE);

            png_set_compression_level(handle_->getPngPtr(), compression_level);
        },
        "image inizialization");

    LOG_INFO(log, "Image header set successfully: {}x{} with bit_depth {} and color_type RGBA", width, height, bit_depth);
}

void PngWriter::writeRows(const unsigned char * data, size_t data_size)
{
    LOG_INFO(log, "Writing png image data");

    size_t bytes_per_component = (bit_depth == 16) ? 2 : 1;
    size_t channels;
    switch (color_type)
    {
        case PNG_COLOR_TYPE_GRAY:
            channels = 1;
            break;
        case PNG_COLOR_TYPE_RGB:
            channels = 3;
            break;
        case PNG_COLOR_TYPE_RGBA:
            channels = 4;
            break;
        default:
            /// Defensive check, already should be caught by constructor
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid color type ({}) encountered", color_type);
    }

    size_t row_bytes = channels * width * bytes_per_component;
    size_t expected_total_size = row_bytes * height;

    if (data_size != expected_total_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incorrect data size provided to write entire png image. Expected {} bytes ({}x{}x{}channels x{}bytes/comp), but received {} "
            "bytes",
            expected_total_size,
            width,
            height,
            channels,
            bytes_per_component,
            data_size);
    }

    std::vector<png_bytep> row_pointers(height);

    for (size_t y = 0; y < height; ++y)
    {
        row_pointers[y] = const_cast<png_bytep>(data + (y * row_bytes));
    }

    executePngOperation(
        [&]()
        {
            png_write_info(handle_->getPngPtr(), handle_->getInfoPtr());
            png_write_image(handle_->getPngPtr(), row_pointers.data());
        },
        "writing image data");
}

void PngWriter::finalize()
{
    if (!handle_)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot finish writing png image before header is set");
    }

    LOG_INFO(log, "Finalizing png image");

    executePngOperation([&]() { png_write_end(handle_->getPngPtr(), handle_->getInfoPtr()); }, "finalizing image");

    try
    {
        handle_.reset();
        out.next();
    }
    catch (...)
    {
        throw;
    }
}
}
