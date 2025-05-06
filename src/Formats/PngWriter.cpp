#include <png.h>
#include <Formats/PngWriter.h>

#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

PngStructWrapper::PngStructWrapper(png_structp png_ptr_, png_infop info_ptr_)
    : png_ptr(png_ptr_)
    , info_ptr(info_ptr_)
{
}

PngStructWrapper::~PngStructWrapper()
{
    if (png_ptr || info_ptr)
        png_destroy_write_struct(&png_ptr, &info_ptr);
}

PngStructWrapper::PngStructWrapper(PngStructWrapper && other) noexcept
    : png_ptr(other.png_ptr)
    , info_ptr(other.info_ptr)
{
    other.png_ptr = nullptr;
    other.info_ptr = nullptr;
}

PngStructWrapper & PngStructWrapper::operator=(PngStructWrapper && other) noexcept
{
    if (this != &other)
    {
        cleanup();
        png_ptr = other.png_ptr;
        info_ptr = other.info_ptr;
        other.png_ptr = nullptr;
        other.info_ptr = nullptr;
    }

    return *this;
}

void PngStructWrapper::cleanup()
{
    if (png_ptr || info_ptr)
    {
        // LOG_INFO(getLogger("PngWriter"), "libpng resources freed in cleanup");
        png_destroy_write_struct(&png_ptr, &info_ptr);
    }

    png_ptr = nullptr;
    info_ptr = nullptr;
}

PngWriter::PngWriter(WriteBuffer & out_, int bit_depth_, int color_type_, int compression_level_)
    : out(out_)
    , bit_depth(bit_depth_)
    , color_type(color_type_)
    , compression_level(compression_level_)
{
    if (bit_depth != 16 && bit_depth != 8)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Invalid bit depth provided ({}). Only 8 or 16 supported by current implementation", bit_depth);
    }

    if (compression_level < -1 || compression_level > 9)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid compression level ({}). Must be between -1 and 9.", compression_level);

    switch (color_type)
    {
        case PNG_COLOR_TYPE_GRAY:
        case PNG_COLOR_TYPE_RGB:
        case PNG_COLOR_TYPE_RGBA:
            break;

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported PNG color type ({}) requested", color_type);
    }

    log = getLogger("PngWriter");
}

PngWriter::~PngWriter() = default;

void PngWriter::writeDataCallback(png_struct_def * png_ptr_, unsigned char * data, size_t length)
{
    auto * self = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr_));
    self->out.write(reinterpret_cast<const char *>(data), length);
}

void PngWriter::flushDataCallback(png_struct_def * png_ptr_)
{
    auto * self = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr_));
    self->out.next();
}

void PngWriter::startImage(size_t width_, size_t height_)
{
    if (png_resource)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempting to start a new png image without finishing the previous one");
    }

    width = width_;
    height = height_;

    // LOG_INFO(getLogger("PngWriter"), "Starting new png image with resolution {}x{}", width, height);

    auto * png_ptr = png_create_write_struct(
        PNG_LIBPNG_VER_STRING,
        nullptr,
        /// Error handling function
        [](png_structp, png_const_charp msg) { throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error: {}", std::string(msg)); },
        /// Warning handling function
        [](png_structp, [[maybe_unused]] png_const_charp msg)
        {
            // LOG_WARNING(getLogger("pngWriter"), "libpng warning: {}", std::string(msg));
        });

    if (!png_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create libpng png_struct. Likely a memory allocation error");

    auto * info_ptr = png_create_info_struct(png_ptr);

    if (!info_ptr)
    {
        png_destroy_write_struct(&png_ptr, nullptr);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create libpng info_struct");
    }

    if (setjmp(png_jmpbuf(png_ptr))) // NOLINT(cert-err52-cpp)
    {
        cleanup();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error during startImage");
    }

    png_resource = std::make_unique<PngStructWrapper>(png_ptr, info_ptr);

    /// png_set_write_fn(png_ptr, reinterpret_cast<void *>(this), &PngWriter::writeDataCallback, &PngWriter::flushDataCallback);
    png_set_write_fn(png_ptr, reinterpret_cast<void *>(this), &PngWriter::writeDataCallback, nullptr);

    png_set_IHDR(
        png_ptr,
        info_ptr,
        static_cast<png_uint_32>(width),
        static_cast<png_uint_32>(height),
        bit_depth,
        color_type,
        PNG_INTERLACE_NONE,
        PNG_COMPRESSION_TYPE_DEFAULT,
        PNG_FILTER_TYPE_DEFAULT);

    if (bit_depth == 16)
        png_set_swap(png_ptr);
    
    /* TODO: Allow to control filters for better image compression */
    png_set_filter(png_ptr, PNG_FILTER_TYPE_BASE, PNG_FILTER_NONE);
    
    png_set_compression_level(png_ptr, compression_level);

    // LOG_INFO(getLogger("PngWriter"), "Image header set: {}x{} with bit_depth {} and color_type RGBA", width, height, bit_depth);
}

void PngWriter::writeEntireImage(const unsigned char * data, size_t data_size)
{
    if (!png_resource)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write png image data before header is set");
    }

    png_structp png_ptr = png_resource->getPngPtr();

    if (setjmp(png_jmpbuf(png_ptr))) // NOLINT(cert-err52-cpp)
    {
        png_resource.reset();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error during image write");
    }


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
        row_pointers[y] = const_cast<png_bytep>(data + (y * row_bytes));

    // LOG_INFO(getLogger("PngWriter"), "Writing png image data: {} rows", height);

    png_write_info(png_resource->getPngPtr(), png_resource->getInfoPtr());
    png_write_image(png_resource->getPngPtr(), row_pointers.data());
    png_write_end(png_resource->getPngPtr(), png_resource->getInfoPtr());
}

void PngWriter::finishImage()
{
    if (!png_resource)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot finish writing png image before header is set");
    }

    png_structp png_ptr = png_resource->getPngPtr();

    if (setjmp(png_jmpbuf(png_ptr))) // NOLINT(cert-err52-cpp)
    {
        cleanup();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Setjmp triggered an error during finishing to write png image");
    }

    // LOG_INFO(getLogger("PngWriter"), "Successfully finished writing png image");

    cleanup();
}

void PngWriter::cleanup()
{
    png_resource.reset();
    out.next();
}

}
