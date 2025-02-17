#include <png.h>
#include <Formats/PngWriter.h>

#include <IO/WriteBuffer.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/Logger.h>

namespace DB
{

PngStructWrapper::PngStructWrapper(png_structp png_ptr_, png_infop info_ptr_)
    : png_ptr(png_ptr_), info_ptr(info_ptr_) 
{
}

PngStructWrapper::~PngStructWrapper()
{
    if (png_ptr || info_ptr)
        png_destroy_write_struct(&png_ptr, &info_ptr);
}

PngStructWrapper::PngStructWrapper(PngStructWrapper && other) noexcept
    : png_ptr(other.png_ptr), info_ptr(other.info_ptr)
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
    if (png_ptr)
        png_destroy_write_struct(&png_ptr, &info_ptr);
    png_ptr = nullptr;
    info_ptr = nullptr;
}

PngWriter::PngWriter(WriteBuffer & out_, Int32 bit_depth_) : out(out_), bit_depth(bit_depth_)
{
    if (bit_depth >= 16 || bit_depth < 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid bit depth: {}", bit_depth);
    }
}

PngWriter::~PngWriter() = default;

void PngWriter::writeDataCallback(png_struct_def * png_ptr_, unsigned char * data, size_t length)
{
    auto * self = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr_));
    if (!self)
        return;

    self->out.write(reinterpret_cast<const char *>(data), length);
}

void PngWriter::flushDataCallback(png_struct_def * png_ptr_)
{
    auto * self = reinterpret_cast<PngWriter *>(png_get_io_ptr(png_ptr_));
    if (!self)
        return;

    self->out.next();
}

void PngWriter::startImage(size_t width_, size_t height_)
{
    if (started)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TODO");
    }

    width = width_;
    height = height_;
    started = true;
    
    auto * png_ptr = png_create_write_struct(
        PNG_LIBPNG_VER_STRING,
        nullptr,
        /// libpng function for errors handling
        [](png_structp, png_const_charp msg) { 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error: {}", std::string(msg)); 
        },
        /// libpng function for warnings handling
        [](png_structp, png_const_charp msg) { 
            LOG_WARNING(getLogger("pngWriter"), "{}", msg);  
        }
    );

    if (!png_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "png_create_write_struct() failed");

    auto * info_ptr = png_create_info_struct(png_ptr);

    if (!info_ptr)
    {
        png_destroy_write_struct(&png_ptr, nullptr);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng failed to create info struct");
    }

    if (setjmp(png_jmpbuf(png_ptr)))
    {
        cleanup();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error during startImage");
    }

    png_resource = std::make_unique<PngStructWrapper>(png_ptr, info_ptr);

    png_set_write_fn(
        png_ptr, 
        reinterpret_cast<void *>(this), 
        &PngWriter::writeDataCallback, 
        &PngWriter::flushDataCallback
    );

    int color_type = PNG_COLOR_TYPE_RGBA;

    png_set_IHDR(
        png_ptr,
        info_ptr,
        static_cast<png_uint_32>(width),
        static_cast<png_uint_32>(height),
        bit_depth,
        color_type,
        PNG_INTERLACE_NONE,
        PNG_COMPRESSION_TYPE_DEFAULT,
        PNG_FILTER_TYPE_DEFAULT
    );
}

void PngWriter::writeEntireImage(const unsigned char * data)
{
    if (!started || !png_resource)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "");
    }
    
    std::vector<png_bytep> row_pointers(height);
    size_t row_bytes = width * 4;

    for (size_t y = 0; y < height; ++y)
        row_pointers[y] = const_cast<png_bytep>(data + (y * row_bytes));
        
    png_write_info(png_resource->getPngPtr(), png_resource->getInfoPtr());
    png_write_image(png_resource->getPngPtr(), row_pointers.data());
    png_write_end(png_resource->getPngPtr(), png_resource->getInfoPtr());
}

void PngWriter::finishImage()
{
    if (!started)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PngWriter::startImage not called before PngWriter::finishImage");
    }

    auto * png_ptr = png_resource ? png_resource->getPngPtr() : nullptr;
    
    if (setjmp(png_jmpbuf(png_ptr)))
    {
        cleanup();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error during finishImage (setjmp triggered)");
    }

    cleanup();
}

void PngWriter::cleanup()
{
    png_resource.reset();

    if (started)
    {
        out.next();
        started = false;
    }
}

}
