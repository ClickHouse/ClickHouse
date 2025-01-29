#include <Formats/PngWriter.h>

#ifdef   USE_LIBPNG
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <IO/WriteBuffer.h>

namespace DB {

PngWriter::PngWriter(WriteBuffer & out_)
    : out(out_)
{
}

PngWriter::~PngWriter()
{
    cleanup();
}

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
    if (started) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
        "TODO");
    }

    width = width_;
    height = height_;
    started = true;

    png_ptr = png_create_write_struct(PNG_LIBPNG_VER_STRING, nullptr, 
        /// libpng UDF for errors
        [](png_structp, png_const_charp msg){
            throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error: {}", std::string(msg));
        }, 
        /// TODO: libpng UDF for warnings
        [](png_structp, png_const_charp){ /* LOG_WARNING */}
    );

    info_ptr = png_create_info_struct(png_ptr);

    if (!info_ptr) {
        png_destroy_write_struct(&png_ptr, nullptr);
        png_ptr = nullptr;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng failed to create png info struct");
    }

    if (setjmp(png_jmpbuf(png_ptr)))
    {
        cleanup();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "libpng error during startImage (setjmp triggered)");
    }

    png_set_write_fn(png_ptr, reinterpret_cast<void *>(this),
                    &PngWriter::writeDataCallback,
                    &PngWriter::flushDataCallback);

    int bit_depth   = 8;
    int color_type  = PNG_COLOR_TYPE_RGBA;

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
    if (!started || !info_ptr || !png_ptr) 
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "PngWriter::startImage not called before PngWriter::writeEntireImage"
        );
    }
    std::vector<png_bytep> row_pointers(height);
    size_t row_bytes = width * 4;

    for (size_t y = 0; y < height; ++y) 
        row_pointers[y] = const_cast<png_bytep>(data + (y * row_bytes));
    
    png_write_info(png_ptr, info_ptr);
    png_write_image(png_ptr, row_pointers.data());
    png_write_end(png_ptr, info_ptr);
}

void PngWriter::finishImage()
{
    if (!started)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "PngWriter::startImage not called before PngWriter::finishImage"
        );
    }

    if (setjmp(png_jmpbuf(png_ptr)))
    {
        cleanup();
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "libpng error during finishImage (setjmp triggered)"
        );
    }

    cleanup();
}

void PngWriter::cleanup()
{
    if (png_ptr || info_ptr)
    {
        png_destroy_write_struct(&png_ptr, &info_ptr);
        png_ptr  = nullptr;
        info_ptr = nullptr;
    }
    
    if (started) 
    {
        out.next();
        started = false;
    }
}


}

#endif /// USE_LIBPNG
