#pragma once

#include <png.h>
#include <boost/noncopyable.hpp>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class WriteBuffer;

/// Utility class for writing in the png format.
/// Just provides useful functions to serialize data
class PngWriter : boost::noncopyable
{
public:
    explicit PngWriter(WriteBuffer &, Int32);

    ~PngWriter();

    void startImage(size_t width_, size_t height_);

    void finishImage();

    void writeEntireImage(const unsigned char * data);

private:
    static void writeDataCallback(png_struct_def * png_ptr, unsigned char * data, size_t length);

    static void flushDataCallback(png_struct_def * png_ptr);

    void cleanup();

    WriteBuffer & out;
    size_t width = 0;
    size_t height = 0;
    Int32 bit_depth;
    bool started = false;

    png_struct_def * png_ptr = nullptr;
    png_info_def * info_ptr = nullptr;
};

}
