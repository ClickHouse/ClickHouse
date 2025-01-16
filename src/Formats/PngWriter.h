#pragma once

#include <png.h>
#include <Common/Exception.h>

namespace DB 
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class WriteBuffer;

/// Utility class for writing in the PNG format.
/// Provides useful functions to serialize data
/// Follows the rule of 5
class PngWriter
{
public:
    explicit PngWriter(WriteBuffer & out_);
    
    ~PngWriter();

    PngWriter(const PngWriter &) = delete;
    
    PngWriter & operator=(const PngWriter &) = delete;
    
    PngWriter(PngWriter && other) noexcept;
    
    PngWriter & operator=(PngWriter && other) noexcept;
    
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
    bool started = false;

    png_struct_def * png_ptr = nullptr;
    png_info_def   * info_ptr = nullptr;
};

}
