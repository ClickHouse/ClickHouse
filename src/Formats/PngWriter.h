#pragma once

#include <png.h>
#include <boost/noncopyable.hpp>
#include "Common/Logger.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_CONVERT_TYPE;
}

class WriteBuffer;

/**
 * RAII move-only wrapper for managing libpng resources
 */
class PngStructWrapper : private boost::noncopyable
{
public:
    PngStructWrapper(png_structp png_ptr_, png_infop info_ptr_);

    ~PngStructWrapper();

    PngStructWrapper(PngStructWrapper && other) noexcept;

    PngStructWrapper & operator=(PngStructWrapper && other) noexcept;

    png_structp getPngPtr() const { return png_ptr; }

    png_infop getInfoPtr() const { return info_ptr; }

private:
    void cleanup();

    png_structp png_ptr = nullptr;
    png_infop info_ptr = nullptr;
};


/** 
 * Utility class for writing in the png format.
 * Just provides useful functions to serialize data 
 */
class PngWriter : private boost::noncopyable
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
    Int32 bit_depth = 8;
    
    size_t width = 0;
    size_t height = 0;
    
    std::unique_ptr<PngStructWrapper> png_resource;

    LoggerPtr log;
};

}
