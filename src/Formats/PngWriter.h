#pragma once

#include <png.h>
#include <boost/noncopyable.hpp>
#include "Common/Logger.h"
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

class WriteBuffer;

/** RAII wrapper for managing libpng resources */
class PngResourceWrapper : private boost::noncopyable
{
    friend class PngWriter;

    png_structp png_ptr = nullptr;
    png_infop info_ptr = nullptr;

public:
    PngResourceWrapper(png_structp png_ptr_, png_infop info_ptr_) noexcept;

    ~PngResourceWrapper();

    PngResourceWrapper(PngResourceWrapper && other) noexcept;

    PngResourceWrapper & operator=(PngResourceWrapper && other) noexcept;

    png_structp getPngPtr() const noexcept { return png_ptr; }

    png_infop getInfoPtr() const noexcept { return info_ptr; }
};


/** Utility class for writing in the png format
 * Provides useful functions to configure, write pixel data and finalize the PNG stream */
class PngWriter : private boost::noncopyable
{
public:
    PngWriter(WriteBuffer & out, int bit_depth, int color_type, int compression_level);

    /// RAII ensures libpng resources are freed, but the WriteBuffer is not flushed
    ~PngWriter() = default;

    /// Initialize the png stream and writes IHDR chunk
    void startImage(size_t width_, size_t height_);

    void finishImage();

    void writeRows(const unsigned char * data, size_t data_size);

    void finalize();

    LoggerPtr getLogger() const noexcept { return log; }

private:
    static void writeDataCallback(png_struct_def * png_ptr, unsigned char * data, size_t length);
    static void flushDataCallback(png_struct_def * png_ptr);

    /**
     * This callback is called by libpng just before it calls longjmp
     * We convert the C error message to a C++ exception.
     * This exception won't be caught locally but indicates the cause of the longjmp **/
    [[noreturn]] static void errorCallback(png_struct_def * png_ptr, png_const_charp error_msg);

    static void warningCallback(png_struct_def * png_ptr, png_const_charp warning_msg);

    /**
     * Executes libpng operations within a setjmp context for error handling **/
    template <typename Func>
    void executePngOperation(Func && func, const char * error_context);

    WriteBuffer & out;

    int bit_depth;
    int color_type;
    int compression_level;

    size_t width = 0;
    size_t height = 0;

    std::unique_ptr<PngResourceWrapper> handle_;

    LoggerPtr log = nullptr;
};

}
