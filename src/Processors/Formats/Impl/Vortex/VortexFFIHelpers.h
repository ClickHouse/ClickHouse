#pragma once

#include "config.h"

#if USE_VORTEX

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>

namespace arrow
{
class Schema;
class Status;
}
namespace arrow::io
{
class RandomAccessFile;
}

struct FFI_VortexExpression;
struct FFI_VortexReader;
struct FFI_VortexRuntime;

namespace DB
{
class ReadBuffer;
struct FormatSettings;
}

namespace DB::Vortex
{

struct VortexExpressionDeleter
{
    void operator()(FFI_VortexExpression * expression) const;
};
using VortexExpressionPtr = std::unique_ptr<FFI_VortexExpression, VortexExpressionDeleter>;

/// The state the read callback works with. Several threads may be inside the callback at once when
/// the buffer underneath supports concurrent positioned reads.
struct VortexReadContext
{
    arrow::io::RandomAccessFile * file = nullptr;
    std::atomic<size_t> bytes_read{0};

    std::mutex exception_mutex;
    /// A failed read surfaces from the library only as a generic scan error, so the original
    /// exception is stored here and thrown instead of it.
    std::exception_ptr exception TSA_GUARDED_BY(exception_mutex);

    void setException(std::exception_ptr e)
    {
        std::lock_guard lock(exception_mutex);
        if (!exception)
            exception = std::move(e);
    }

    std::exception_ptr getException()
    {
        std::lock_guard lock(exception_mutex);
        return exception;
    }
};

String takeVortexError(char * error);

/// Renders an expression for logs and error messages; "none" for null.
String vortexExpressionToString(const FFI_VortexExpression * expression);

std::exception_ptr makeVortexException(const String & message, const std::exception_ptr & callback_exception);

[[noreturn]] void throwVortexError(char * error, const std::exception_ptr & callback_exception);

void throwFromArrowStatusIfFailed(const arrow::Status & status);

FFI_VortexReader * openVortexReader(
    const FFI_VortexRuntime * runtime,
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped,
    std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    VortexReadContext & read_context,
    std::shared_ptr<arrow::Schema> & file_schema,
    size_t io_threads,
    bool is_remote_fs);

}

#endif
