#pragma once

#include "config.h"

#if USE_VORTEX

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>

struct FFI_VortexExpression;
struct FFI_VortexReader;
struct FFI_VortexRuntime;
enum class FFI_VortexComparisonOperator : int32_t;

namespace DB
{
class ReadBuffer;
class SeekableReadBuffer;
struct FormatSettings;
}

namespace DB::ArrowIPC
{
struct ArrowSchema;
}

namespace DB::Vortex
{

struct VortexExpressionDeleter
{
    void operator()(FFI_VortexExpression * expression) const;
};
using VortexExpressionPtr = std::unique_ptr<FFI_VortexExpression, VortexExpressionDeleter>;

/// A Vortex file as the library reads it: by positioned reads. A buffer that supports concurrent positioned reads is
/// read directly; one that can only seek is read under a lock, one read at a time; anything else is first read into
/// memory as a whole.
class VortexFile
{
public:
    static std::unique_ptr<VortexFile> open(ReadBuffer & in, const FormatSettings & format_settings, std::atomic<int> & is_stopped);

    [[nodiscard]] size_t size() const { return file_size; }
    /// Whether several threads may read at once.
    [[nodiscard]] bool isThreadSafe() const { return !seekable_in || read_at; }
    [[nodiscard]] bool isInMemory() const { return !seekable_in; }

    /// Reads exactly `length` bytes at `offset`, or throws.
    void readAt(char * to, size_t length, size_t offset);

private:
    SeekableReadBuffer * seekable_in = nullptr;
    bool read_at = false;
    std::mutex mutex;
    String data;
    size_t file_size = 0;
};

/// The state the read callback works with. Several threads may be inside the callback at once when
/// the buffer underneath supports concurrent positioned reads.
struct VortexReadContext
{
    VortexFile * file = nullptr;
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

FFI_VortexReader * openVortexReader(
    const FFI_VortexRuntime * runtime,
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped,
    std::unique_ptr<VortexFile> & file,
    VortexReadContext & read_context,
    std::shared_ptr<const ArrowIPC::ArrowSchema> & file_schema,
    size_t io_threads,
    bool is_remote_fs);

}

#endif
