#include <Storages/MergeTree/ProjectionNameLength.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>

#include <climits>
#include <exception>
#include <filesystem>
#include <limits>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

size_t maxProjectionNameLength()
{
    /// Reserve for the longest derived form, `delete_tmp_<name>_<block_num>.tmp_proj`; the block
    /// number has no static bound, so reserve its widest decimal form.
    constexpr size_t reserved = std::string_view("delete_tmp_").size() + std::string_view("_").size()
        + std::numeric_limits<size_t>::digits10 + 1 + std::string_view(".tmp_proj").size();

    /// The reserve must leave a usable name length; an unsigned wrap below would report a huge limit
    /// instead of rejecting the name.
    static_assert(NAME_MAX > reserved);
    return NAME_MAX - reserved;
}

void checkProjectionNameLength(const String & name)
{
    /// The name is used raw (not escaped) as a path component, so measure the raw length.
    const size_t allowed_max_length = maxProjectionNameLength();
    if (name.length() > allowed_max_length)
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "The max length of projection name is {}, current length is {}",
            allowed_max_length,
            name.length());
}

namespace
{

/// Matches on the error condition, never on the message text.
bool isFilenameTooLongError(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (const std::filesystem::filesystem_error & fs_error)
    {
        return fs_error.code() == std::errc::filename_too_long;
    }
    catch (const ErrnoException & errno_error)
    {
        return errno_error.getErrno() == ENAMETOOLONG;
    }
    catch (...)
    {
        /// Ok to discard: this only classifies an exception the caller still owns and rethrows.
        return false;
    }
}

}

void rethrowIfProjectionDirectoryNameTooLong(
    const String & projection_name, const String & directory_name, size_t allowed_max_length)
{
    if (!isFilenameTooLongError(std::current_exception()))
        return;

    /// ENAMETOOLONG also covers a whole path over PATH_MAX, which this directory name has nothing to
    /// do with. Only claim the failure when the component itself is over the limit the filesystem
    /// enforces, so a deep data root keeps reporting its own error.
    if (directory_name.length() <= NAME_MAX)
        return;

    throw Exception(
        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
        "Directory {} for projection {} does not fit the file name limit. The max length of projection name is {}, "
        "current length is {}. Drop and recreate the projection with a shorter name",
        directory_name,
        projection_name,
        allowed_max_length,
        projection_name.length());
}

}
