#include <Storages/LogStreamFileNameLength.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>

#include <climits>
#include <filesystem>
#include <string_view>

#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

constexpr std::string_view DATA_FILE_EXTENSION = ".bin";

/// Mirrors `computeMaxTableNameLength`. Probed at the disk root, which `DiskLocal::setup` creates at
/// registration: `pathconf` on a path that does not exist yet answers -1/ENOENT.
size_t componentBudget(const DiskPtr & disk)
{
    const auto length = pathconf(disk->getPath().c_str(), _PC_NAME_MAX);
    return length == -1 ? NAME_MAX : static_cast<size_t>(length);
}

/// The failing path as the backend reported it, mirroring the dispatch of `getExtraExceptionInfo`.
/// Empty when the exception carries no path.
String recoverFailedPath(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (const std::filesystem::filesystem_error & fs_error)
    {
        if (!fs_error.path1().empty())
            return fs_error.path1().string();
        return fs_error.path2().string();
    }
    catch (const ErrnoException & errno_error)
    {
        return errno_error.getPath().value_or("");
    }
    catch (...) /// Ok: classifies only. The exception stays with the caller, which rethrows it.
    {
        return {};
    }
}

}

size_t maxLogStreamFileNameLength(const DiskPtr & disk)
{
    const size_t budget = componentBudget(disk);

    /// Saturate rather than wrap: an unsigned wrap would report a huge limit instead of rejecting.
    if (budget <= DATA_FILE_EXTENSION.size())
        return 0;
    return budget - DATA_FILE_EXTENSION.size();
}

bool isFilenameTooLongError(std::exception_ptr e)
{
    /// Match on the error condition, never on the message text.
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
    catch (...) /// Ok: classifies only. The exception stays with the caller, which rethrows it.
    {
        return false;
    }
}

void rethrowIfLogFileNameTooLong(const DiskPtr & disk, const String & table_path)
{
    auto current = std::current_exception();
    if (!isFilenameTooLongError(current))
        return;

    const String failed_path = recoverFailedPath(std::move(current));
    if (failed_path.empty())
        return;

    /// Attribute to the file name only when the refused component really is over the budget:
    /// ENAMETOOLONG also covers a PATH_MAX overflow, which a short column name can hit through a deep
    /// table path. Blaming the column there would send the user to shorten an innocent name.
    const String failed_name = std::filesystem::path(failed_path).filename().string();
    const size_t budget = componentBudget(disk);
    if (failed_name.length() <= budget)
        return;

    throw Exception(
        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
        "File name {} of table {} does not fit the file name limit of {}, current length is {}. The max length of a column "
        "stream name is {}. Recreate the table with a shorter column name",
        failed_name,
        table_path,
        budget,
        failed_name.length(),
        maxLogStreamFileNameLength(disk));
}

}
