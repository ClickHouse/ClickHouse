#include <Common/computeMaxTableNameLength.h>
#include <base/pathToString.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <unistd.h>


namespace DB
{

size_t computeMaxTableNameLength(const String & database_name, ContextPtr context)
{
    namespace fs = std::filesystem;

    const String suffix = ".sql.detached";
    const String metadata_path = pathToString(fs::path(context->getPath()) / "metadata");
    const String metadata_dropped_path = pathToString(fs::path(context->getPath()) / "metadata_dropped");

    // Helper lambda to get the maximum name length
    auto get_max_name_length = []([[maybe_unused]] const String & path) -> size_t {
#if defined(OS_WINDOWS)
        /// Windows has no `pathconf`. The limit is a property of the filesystem rather than of a
        /// path, and it is 255 for every filesystem Windows can create a table directory on
        /// (NTFS, ReFS, exFAT), which is also what `NAME_MAX` is on Linux.
        return 255;
#else
        auto length = pathconf(path.c_str(), _PC_NAME_MAX);
        return (length == -1) ? NAME_MAX : static_cast<size_t>(length);
#endif
    };

    size_t max_create_length = get_max_name_length(metadata_path) - suffix.length();
    size_t max_dropped_length = get_max_name_length(metadata_dropped_path);

    size_t escaped_db_name_length = escapeForFileName(database_name).length();
    const size_t dot = 1;
    const size_t uuid_length = 36; // Standard UUID length
    const size_t extension_length = strlen(".sql");

    // Adjust for database name and UUID in dropped table filenames
    // Max path will look like this: ./metadata_dropped/{db_name}.{table_name}.{uuid}.{extension}
    // Saturate at zero: the prefix alone can already exceed the limit for a long database name,
    // and an unsigned wrap here would report a huge limit instead of rejecting the name.
    const size_t dropped_prefix_length = dot + escaped_db_name_length + dot + uuid_length + extension_length;
    const size_t max_to_drop = max_dropped_length > dropped_prefix_length ? max_dropped_length - dropped_prefix_length : 0;

    // Return the minimum of the two calculated lengths
    return std::min(max_create_length, max_to_drop);
}
}
