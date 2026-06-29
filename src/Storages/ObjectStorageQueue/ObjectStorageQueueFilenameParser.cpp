#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueFilenameParser.h>
#include <__filesystem/path.h>
#include <Common/logger_useful.h>

namespace DB
{

ObjectStorageQueueFilenameParser::ObjectStorageQueueFilenameParser(
    const std::string & partition_regex,
    const std::string & partition_component)
    : partition_component_name(partition_component)
    , log(getLogger("ObjectStorageQueueFilenameParser"))
{
    /// Compile partition regex
    if (!partition_regex.empty())
    {
        partition_pattern = std::make_unique<re2::RE2>(partition_regex);
        if (!partition_pattern->ok())
        {
            error_message = fmt::format("Failed to compile partition_regex: {}", partition_pattern->error());
            partition_pattern.reset();
            LOG_ERROR(log, "{}", error_message);
        }
    }
}

std::optional<std::string> ObjectStorageQueueFilenameParser::parse(const std::string & file_name) const
{
    if (!isValid())
        return std::nullopt;

    /// Extract basename (filename only) from the full path for regex matching
    /// This ensures regexes match against the filename, not directory/bucket names
    /// Example: "/bucket/path/to/server-1_20250115T103045_0001" -> "server-1_20250115T103045_0001"
    std::string basename = std::filesystem::path(file_name).filename().string();

    /// Extract all named capture groups from partition_regex
    const auto & groups = partition_pattern->NamedCapturingGroups();
    int num_groups = partition_pattern->NumberOfCapturingGroups();

    if (num_groups == 0)
    {
        LOG_ERROR(log, "partition_regex must have at least one capture group");
        return std::nullopt;
    }

    /// Enforce that all groups are named (no unnamed groups allowed)
    if (static_cast<int>(groups.size()) != num_groups)
    {
        LOG_ERROR(
            log,
            "partition_regex must use only named capture groups (?P<name>...). "
            "Found {} total groups but only {} are named",
            num_groups,
            groups.size());
        return std::nullopt;
    }

    /// Create vector to hold all capture groups
    std::vector<std::string> captures(num_groups + 1); /// +1 for full match at index 0

    /// Create RE2::Arg array
    std::vector<RE2::Arg> args(num_groups);
    std::vector<const RE2::Arg *> arg_ptrs(num_groups);
    for (int i = 0; i < num_groups; ++i)
    {
        args[i] = &captures[i + 1]; /// Skip index 0 (full match)
        arg_ptrs[i] = &args[i];
    }

    if (!RE2::PartialMatchN(basename, *partition_pattern, arg_ptrs.data(), num_groups))
    {
        LOG_TEST(log, "Failed to match partition_regex against basename: {}", basename);
        return std::nullopt;
    }

    /// Find the partition key from named capture groups
    if (partition_component_name.empty())
    {
        LOG_ERROR(log, "partition_component setting must be specified when using regex partitioning mode");
        return std::nullopt;
    }

    auto it = groups.find(partition_component_name);
    if (it == groups.end())
    {
        LOG_ERROR(log, "partition_component '{}' not found in captured groups from partition_regex", partition_component_name);
        return std::nullopt;
    }

    std::string partition_key = captures[it->second];

    LOG_TEST(log, "Parsed file: path={}, basename={}, partition={}", file_name, basename, partition_key);

    return partition_key;
}

}
