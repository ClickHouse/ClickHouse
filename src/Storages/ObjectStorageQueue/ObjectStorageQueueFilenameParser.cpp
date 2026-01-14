#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueFilenameParser.h>
#include <Common/logger_useful.h>

namespace DB
{

ObjectStorageQueueFilenameParser::ObjectStorageQueueFilenameParser(const std::string & partition_regex, const std::string & ordering_components_regex)
    : log(getLogger("ObjectStorageQueueFilenameParser"))
{
    // Compile partition regex
    if (!partition_regex.empty())
    {
        partition_pattern = std::make_unique<re2::RE2>(partition_regex);
        if (!partition_pattern->ok())
        {
            error_message = fmt::format("Failed to compile partition_by_regex: {}", partition_pattern->error());
            partition_pattern.reset();
            LOG_ERROR(log, "{}", error_message);
        }
    }

    // Compile ordering components regex
    if (!ordering_components_regex.empty())
    {
        ordering_components_pattern = std::make_unique<re2::RE2>(ordering_components_regex);
        if (!ordering_components_pattern->ok())
        {
            error_message = fmt::format("Failed to compile ordering_components_regex: {}", ordering_components_pattern->error());
            ordering_components_pattern.reset();
            LOG_ERROR(log, "{}", error_message);
        }
    }
}

std::optional<ObjectStorageQueueFilenameParser::ParsedFilename> ObjectStorageQueueFilenameParser::parse(const std::string & file_name) const
{
    if (!isValid())
        return std::nullopt;

    ParsedFilename result;
    result.original_file_name = file_name;

    // Extract basename (filename only) from the full path for regex matching
    // This ensures regexes match against the filename, not directory/bucket names
    // Example: "/ch-s3-8472f63d-66e6-488f-ad5c-41c36f7161fe/system-tables/text_log/624719514148126379255847166409147339/c-ashaws-cm-56-server-zrgbii5-0_20251217T105757.651467Z_0000"
    // -> "c-ashaws-cm-56-server-zrgbii5-0_20251217T105757.651467Z_0000"
    std::string basename = file_name;
    auto last_slash = file_name.find_last_of('/');
    if (last_slash != std::string::npos)
        basename = file_name.substr(last_slash + 1);

    // Extract partition key
    std::string partition_capture;
    if (partition_pattern)
    {
        // Try named group 'partition' first
        if (partition_pattern->NamedCapturingGroups().contains("partition"))
        {
            if (!RE2::PartialMatch(basename, *partition_pattern, &partition_capture))
            {
                LOG_DEBUG(log, "Failed to match partition_by_regex against basename: {}", basename);
                return std::nullopt;
            }
        }
        else
        {
            // Use first capture group
            if (!RE2::PartialMatch(basename, *partition_pattern, &partition_capture))
            {
                LOG_DEBUG(log, "Failed to extract partition from basename: {}", basename);
                return std::nullopt;
            }
        }
        result.partition_key = partition_capture;
    }
    else
    {
        // No partition regex - use full path as partition key
        result.partition_key = file_name;
    }

    // Extract timestamp and sequence
    if (ordering_components_pattern)
    {
        // Must have named groups 'timestamp' and 'sequence'
        const auto & groups = ordering_components_pattern->NamedCapturingGroups();
        if (!groups.contains("timestamp") || !groups.contains("sequence"))
        {
            LOG_ERROR(log, "ordering_components_regex must contain named groups 'timestamp' and 'sequence'");
            return std::nullopt;
        }

        // Get the indices of the named groups
        int timestamp_idx = groups.at("timestamp");
        int sequence_idx = groups.at("sequence");
        int num_groups = ordering_components_pattern->NumberOfCapturingGroups();

        // Create vector to hold all capture groups
        std::vector<std::string> captures(num_groups + 1); // +1 for full match at index 0

        // Create RE2::Arg array
        std::vector<RE2::Arg> args(num_groups);
        std::vector<const RE2::Arg*> arg_ptrs(num_groups);
        for (int i = 0; i < num_groups; ++i)
        {
            args[i] = &captures[i + 1]; // Skip index 0 (full match)
            arg_ptrs[i] = &args[i];
        }

        if (!RE2::PartialMatchN(basename, *ordering_components_pattern, arg_ptrs.data(), num_groups))
        {
            LOG_DEBUG(log, "Failed to match ordering_components_regex against basename: {}", basename);
            return std::nullopt;
        }

        result.timestamp = captures[timestamp_idx];

        // Parse sequence number
        try
        {
            result.sequence_number = std::stoull(captures[sequence_idx]);
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to parse sequence number '{}' from basename: {}", captures[sequence_idx], basename);
            return std::nullopt;
        }
    }
    else
    {
        // No ordering regex - use empty/zero defaults (lexicographic fallback)
        result.timestamp = "";
        result.sequence_number = 0;
    }

    LOG_TRACE(
        log,
        "Parsed file: path={}, basename={}, partition={}, timestamp={}, sequence={}",
        file_name,
        basename,
        result.partition_key,
        result.timestamp,
        result.sequence_number);

    return result;
}

}
