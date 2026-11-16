#pragma once

#include <memory>
#include <optional>
#include <string>

#include <base/types.h>
#include <Common/re2.h>
#include <Common/Logger.h>


namespace DB
{

class ObjectStorageQueueFilenameParser
{
public:
    /// Construct parser with regex pattern
    /// @param partition_regex - Regex to extract named capture groups from filename
    /// @param partition_component - Name of capture group to use as partition key (required, must be non-empty)
    explicit ObjectStorageQueueFilenameParser(const std::string & partition_regex, const std::string & partition_component);

    /// Parse filename and extract partition key
    /// @returns partition key if successful, std::nullopt if parsing fails
    std::optional<std::string> parse(const std::string & file_name) const;

    /// Check if parser is valid (regex compiled successfully)
    bool isValid() const { return partition_pattern != nullptr; }

    /// Get error message if parser is invalid
    std::string getError() const { return error_message; }

private:
    std::unique_ptr<re2::RE2> partition_pattern;
    std::string partition_component_name;
    std::string error_message;

    LoggerPtr log;
};

}
