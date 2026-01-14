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
    struct ParsedFilename
    {
        /// example:c-aquamarine-xj-70-server-5mhfwzb-0_2025-11-10T07-45-33.123456Z_0000000001
        std::string original_file_name;
        /// example: 20251110T101234.000001Z
        std::string timestamp;
        /// example: 0000000001
        UInt64 sequence_number;
        /// example: c-aquamarine-xj-70-server-5mhfwzb-0
        std::string partition_key;


        /// Comparison operator for ordering: partition → timestamp → sequence
        bool operator<(const ParsedFilename & other) const
        {
            if (partition_key != other.partition_key)
                return partition_key < other.partition_key;
            if (timestamp != other.timestamp)
                return timestamp < other.timestamp;
            return sequence_number < other.sequence_number;
        }

        /// Check if same partition (for bucket assignment)
        bool isSamePartition(const ParsedFilename & other) const { return partition_key == other.partition_key; }
    };

    /// Construct parser with regex patterns
    /// @param partition_regex - Regex to extract partition key (use named group 'partition' or first capture group)
    /// @param ordering_components_regex - Regex to extract 'timestamp' and 'sequence' named groups
    ObjectStorageQueueFilenameParser(const std::string & partition_regex, const std::string & ordering_components_regex);

    /// Parse filename and extract partition/ordering information
    /// @returns ParsedFilename if successful, std::nullopt if parsing fails
    std::optional<ParsedFilename> parse(const std::string & file_name) const;

    /// Check if parser is valid (regexes compiled successfully)
    bool isValid() const { return partition_pattern && ordering_components_pattern; }

    /// Get error message if parser is invalid
    std::string getError() const { return error_message; }

private:
    std::unique_ptr<re2::RE2> partition_pattern;
    std::unique_ptr<re2::RE2> ordering_components_pattern;
    std::string error_message;

    LoggerPtr log;
};

}
