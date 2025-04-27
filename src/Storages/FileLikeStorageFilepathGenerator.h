#pragma once

#include <string>

namespace DB
{
    struct PartitionedFilepathGenerator
    {
        virtual ~PartitionedFilepathGenerator() = default;
        virtual std::string generateReadingPath(const std::string & ) const = 0;
        virtual std::string generateWritingPath(const std::string & root, const std::string & partition_key) const = 0;
    };

    struct SnowflakePartitionedFilePathGenerator : PartitionedFilepathGenerator
    {
        explicit SnowflakePartitionedFilePathGenerator(const std::string & file_format_);

        std::string generateReadingPath(const std::string & ) const override;
        std::string generateWritingPath(const std::string & root, const std::string & partition_key) const override;

    private:
        std::string file_format;
    };

    struct WildcardPartitionedFilePathGenerator : PartitionedFilepathGenerator
    {
        std::string generateReadingPath(const std::string & root) const override;
        std::string generateWritingPath(const std::string & root, const std::string & partition_key) const override;
    };

}

