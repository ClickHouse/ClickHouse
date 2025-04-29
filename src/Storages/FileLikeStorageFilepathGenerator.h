#pragma once

#include <string>

namespace DB
{
    struct FileLikeStorageFilepathGenerator
    {
        virtual ~FileLikeStorageFilepathGenerator() = default;
        virtual std::string generateReadingPath(const std::string & ) const = 0;
        virtual std::string generateWritingPath(const std::string & root, const std::string & partition_key) const = 0;
    };

    struct NoOpFileLikeStoragePathGenerator : FileLikeStorageFilepathGenerator
    {
        std::string generateReadingPath(const std::string & root) const override { return root; }
        std::string generateWritingPath(const std::string & root, const std::string &) const override { return root; }
    };

    struct SnowflakeFileLikeStoragePathGenerator : FileLikeStorageFilepathGenerator
    {
        explicit SnowflakeFileLikeStoragePathGenerator(const std::string & file_format_);

        std::string generateReadingPath(const std::string & ) const override;
        std::string generateWritingPath(const std::string & root, const std::string & partition_key) const override;

    private:
        std::string file_format;
    };

    struct WildcardFileLikeStoragePathGenerator : FileLikeStorageFilepathGenerator
    {
        std::string generateReadingPath(const std::string & root) const override;
        std::string generateWritingPath(const std::string & root, const std::string & partition_key) const override;
    };

}

