#pragma once

#include <string>
#include <Storages/PartitionedSink.h>
#include <Poco/String.h>
#include <Functions/generateSnowflakeID.h>
#include <boost/algorithm/string/replace.hpp>

namespace DB
{
    struct ObjectStorageFilePathGenerator
    {
        virtual ~ObjectStorageFilePathGenerator() = default;
        std::string getPathForWrite(const std::string & partition_id) const { 
            return getPathForWrite(partition_id, "");
        }
        virtual std::string getPathForWrite(const std::string & partition_id, const std::string & /* file_name_override */) const = 0;
        virtual std::string getPathForRead() const = 0;
    };

    struct ObjectStorageWildcardFilePathGenerator : ObjectStorageFilePathGenerator
    {
        static constexpr const char * FILE_WILDCARD = "{_file}";
        explicit ObjectStorageWildcardFilePathGenerator(const std::string & raw_path_) : raw_path(raw_path_) {}

        using ObjectStorageFilePathGenerator::getPathForWrite;  // Bring base class overloads into scope
        std::string getPathForWrite(const std::string & partition_id, const std::string & file_name_override) const override
        {
            const auto partition_replaced_path = PartitionedSink::replaceWildcards(raw_path, partition_id);
            const auto final_path = boost::replace_all_copy(partition_replaced_path, FILE_WILDCARD, file_name_override);
            return final_path;
        }

        std::string getPathForRead() const override
        {
            return raw_path;
        }

    private:
        std::string raw_path;

    };

    struct ObjectStorageAppendFilePathGenerator : ObjectStorageFilePathGenerator
    {
        explicit ObjectStorageAppendFilePathGenerator(
            const std::string & raw_path_,
            const std::string & file_format_)
        : raw_path(raw_path_), file_format(Poco::toLower(file_format_)){}

        using ObjectStorageFilePathGenerator::getPathForWrite;  // Bring base class overloads into scope
        std::string getPathForWrite(const std::string & partition_id, const std::string & file_name_override) const override
        {
            std::string result;

            result += raw_path;

            if (!raw_path.empty() && raw_path.back() != '/')
            {
                result += "/";
            }

            /// Not adding '/' because buildExpressionHive() always adds a trailing '/'
            result += partition_id;

            const auto file_name = file_name_override.empty() ? std::to_string(generateSnowflakeID()) : file_name_override;

            result += file_name + "." + file_format;

            return result;
        }

        std::string getPathForRead() const override
        {
            return raw_path + "**." + file_format;
        }

    private:
        std::string raw_path;
        std::string file_format;
    };

}
