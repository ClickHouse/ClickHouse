#include <Storages/PartitionedFilepathGenerator.h>
#include <Storages/PartitionedSink.h>
#include <Functions/generateSnowflakeID.h>
#include <boost/algorithm/string/replace.hpp>
#include <Poco/String.h>

namespace DB
{
SnowflakePartitionedFilePathGenerator::SnowflakePartitionedFilePathGenerator(const std::string & file_format_)
    : file_format(file_format_)
{}

std::string SnowflakePartitionedFilePathGenerator::generateReadingPath(const std::string & root) const
{
    return root + "**." + Poco::toLower(file_format);
}
std::string SnowflakePartitionedFilePathGenerator::generateWritingPath(const std::string & root, const std::string & partition_key) const
{
    std::string path;

    if (!root.empty())
    {
        path += root + "/";
    }

    /*
     * File extension is toLower(format)
     * This isn't ideal, but I guess multiple formats can be specified and introduced.
     * So I think it is simpler to keep it this way.
     *
     * Or perhaps implement something like `IInputFormat::getFileExtension()`
     */
    return path + partition_key + "/" + std::to_string(generateSnowflakeID()) + "." + Poco::toLower(file_format);
}

std::string WildcardPartitionedFilePathGenerator::generateReadingPath(const std::string & root) const
{
    // todo perhaps it should be as simple as:
    // return boost::replace_all_copy(root, PartitionedSink::PARTITION_ID_WILDCARD, "**");
    return root;
}
std::string WildcardPartitionedFilePathGenerator::generateWritingPath(const std::string & root, const std::string & partition_key) const
{
    return boost::replace_all_copy(root, PartitionedSink::PARTITION_ID_WILDCARD, partition_key);
}

}
