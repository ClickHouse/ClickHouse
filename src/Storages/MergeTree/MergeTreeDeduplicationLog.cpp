#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <filesystem>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>



namespace DB
{

namespace
{

std::string getLogPath(const std::string & prefix, size_t number)
{
    std::filesystem::path path(prefix);
    path /= std::filesystem::path(std::string{"deduplication_log_"} + std::to_string(number) + ".txt");
    return path;
}

size_t getLogNumber(const std::string & path_str)
{
    std::filesystem::path path(path_str);
    std::string filename = path.stem();
    Strings filename_parts;
    boost::split(filename_parts, filename, boost::is_any_of("_"));

    return parse<size_t>(filename_parts[2]);
}

}

MergeTreeDeduplicationLog::MergeTreeDeduplicationLog(
    const std::string & logs_dir_,
    size_t deduplication_window_,
    size_t rotate_interval_)
    : logs_dir(logs_dir_)
    , deduplication_window(deduplication_window_)
    , rotate_interval(rotate_interval_)
{}

void MergeTreeDeduplicationLog::load()
{
    namespace fs = std::filesystem;
    if (!fs::exists(logs_dir))
        fs::create_directories(logs_dir);

    for (const auto & p : fs::directory_iterator(logs_dir))
    {
        auto path = p.path();
        auto log_number = getLogNumber(path);
        existing_logs[log_description.log_number] = {path, 0};
    }
}

std::unordered_set<std::string> MergeTreeDeduplicationLog::loadSingleLog(const std::string & path)
{
    ReadBufferFromFile read_buf(path);

    while (!read_buf.eof())
    {
        readIntBinary(record_checksum, read_buf);
    }
}

void MergeTreeDeduplicationLog::rotate()
{
    size_t new_log_number = log_counter++;
    auto new_description = getLogDescription(logs_dir, new_log_number, rotate_interval);
    existing_logs.emplace(new_log_number, new_description);
    current_writer->sync();

    current_writer = std::make_unique<ChangelogWriter>(description.path, WriteMode::Append, description.from_log_index);

}

}
