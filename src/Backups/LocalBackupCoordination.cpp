#include <Backups/LocalBackupCoordination.h>
#include <fmt/format.h>


namespace DB
{
using FileInfo = IBackupCoordination::FileInfo;

LocalBackupCoordination::LocalBackupCoordination() = default;
LocalBackupCoordination::~LocalBackupCoordination() = default;

void LocalBackupCoordination::addFileInfo(const FileInfo & file_info, bool & is_new_checksum)
{
    std::lock_guard lock{mutex};
    file_names.emplace(file_info.file_name, file_info.checksum);
    is_new_checksum = (file_info.checksum && !file_infos.contains(file_info.checksum));
    if (is_new_checksum)
        file_infos.emplace(file_info.checksum, std::move(file_info));
}

void LocalBackupCoordination::updateFileInfo(const FileInfo & file_info)
{
    std::lock_guard lock{mutex};
    auto & dest = file_infos.at(file_info.checksum);
    dest.archive_suffix = file_info.archive_suffix;
}

std::vector<FileInfo> LocalBackupCoordination::getAllFileInfos()
{
    std::lock_guard lock{mutex};
    std::vector<FileInfo> res;
    for (const auto & [file_name, checksum] : file_names)
    {
        FileInfo info = file_infos.at(checksum);
        info.file_name = file_name;
        res.push_back(std::move(info));
    }
    return res;
}

Strings LocalBackupCoordination::listFiles(const String & prefix, const String & terminator)
{
    std::lock_guard lock{mutex};
    Strings elements;
    for (auto it = file_names.lower_bound(prefix); it != file_names.end(); ++it)
    {
        const String & name = it->first;
        if (!name.starts_with(prefix))
            break;
        size_t start_pos = prefix.length();
        size_t end_pos = String::npos;
        if (!terminator.empty())
            end_pos = name.find(terminator, start_pos);
        std::string_view new_element = std::string_view{name}.substr(start_pos, end_pos - start_pos);
        if (!elements.empty() && (elements.back() == new_element))
            continue;
        elements.push_back(String{new_element});
    }
    return elements;
}

std::optional<UInt128> LocalBackupCoordination::getChecksumByFileName(const String & file_name)
{
    std::lock_guard lock{mutex};
    auto it = file_names.find(file_name);
    if (it == file_names.end())
        return std::nullopt;
    return it->second;
}

std::optional<FileInfo> LocalBackupCoordination::getFileInfoByChecksum(const UInt128 & checksum)
{
    std::lock_guard lock{mutex};
    auto it = file_infos.find(checksum);
    if (it == file_infos.end())
        return std::nullopt;
    return it->second;
}

std::optional<FileInfo> LocalBackupCoordination::getFileInfoByFileName(const String & file_name)
{
    std::lock_guard lock{mutex};
    auto it = file_names.find(file_name);
    if (it == file_names.end())
        return std::nullopt;
    FileInfo info = file_infos.at(it->second);
    info.file_name = file_name;
    return info;
}

String LocalBackupCoordination::getNextArchiveSuffix()
{
    String new_archive_suffix = fmt::format("{:03}", ++current_archive_suffix); /// Outputs 001, 002, 003, ...
    archive_suffixes.push_back(new_archive_suffix);
    return new_archive_suffix;
}

Strings LocalBackupCoordination::getAllArchiveSuffixes()
{
    std::lock_guard lock{mutex};
    return archive_suffixes;
}

}
