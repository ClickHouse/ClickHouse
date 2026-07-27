#include <Backups/BackupCoordinationFileInfos.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_ENTRY_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

using SizeAndChecksum = std::pair<UInt64, UInt128>;


void BackupCoordinationFileInfos::addFileInfos(BackupFileInfos && file_infos_, const String & host_id_)
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addFileInfos() must not be called after preparing");
    file_infos.emplace(host_id_, std::move(file_infos_));
}

BackupFileInfos BackupCoordinationFileInfos::getFileInfos(const String & host_id_) const
{
    prepare();
    auto it = file_infos.find(host_id_);
    if (it == file_infos.end())
        return {};
    return it->second;
}

BackupFileInfos BackupCoordinationFileInfos::getFileInfosForAllHosts() const
{
    prepare();
    BackupFileInfos res;
    res.reserve(file_infos_for_all_hosts.size());
    for (const auto * file_info : file_infos_for_all_hosts)
        res.emplace_back(*file_info);
    return res;
}

BackupFileInfo BackupCoordinationFileInfos::getFileInfoByDataFileIndex(size_t data_file_index) const
{
    prepare();
    if (data_file_index >= file_infos_for_all_hosts.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid data file index: {}", data_file_index);
    return *(file_infos_for_all_hosts[data_file_index]);
}

namespace
{

/// copy all the file infos that are shared between reference target and source
void copyFileInfoToReference(const BackupFileInfo & target, BackupFileInfo & reference)
{
    reference.size = target.size;
    reference.checksum = target.checksum;
    reference.base_size = target.base_size;
    reference.base_checksum = target.base_checksum;
    reference.encrypted_by_disk = target.encrypted_by_disk;
}

}

void BackupCoordinationFileInfos::prepare() const
{
    if (prepared)
        return;

    /// Make a list of all file infos from all hosts.
    size_t total_num_infos = 0;
    for (const auto & [_, infos] : file_infos)
        total_num_infos += infos.size();

    file_infos_for_all_hosts.reserve(total_num_infos);
    for (auto & [_, infos] : file_infos)
        for (auto & info : infos)
            file_infos_for_all_hosts.emplace_back(&info);

    /// Sort the list of all file infos by file name (file names must be unique).
    std::sort(file_infos_for_all_hosts.begin(), file_infos_for_all_hosts.end(), BackupFileInfo::LessByFileName{});

    auto adjacent_it = std::adjacent_find(file_infos_for_all_hosts.begin(), file_infos_for_all_hosts.end(), BackupFileInfo::EqualByFileName{});
    if (adjacent_it != file_infos_for_all_hosts.end())
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Entry {} added multiple times to backup", quoteString((*adjacent_it)->file_name));
    }

    num_files = 0;
    total_size_of_files = 0;

    std::vector<BackupFileInfo *> unresolved_references;
    std::unordered_map<std::string_view, BackupFileInfo *> file_name_to_info;

    const auto handle_unresolved_references = [&](const auto & try_resolve_reference)
    {
        for (auto * reference : unresolved_references)
        {
            if (!try_resolve_reference(*reference))
                throw DB::Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Couldn't resolve reference {} with target {}",
                    reference->file_name,
                    reference->reference_target);
        }
    };

    if (plain_backup)
    {
        const auto try_resolve_reference = [&](BackupFileInfo & reference)
        {
            auto it = file_name_to_info.find(reference.reference_target);

            if (it == file_name_to_info.end())
                return false;

            auto & target_info = it->second;
            target_info->data_file_copies.push_back(reference.file_name);
            copyFileInfoToReference(*target_info, reference);
            total_size_of_files += reference.size;
            return true;
        };

        /// For plain backup all file infos are stored as is, without checking for duplicates or skipping empty files.
        for (size_t i = 0; i != file_infos_for_all_hosts.size(); ++i)
        {
            auto & info = *(file_infos_for_all_hosts[i]);
            info.data_file_name = info.file_name;
            info.data_file_index = i;
            info.base_size = 0; /// Base backup must not be used while creating a plain backup.
            info.base_checksum = 0;

            if (info.reference_target.empty())
            {
                file_name_to_info.emplace(info.file_name, &info);
                total_size_of_files += info.size;
            }
            else if (!try_resolve_reference(info))
            {
                unresolved_references.push_back(&info);
            }
        }

        handle_unresolved_references(try_resolve_reference);

        num_files = file_infos_for_all_hosts.size();
    }
    else
    {
        const auto try_resolve_reference = [&](BackupFileInfo & reference)
        {
            auto it = file_name_to_info.find(reference.reference_target);

            if (it == file_name_to_info.end())
                return false;

            auto & target_info = it->second;
            copyFileInfoToReference(*target_info, reference);
            reference.data_file_name = target_info->data_file_name;
            reference.data_file_index = target_info->data_file_index;
            return true;
        };

        /// For non-plain backups files with the same size and checksum are stored only once,
        /// in order to find those files we'll use this map.
        std::map<SizeAndChecksum, size_t> data_file_index_by_checksum;

        for (size_t i = 0; i != file_infos_for_all_hosts.size(); ++i)
        {
            auto & info = *(file_infos_for_all_hosts[i]);

            if (!info.reference_target.empty())
            {
                if (!try_resolve_reference(info))
                    unresolved_references.push_back(&info);

                continue;
            }

            if (info.size == info.base_size)
            {
                /// A file is either empty or can be get from the base backup as a whole.
                info.data_file_name.clear();
                info.data_file_index = static_cast<size_t>(-1);
            }
            else
            {
                SizeAndChecksum size_and_checksum{info.size, info.checksum};
                auto [it, inserted] = data_file_index_by_checksum.emplace(size_and_checksum, i);
                if (inserted)
                {
                    /// Found a new file.
                    info.data_file_name = info.file_name;
                    info.data_file_index = i;
                    ++num_files;
                    total_size_of_files += info.size - info.base_size;
                }
                else
                {
                    /// Found a file with the same size and checksum as some file before, reuse old `data_file_index` and `data_file_name`.
                    info.data_file_index = it->second;
                    info.data_file_name = file_infos_for_all_hosts[it->second]->data_file_name;
                }
            }

            file_name_to_info.emplace(info.file_name, &info);
        }

        handle_unresolved_references(try_resolve_reference);

        num_files = file_infos_for_all_hosts.size();
    }

    prepared = true;
}

size_t BackupCoordinationFileInfos::getNumFiles() const
{
    prepare();
    return num_files;
}

size_t BackupCoordinationFileInfos::getTotalSizeOfFiles() const
{
    prepare();
    return total_size_of_files;
}

}
