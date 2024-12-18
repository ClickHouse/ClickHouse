#include "DisksClient.h"
#include <Client/ClientBase.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskLocal.h>
#include <Disks/registerDisks.h>
#include <Common/Config/ConfigProcessor.h>
#include "Disks/IDisk.h"
#include "base/types.h"

#include <Formats/registerFormats.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_DISK;
    }

namespace DB
{
DiskWithPath::DiskWithPath(DiskPtr disk_, std::optional<String> path_) : disk(disk_)
{
    if (path_.has_value())
    {
        if (!fs::path{path_.value()}.is_absolute())
        {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Initializing path {} is not absolute", path_.value());
        }
        path = path_.value();
    }
    else
    {
        path = String{"/"};
    }

    String relative_path = normalizePathAndGetAsRelative(path);
    if (disk->existsDirectory(relative_path) || (relative_path.empty() && (disk->existsDirectory("/"))))
    {
        return;
    }
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Initializing path {} (normalized path: {}) at disk {} is not a directory",
        path,
        relative_path,
        disk->getName());
}

std::vector<String> DiskWithPath::listAllFilesByPath(const String & any_path) const
{
    if (isDirectory(any_path))
    {
        std::vector<String> file_names;
        disk->listFiles(getRelativeFromRoot(any_path), file_names);
        return file_names;
    }

    return {};
}

std::vector<String> DiskWithPath::getAllFilesByPattern(const String & pattern) const
{
    auto [path_before, path_after] = [&]() -> std::pair<String, String>
    {
        auto slash_pos = pattern.find_last_of('/');
        if (slash_pos >= pattern.size())
            return {"", pattern};

        return {pattern.substr(0, slash_pos + 1), pattern.substr(slash_pos + 1, pattern.size() - slash_pos - 1)};
    }();

    if (!isDirectory(path_before))
        return {};

    std::vector<String> file_names = listAllFilesByPath(path_before);
    std::vector<String> answer;

    for (const auto & file_name : file_names)
    {
        if (file_name.starts_with(path_after))
        {
            String file_pattern = path_before + file_name;
            if (isDirectory(file_pattern))
            {
                file_pattern = file_pattern + "/";
            }
            answer.push_back(file_pattern);
        }
    }
    return answer;
};

void DiskWithPath::setPath(const String & any_path)
{
    if (isDirectory(any_path))
    {
        path = getAbsolutePath(any_path);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} at disk {} is not a directory", any_path, disk->getName());
    }
}

String DiskWithPath::validatePathAndGetAsRelative(const String & path)
{
    String lexically_normal_path = fs::path(path).lexically_normal();
    if (lexically_normal_path.contains(".."))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Path {} is not normalized", path);

    /// If path is absolute we should keep it as relative inside disk, so disk will look like
    /// an ordinary filesystem with root.
    if (fs::path(lexically_normal_path).is_absolute())
        return lexically_normal_path.substr(1);

    return lexically_normal_path;
}

String DiskWithPath::normalizePathAndGetAsRelative(const String & messyPath)
{
    std::filesystem::path path(messyPath);
    std::filesystem::path canonical_path = std::filesystem::weakly_canonical(path);
    String npath = canonical_path.make_preferred().string();
    return validatePathAndGetAsRelative(npath);
}

String DiskWithPath::normalizePath(const String & path)
{
    std::filesystem::path canonical_path = std::filesystem::weakly_canonical(path);
    return canonical_path.make_preferred().string();
}

DisksClient::DisksClient(const Poco::Util::AbstractConfiguration & config_, ContextPtr context_)
{
    String begin_disk = config_.getString("disk", "default");
    String config_prefix = "storage_configuration.disks";
    Poco::Util::AbstractConfiguration::Keys keys;
    config_.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    bool has_default_disk = false;
    bool has_local_disk = false;
    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_name);

        if (disk_name == DEFAULT_DISK_NAME)
            has_default_disk = true;

        if (disk_name == LOCAL_DISK_NAME)
            has_local_disk = true;

        const auto disk_config_prefix = config_prefix + "." + disk_name;

        postponed_disks.emplace(
            disk_name,
            [disk_name, disk_config_prefix, &factory, &config_, context_, this]()
            {
                return factory.create(
                    disk_name,
                    config_,
                    disk_config_prefix,
                    context_,
                    created_disks,
                    /*attach*/ false,
                    /*custom_disk*/ false,
                    {"cache", "encrypted"});
            });
    }
    if (!has_default_disk)
    {
        postponed_disks.emplace(
            DEFAULT_DISK_NAME,
            [context_, &config_, config_prefix]()
            { return std::make_shared<DiskLocal>(DEFAULT_DISK_NAME, context_->getPath(), 0, context_, config_, config_prefix); });
    }

    if (!has_local_disk)
    {
        postponed_disks.emplace(
            LOCAL_DISK_NAME,
            [context_, &config_, config_prefix]()
            { return std::make_shared<DiskLocal>(LOCAL_DISK_NAME, "/", 0, context_, config_, config_prefix); });
    }
}

const DiskWithPath & DisksClient::getDiskWithPath(const String & disk) const
{
    try
    {
        return disks_with_paths.at(disk);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown or uninitialized", disk);
    }
}

DiskWithPath & DisksClient::getDiskWithPath(const String & disk)
{
    try
    {
        return disks_with_paths.at(disk);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown or uninitialized", disk);
    }
}

const DiskWithPath & DisksClient::getCurrentDiskWithPath() const
{
    try
    {
        return disks_with_paths.at(current_disk);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no current disk in client");
    }
}

DiskWithPath & DisksClient::getCurrentDiskWithPath()
{
    try
    {
        return disks_with_paths.at(current_disk);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no current disk in client");
    }
}

void DisksClient::switchToDisk(const String & disk_, const std::optional<String> & path_)
{
    if (disks_with_paths.contains(disk_))
    {
        if (path_.has_value())
        {
            disks_with_paths.at(disk_).setPath(path_.value());
        }
        current_disk = disk_;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown or unitialized", disk_);
    }
}

std::vector<String> DisksClient::getInitializedDiskNames() const
{
    std::vector<String> answer{};
    answer.reserve(created_disks.size());
    for (const auto & [disk_name, _] : created_disks)
    {
        answer.push_back(disk_name);
    }
    return answer;
}

std::vector<String> DisksClient::getUninitializedDiskNames() const
{
    std::vector<String> answer{};
    answer.reserve(created_disks.size());
    for (const auto & [disk_name, _] : postponed_disks)
    {
        answer.push_back(disk_name);
    }
    return answer;
}


std::vector<String> DisksClient::getAllDiskNames() const
{
    std::vector<String> answer(getInitializedDiskNames());
    std::vector<String> uninitialized_disks = getUninitializedDiskNames();
    answer.insert(answer.end(), std::make_move_iterator(uninitialized_disks.begin()), std::make_move_iterator(uninitialized_disks.end()));
    return answer;
}


std::vector<String> DisksClient::getAllFilesByPatternFromAllDisks(const String & pattern) const
{
    std::vector<String> answer{};
    for (const auto & [_, disk] : disks_with_paths)
    {
        for (auto & word : disk.getAllFilesByPattern(pattern))
        {
            answer.push_back(word);
        }
    }
    return answer;
}

void DisksClient::addDisk(String disk_name, const std::optional<String> & path)
{
    if (created_disks.contains(disk_name))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk {} already exists", disk_name);
    }
    if (!postponed_disks.contains(disk_name))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown", disk_name);
    }

    DiskPtr disk = postponed_disks.at(disk_name)();
    chassert(disk_name == disk->getName());
    created_disks.emplace(disk_name, disk);
    postponed_disks.erase(disk_name);
    disks_with_paths.emplace(disk_name, DiskWithPath{disk, path});
}
}
