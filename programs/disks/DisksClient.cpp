#include <DisksClient.h>
#include <optional>
#include <Client/ClientBase.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskLocal.h>
#include <Disks/registerDisks.h>
#include <Common/Config/ConfigProcessor.h>
#include <Disks/IDisk.h>
#include <base/types.h>

#include <Formats/registerFormats.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>

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
    else
    {
        path = String{"/"};
    }

    String relative_path = normalizePathAndGetAsRelative(path);
    if (disk->existsDirectory(relative_path) || relative_path.empty())
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

std::vector<String> DiskWithPath::getAllFilesByPrefix(const String & prefix, bool ignore_exception) const
{
    std::vector<String> answer;
    try
    {
        auto [path_before, path_after] = [&]() -> std::pair<String, String>
        {
            auto slash_pos = prefix.find_last_of('/');
            if (slash_pos == String::npos)
                return {"", prefix};

            std::filesystem::path pattern_path{prefix};
            return {pattern_path.parent_path(), pattern_path.filename()};
        }();

        if (!isDirectory(path_before))
            return {};

        std::vector<String> file_names = listAllFilesByPath(path_before);

        for (const auto & file_name : file_names)
        {
            if (file_name.starts_with(path_after))
            {
                String file_pattern = path_before + file_name;
                if (!file_pattern.ends_with('/') && isDirectory(file_pattern))
                {
                    file_pattern = file_pattern + "/";
                }
                answer.push_back(file_pattern);
            }
        }
        return answer;
    }
    catch (...)
    {
        if (!ignore_exception)
            throw;
    }
    return answer;
};

void DiskWithPath::setPath(const String & any_path)
{
    if (isDirectory(any_path))
    {
        LOG_INFO(&Poco::Logger::get("DisksClient"), "Setting path to '{}' at disk '{}'", any_path, disk->getName());
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
    : config(config_), context(std::move(context_)), log(getLogger("DisksClient"))
{
    String begin_disk = config.getString("disk", DEFAULT_DISK_NAME);
    String config_prefix = "storage_configuration.disks";
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

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

        uninitialized_disks.emplace(
            disk_name,
            std::pair{
                [disk_name, disk_config_prefix, &factory, this]()
                {
                    return factory.create(
                        disk_name,
                        config,
                        disk_config_prefix,
                        this->context,
                        initialized_disks,
                        /*attach*/ false,
                        /*custom_disk*/ false,
                        {"cache", "encrypted"});
                },
                std::nullopt});
    }
    if (!has_default_disk)
    {
        uninitialized_disks.emplace(
            DEFAULT_DISK_NAME,
            std::pair{
                [this, config_prefix]() -> DiskPtr
                {
                    return std::make_shared<DiskLocal>(
                        DEFAULT_DISK_NAME, this->context->getPath(), 0, this->context, this->config, config_prefix);
                },
                std::nullopt});
    }

    if (!has_local_disk)
    {
        /// We mount the local disk at the root of the file system so it can be used as a local filesystem within the Disks app. Please note that the initial path matches the path from which you launch the Disks app, unless it is explicitly specified in the switch-disk command.
        uninitialized_disks.emplace(
            LOCAL_DISK_NAME,
            std::pair{
                [this, config_prefix]()
                { return std::make_shared<DiskLocal>(LOCAL_DISK_NAME, "/", 0, this->context, this->config, config_prefix); },
                fs::current_path().string()});
    }

    addDisk(begin_disk, std::nullopt);
    switchToDisk(begin_disk, std::nullopt);
}

const DiskWithPath & DisksClient::getDiskWithPath(const String & disk) const
{
    try
    {
        return disks_with_paths.at(disk);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::UNKNOWN_DISK, "The disk '{}' is unknown or uninitialized", disk);
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
        throw Exception(ErrorCodes::UNKNOWN_DISK, "The disk '{}' is unknown or uninitialized", disk);
    }
}

DiskWithPath & DisksClient::getDiskWithPathLazyInitialization(const String & disk)
{
    addDisk(disk, std::nullopt);
    try
    {
        return disks_with_paths.at(disk);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::UNKNOWN_DISK, "The disk '{}' is unknown or uninitialized", disk);
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
            LOG_INFO(log, "Switching to disk '{}' with path '{}'", disk_, path_.value());
        }
        else
        {
            LOG_INFO(log, "Switching to disk '{}' with default path", disk_);
        }
        current_disk = disk_;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown or uninitialized", disk_);
    }
}

std::vector<String> DisksClient::getInitializedDiskNames() const
{
    std::vector<String> answer{};
    answer.reserve(initialized_disks.size());
    for (const auto & [disk_name, _] : initialized_disks)
    {
        answer.push_back(disk_name);
    }
    return answer;
}

std::vector<String> DisksClient::getUninitializedDiskNames() const
{
    std::vector<String> answer{};
    answer.reserve(initialized_disks.size());
    for (const auto & [disk_name, _] : uninitialized_disks)
    {
        answer.push_back(disk_name);
    }
    return answer;
}


std::vector<String> DisksClient::getAllDiskNames() const
{
    std::vector<String> answer(getInitializedDiskNames());
    std::vector<String> uninitialized_disks_names = getUninitializedDiskNames();
    answer.insert(
        answer.end(), std::make_move_iterator(uninitialized_disks_names.begin()), std::make_move_iterator(uninitialized_disks_names.end()));
    return answer;
}


std::vector<String> DisksClient::getAllFilesByPrefixFromInitializedDisks(const String & prefix, bool ignore_exception) const
{
    std::vector<String> answer{};
    for (const auto & [_, disk] : disks_with_paths)
    {
        try
        {
            for (auto & word : disk.getAllFilesByPrefix(prefix, ignore_exception))
            {
                answer.push_back(word);
            }
        }
        catch (...)
        {
            if (!ignore_exception)
                throw;
        }
    }
    return answer;
}

void DisksClient::addDisk(String disk_name, std::optional<String> path)
{
    if (initialized_disks.contains(disk_name))
    {
        return;
    }
    if (!uninitialized_disks.contains(disk_name))
    {
        throw Exception(ErrorCodes::UNKNOWN_DISK, "The disk '{}' is unknown and can't be initialized", disk_name);
    }

    DiskPtr disk = uninitialized_disks.at(disk_name).first();
    chassert(disk_name == disk->getName());
    if (!path.has_value())
    {
        path = uninitialized_disks.at(disk_name).second;
    }
    auto disk_with_path = DiskWithPath{disk, path};

    LOG_INFO(log, "Adding disk '{}' with path '{}'", disk_name, path.value_or(""));

    /// This block of code should not throw exceptions to preserve the program's invariants. We hope that no exceptions occur here.
    try
    {
        initialized_disks.emplace(disk_name, disk);
        uninitialized_disks.erase(disk_name);
        disks_with_paths.emplace(disk_name, std::move(disk_with_path));
    }
    catch (...)
    {
        LOG_FATAL(log, "Disk '{}' was not created, which led to broken invariants in the program", disk_name);
        std::terminate();
    }
}
}
