#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Disks/IDisk.h>

#include <Interpreters/Context_fwd.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>

#include <Poco/Util/AbstractConfiguration.h>

namespace fs = std::filesystem;

namespace DB
{

std::vector<String> split(const String & text, const String & delimiters);

using ProgramOptionsDescription = boost::program_options::options_description;
using CommandLineOptions = boost::program_options::variables_map;

class DiskWithPath
{
public:
    explicit DiskWithPath(DiskPtr disk_, std::optional<String> path_ = std::nullopt);

    String getAbsolutePath(const String & any_path) const { return normalizePath(fs::path(path) / any_path); }

    String getCurrentPath() const { return path; }

    bool isDirectory(const String & any_path) const
    {
        return disk->existsDirectory(getRelativeFromRoot(any_path)) || (getRelativeFromRoot(any_path).empty() && (disk->existsDirectory("/")));
    }

    std::vector<String> listAllFilesByPath(const String & any_path) const;

    /// If ignore_exception is true, then the function will not throw an exception but can return incomplete result (though all returned paths are valid). This is useful for autocomplete which should not fail if there are any problems with disks.
    std::vector<String> getAllFilesByPrefix(const String & prefix, bool ignore_exception) const;

    DiskPtr getDisk() const { return disk; }

    void setPath(const String & any_path);

    String getRelativeFromRoot(const String & any_path) const { return normalizePathAndGetAsRelative(getAbsolutePath(any_path)); }

private:
    static String validatePathAndGetAsRelative(const String & path);
    static std::string normalizePathAndGetAsRelative(const std::string & messyPath);
    static std::string normalizePath(const std::string & messyPath);

    const DiskPtr disk;
    String path;
};

class DisksClient
{
    inline static const String DEFAULT_DISK_NAME = "default";
    inline static const String LOCAL_DISK_NAME = "local";
    using DiskCreator = std::function<DiskPtr()>;

public:
    DisksClient(const Poco::Util::AbstractConfiguration & config_, ContextPtr context_);

    const DiskWithPath & getDiskWithPath(const String & disk) const;

    DiskWithPath & getDiskWithPath(const String & disk);

    DiskWithPath & getDiskWithPathLazyInitialization(const String & disk);

    const DiskWithPath & getCurrentDiskWithPath() const;

    DiskWithPath & getCurrentDiskWithPath();

    DiskPtr getCurrentDisk() const { return getCurrentDiskWithPath().getDisk(); }

    DiskPtr getDisk(const String & disk) const { return getDiskWithPath(disk).getDisk(); }

    void switchToDisk(const String & disk_, const std::optional<String> & path_);

    std::vector<String> getInitializedDiskNames() const;
    std::vector<String> getUninitializedDiskNames() const;
    std::vector<String> getAllDiskNames() const;

    /// If ignore_exception is true, then the function will not throw an exception but can return incomplete result (though all returned paths are valid). This is useful for autocomplete which should not fail if there are any problems with disks.
    std::vector<String> getAllFilesByPrefixFromInitializedDisks(const String & prefix, bool ignore_exception) const;

    void addDisk(String disk_name, std::optional<String> path);

    bool isDiskInitialized(const String & disk_name) const { return initialized_disks.contains(disk_name); }

private:
    String current_disk;
    std::unordered_map<String, DiskWithPath> disks_with_paths;
    DisksMap initialized_disks;

    using PostponedDisksMap = std::map<String, std::pair<DiskCreator, std::optional<String>>>;
    PostponedDisksMap uninitialized_disks;

    const Poco::Util::AbstractConfiguration & config;
    ContextPtr context;
    LoggerPtr log;
};
}
