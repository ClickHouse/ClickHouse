#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Loggers/Loggers.h>
#include <Disks/IDisk.h>

#include <Interpreters/Context.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>

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

    std::vector<String> getAllFilesByPattern(const String & pattern) const;

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
    explicit DisksClient(const Poco::Util::AbstractConfiguration & config, ContextPtr context);

    explicit DisksClient(std::vector<std::pair<DiskPtr, std::optional<String>>> && disks_with_paths, std::optional<String> begin_disk);

    const DiskWithPath & getDiskWithPath(const String & disk) const;

    DiskWithPath & getDiskWithPath(const String & disk);

    const DiskWithPath & getCurrentDiskWithPath() const;

    DiskWithPath & getCurrentDiskWithPath();

    DiskPtr getCurrentDisk() const { return getCurrentDiskWithPath().getDisk(); }

    DiskPtr getDisk(const String & disk) const { return getDiskWithPath(disk).getDisk(); }

    void switchToDisk(const String & disk_, const std::optional<String> & path_);

    std::vector<String> getInitializedDiskNames() const;
    std::vector<String> getUninitializedDiskNames() const;
    std::vector<String> getAllDiskNames() const;


    std::vector<String> getAllFilesByPatternFromAllDisks(const String & pattern) const;

    void addDisk(String disk_name, const std::optional<String> & path);

    bool isDiskInitialized(const String & disk_name) const { return created_disks.contains(disk_name); }

private:
    String current_disk;
    std::unordered_map<String, DiskWithPath> disks_with_paths;
    DisksMap created_disks;

    using PostponedDisksMap = std::map<String, DiskCreator>;
    PostponedDisksMap postponed_disks;
};
}
