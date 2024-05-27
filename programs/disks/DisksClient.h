#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Client/ReplxxLineReader.h>
#include <Loggers/Loggers.h>
#include "Disks/IDisk.h"

#include <Interpreters/Context.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include "Common/Exception.h"

// #include <boost/program_options.hpp>
namespace fs = std::filesystem;

namespace DB
{

std::vector<String> split(const String & text, const String & delimiters);

using ProgramOptionsDescription = boost::program_options::options_description;
using CommandLineOptions = boost::program_options::variables_map;


namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
};

class DiskWithPath
{
public:
    explicit DiskWithPath(DiskPtr disk_, std::optional<String> path_ = std::nullopt)
        : disk(disk_)
        , path(
              [&]()
              {
                  if (path_.has_value())
                  {
                      if (!fs::path{path_.value()}.is_absolute())
                      {
                          throw Exception(ErrorCodes::BAD_ARGUMENTS, "Initializing path {} is not absolute", path_.value());
                      }
                      return path_.value();
                  }
                  else
                  {
                      return String{"/"};
                  }
              }())
    {
        if (!disk->isDirectory(normalizePathAndGetAsRelative(path)))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Initializing path {} at disk {} is not a directory", path, disk->getName());
        }
    }

    String getAbsolutePath(const String & any_path) const { return normalizePath(fs::path(path) / any_path); }

    String getCurrentPath() const { return path; }

    bool isDirectory(const String & any_path) const { return disk->isDirectory(getRelativeFromRoot(any_path)); }

    std::vector<String> listAllFilesByPath(const String & any_path) const
    {
        if (isDirectory(any_path))
        {
            std::vector<String> file_names;
            disk->listFiles(getRelativeFromRoot(any_path), file_names);
            return file_names;
        }
        else
        {
            return {};
        }
    }

    std::vector<String> getAllFilesByPattern(std::string pattern) const
    {
        auto [path_before, path_after] = [&]() -> std::pair<String, String>
        {
            auto slash_pos = pattern.find_last_of('/');
            if (slash_pos >= pattern.size())
            {
                return {"", pattern};
            }
            else
            {
                return {pattern.substr(0, slash_pos + 1), pattern.substr(slash_pos + 1, pattern.size() - slash_pos - 1)};
            }
        }();

        if (!isDirectory(path_before))
        {
            return {};
        }
        else
        {
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
        }
    }

    DiskPtr getDisk() const { return disk; }

    void setPath(const String & any_path)
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

    String getRelativeFromRoot(const String & any_path) const { return normalizePathAndGetAsRelative(getAbsolutePath(any_path)); }

private:
    static String validatePathAndGetAsRelative(const String & path)
    {
        String lexically_normal_path = fs::path(path).lexically_normal();
        if (lexically_normal_path.find("..") != std::string::npos)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Path {} is not normalized", path);

        /// If path is absolute we should keep it as relative inside disk, so disk will look like
        /// an ordinary filesystem with root.
        if (fs::path(lexically_normal_path).is_absolute())
            return lexically_normal_path.substr(1);

        return lexically_normal_path;
    }

    static std::string normalizePathAndGetAsRelative(const std::string & messyPath)
    {
        std::filesystem::path path(messyPath);
        std::filesystem::path canonical_path = std::filesystem::weakly_canonical(path);
        std::string npath = canonical_path.make_preferred().string();
        return validatePathAndGetAsRelative(npath);
    }

    static std::string normalizePath(const std::string & messyPath)
    {
        std::filesystem::path path(messyPath);
        std::filesystem::path canonical_path = std::filesystem::weakly_canonical(path);
        return canonical_path.make_preferred().string();
    }

    const DiskPtr disk;
    String path;
};

class DisksClient
{
public:
    explicit DisksClient(std::vector<std::pair<DiskPtr, std::optional<String>>> && disks_with_paths, std::optional<String> begin_disk)
    {
        if (disks_with_paths.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Initializing array of disks is empty");
        }
        if (!begin_disk.has_value())
        {
            begin_disk = disks_with_paths[0].first->getName();
        }
        bool has_begin_disk = true;
        for (auto & [disk, path] : disks_with_paths)
        {
            addDisk(disk, path);
            if (disk->getName() == begin_disk.value())
            {
                has_begin_disk = true;
            }
        }
        if (!has_begin_disk)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no begin_disk '{}' in initializing array", begin_disk.value());
        }
        current_disk = std::move(begin_disk.value());
    }

    const DiskWithPath & getDiskWithPath(const String & disk) const
    {
        try
        {
            return disks.at(disk);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown", disk);
        }
    }

    DiskWithPath & getDiskWithPath(const String & disk)
    {
        try
        {
            return disks.at(disk);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown", disk);
        }
    }

    const DiskWithPath & getCurrentDiskWithPath() const
    {
        try
        {
            return disks.at(current_disk);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no current disk in client");
        }
    }

    DiskWithPath & getCurrentDiskWithPath()
    {
        try
        {
            return disks.at(current_disk);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no current disk in client");
        }
    }

    DiskPtr getCurrentDisk() const { return getCurrentDiskWithPath().getDisk(); }

    DiskPtr getDisk(const String & disk) const { return getDiskWithPath(disk).getDisk(); }

    bool switchToDisk(const String & disk_, const std::optional<String> & path_)
    {
        if (disks.contains(disk_))
        {
            if (path_.has_value())
            {
                disks.at(disk_).setPath(path_.value());
            }
            current_disk = disk_;
            return true;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' is unknown", disk_);
        }
    }

    std::vector<String> getAllDiskNames() const
    {
        std::vector<String> answer{};
        answer.reserve(disks.size());
        for (const auto & [disk_name, _] : disks)
        {
            answer.push_back(disk_name);
        }
        return answer;
    }

    std::vector<String> getAllFilesByPatternFromAllDisks(std::string pattern) const
    {
        std::vector<String> answer{};
        for (const auto & [_, disk] : disks)
        {
            for (auto & word : disk.getAllFilesByPattern(pattern))
            {
                answer.push_back(word);
            }
        }
        return answer;
    }

private:
    void addDisk(DiskPtr disk_, const std::optional<String> & path_)
    {
        String disk_name = disk_->getName();
        if (disks.contains(disk_->getName()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk '{}' already exists", disk_name);
        }
        disks.emplace(disk_name, DiskWithPath{disk_, path_});
    }

    String current_disk;
    std::unordered_map<String, DiskWithPath> disks;
};
}
