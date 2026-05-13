#include <Interpreters/NamedScalars/NamedScalarDefinitionStoreLocal.h>

#include <Interpreters/NamedScalars/NamedScalar.h>
#include <Interpreters/NamedScalars/NamedScalarsManager.h>

#include <Common/Exception.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

#include <Core/Settings.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Poco/DirectoryIterator.h>

#include <filesystem>
#include <string_view>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int NAMED_SCALAR_ALREADY_EXISTS;
    extern const int NAMED_SCALAR_NOT_FOUND;
}

namespace
{

constexpr std::string_view file_prefix = "named_scalar_";
constexpr std::string_view definition_suffix = ".sql";

String makeDirectoryPathCanonical(const String & directory_path)
{
    auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
    if (canonical_directory_path.has_filename())
        canonical_directory_path += std::filesystem::path::preferred_separator;
    return canonical_directory_path;
}

void createDirectory(const String & dir_path)
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(
            ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "Couldn't create directory {} reason: '{}'",
            dir_path,
            create_dir_error_code.message());
}

String getDefinitionFilePath(const String & dir_path, const String & name)
{
    return dir_path + String(file_prefix) + escapeForFileName(name) + String(definition_suffix);
}

std::optional<String> parseDefinitionFileName(const String & file_name)
{
    if (!file_name.starts_with(file_prefix) || !file_name.ends_with(definition_suffix))
        return std::nullopt;

    size_t prefix_length = file_prefix.size();
    size_t suffix_length = definition_suffix.size();
    String escaped_name = file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length);

    String name = unescapeForFileName(escaped_name);
    if (name.empty())
        return std::nullopt;

    return name;
}

}

NamedScalarDefinitionStoreLocal::NamedScalarDefinitionStoreLocal(String dir_path_, LoggerPtr log_)
    : dir_path(makeDirectoryPathCanonical(dir_path_))
    , log(std::move(log_))
{
}

void NamedScalarDefinitionStoreLocal::initialize()
{
    createDirectory(dir_path);
}

std::vector<LoadedNamedScalarDefinition> NamedScalarDefinitionStoreLocal::loadAll()
{
    LOG_INFO(log, "Loading named scalar definitions from {}", dir_path);

    if (!std::filesystem::exists(dir_path))
    {
        LOG_DEBUG(log, "The directory for named scalars ({}) does not exist: nothing to load", dir_path);
        return {};
    }

    {
        Poco::DirectoryIterator sweep_end;
        for (Poco::DirectoryIterator it(dir_path); it != sweep_end; ++it)
        {
            const String & file_name = it.name();
            if (file_name.find(".tmp.") != String::npos)
            {
                try { std::filesystem::remove(dir_path + file_name); }
                catch (...) { tryLogCurrentException(log, fmt::format("while sweeping orphan temp {}", file_name)); }
            }
        }
    }

    std::vector<LoadedNamedScalarDefinition> objects;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();
        auto object_name = parseDefinitionFileName(file_name);
        if (!object_name)
            continue;

        try { NamedScalarsManager::checkName(*object_name); }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("rejecting on-disk scalar '{}': name violates cap", *object_name));
            continue;
        }

        const String path = dir_path + file_name;

        try
        {
            ReadBufferFromFile in(path);
            String contents;
            readStringUntilEOF(contents, in);

            objects.push_back({
                *object_name,
                std::move(contents),
            });
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("while loading named scalar definition from {}", path));
        }
    }

    return objects;
}

bool NamedScalarDefinitionStoreLocal::definitionExists(const String & name)
{
    return fs::exists(getDefinitionFilePath(dir_path, name));
}

bool NamedScalarDefinitionStoreLocal::publishDefinition(
    const String & name,
    const String & definition_blob,
    bool if_not_exists,
    bool or_replace,
    const Settings & settings)
{
    const String file_path = getDefinitionFilePath(dir_path, name);
    LOG_DEBUG(log, "Storing named scalar definition {} to file {}", name, file_path);

    const bool exists = fs::exists(file_path);
    if (exists && !or_replace)
    {
        if (if_not_exists)
            return false;
        throw Exception(ErrorCodes::NAMED_SCALAR_ALREADY_EXISTS, "Named scalar '{}' already exists", name);
    }

    /// Random-suffix temp file: two concurrent writers for the same
    /// name would otherwise corrupt each other's temp / catch path.
    const String temp_file_path = file_path + ".tmp." + getRandomASCIIString(8);

    try
    {
        WriteBufferFromFile out(temp_file_path, definition_blob.size());
        writeString(definition_blob, out);
        out.next();
        if (settings[Setting::fsync_metadata])
            out.sync();
        out.close();

        if (or_replace)
            fs::rename(temp_file_path, file_path);
        else
            renameNoReplace(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }

    LOG_TRACE(log, "Named scalar definition {} stored", name);
    return true;
}

bool NamedScalarDefinitionStoreLocal::removeDefinition(const String & name, bool throw_if_not_exists)
{
    const String file_path = getDefinitionFilePath(dir_path, name);
    LOG_DEBUG(log, "Removing named scalar definition {} from file {}", name, file_path);

    const bool existed = fs::remove(file_path);
    if (!existed)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::NAMED_SCALAR_NOT_FOUND, "Named scalar '{}' doesn't exist", name);
        return false;
    }

    LOG_TRACE(log, "Named scalar definition {} removed", name);
    return true;
}

bool NamedScalarDefinitionStoreLocal::readDefinition(const String & name, String & out)
{
    const String path = getDefinitionFilePath(dir_path, name);
    if (!fs::exists(path))
        return false;

    ReadBufferFromFile in(path);
    readStringUntilEOF(out, in);
    return true;
}

}
