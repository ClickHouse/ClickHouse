#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/IDisk.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>

#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The base URL of a web disk is a URL, not a filesystem path: joining it with `std::filesystem`
/// gives it path semantics (on Windows it is not even parsed the same way), so the whole class
/// keeps it in plain URL string space instead.
String joinUrl(const String & base, const String & suffix)
{
    if (base.empty())
        return suffix;
    if (suffix.empty())
        return base;
    if (base.ends_with('/'))
        return suffix.starts_with('/') ? base + suffix.substr(1) : base + suffix;
    return suffix.starts_with('/') ? base + suffix : base + "/" + suffix;
}

/// The remote path of an object, relative to the base URL, with the file name escaped
/// the same way `escapeForFileName` escapes it for the uploaded static files.
String makeRemotePath(const String & path)
{
    std::string_view rest = path;
    while (rest.starts_with('/'))
        rest.remove_prefix(1);

    const size_t slash_pos = rest.find_last_of('/');
    const std::string_view directory = slash_pos == std::string_view::npos ? std::string_view{} : rest.substr(0, slash_pos + 1);
    const std::string_view file_name = slash_pos == std::string_view::npos ? rest : rest.substr(slash_pos + 1);

    /// The extension starts at the last dot, unless the name is "." or ".." or the dot is leading.
    std::string_view stem = file_name;
    std::string_view extension;
    if (file_name != "." && file_name != "..")
    {
        const size_t dot_pos = file_name.find_last_of('.');
        if (dot_pos != std::string_view::npos && dot_pos != 0)
        {
            stem = file_name.substr(0, dot_pos);
            extension = file_name.substr(dot_pos);
        }
    }

    return fmt::format("/{}{}{}", directory, escapeForFileName(String(stem)), extension);
}

/// The metadata of a web disk is a namespace of `/`-separated logical paths, not of local filesystem paths,
/// so it is taken apart with string operations: a `std::filesystem::path` would split a name at a backslash on
/// Windows and decode it through the active code page.
String trimTrailingSlashes(const String & path)
{
    size_t size = path.size();
    while (size > 0 && path[size - 1] == '/')
        --size;
    return path.substr(0, size);
}

/// The parent of a path without a trailing slash, also without one; empty for a top-level name.
String logicalParentPath(const String & path)
{
    const size_t slash_pos = path.find_last_of('/');
    if (slash_pos == String::npos)
        return {};
    return trimTrailingSlashes(path.substr(0, slash_pos));
}

}

MetadataStorageFromStaticFilesWebServer::MetadataStorageFromStaticFilesWebServer(
    const WebObjectStorage & object_storage_)
    : object_storage(object_storage_)
    , log(getLogger("MetadataStorageFromStaticFilesWebServer"))
{
}

MetadataTransactionPtr MetadataStorageFromStaticFilesWebServer::createTransaction()
{
    throwNotImplemented();
}

const std::string & MetadataStorageFromStaticFilesWebServer::getPath() const
{
    static const String no_root;
    return no_root;
}

bool MetadataStorageFromStaticFilesWebServer::existsFileOrDirectory(const std::string & path) const
{
    return tryGetFileInfo(path) != nullptr;
}

void MetadataStorageFromStaticFilesWebServer::assertExists(const std::string & path) const
{
    if (!existsFileOrDirectory(path))
#ifdef NDEBUG
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
#else
    {
        std::string all_files;
        std::shared_lock shared_lock(metadata_mutex);
        for (const auto & [file, _] : files)
        {
            if (!all_files.empty())
                all_files += ", ";
            all_files += file;
        }
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {} (available files: {})", path, all_files);
    }
#endif
}

bool MetadataStorageFromStaticFilesWebServer::existsFile(const std::string & path) const
{
    auto file_info = tryGetFileInfo(path);
    return file_info && file_info->type == FileType::File;
}

bool MetadataStorageFromStaticFilesWebServer::existsDirectory(const std::string & path) const
{
    auto file_info = tryGetFileInfo(path);
    return file_info && file_info->type == FileType::Directory;
}

uint64_t MetadataStorageFromStaticFilesWebServer::getFileSize(const String & path) const
{
    assertExists(path);
    auto file_info = getFileInfo(path);
    return file_info->size;
}

std::optional<uint64_t> MetadataStorageFromStaticFilesWebServer::getFileSizeIfExists(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (file_info)
        return file_info->size;
    else
        return std::nullopt;
}

StoredObjects MetadataStorageFromStaticFilesWebServer::getStorageObjects(const std::string & path) const
{
    assertExists(path);

    const std::string remote_path = makeRemotePath(path);

    auto file_info = getFileInfo(path);
    return {StoredObject(remote_path, path, file_info->size)};
}

std::optional<StoredObjects> MetadataStorageFromStaticFilesWebServer::getStorageObjectsIfExist(const std::string & path) const
{
    const std::string remote_path = makeRemotePath(path);

    if (auto file_info = tryGetFileInfo(path))
        return StoredObjects{StoredObject(remote_path, path, file_info->size)};
    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromStaticFilesWebServer::listDirectory(const std::string & path) const
{
    std::vector<std::string> result;
    std::shared_lock shared_lock(metadata_mutex);
    for (const auto & [file_path, _] : files)
    {
        if (file_path.starts_with(path))
            result.push_back(file_path); /// It looks more like recursive listing, not sure it is right
    }
    return result;
}

DirectoryIteratorPtr MetadataStorageFromStaticFilesWebServer::iterateDirectory(const std::string & path) const
{
    std::vector<String> dir_file_paths;

    if (!existsDirectory(path))
        return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));

    dir_file_paths = listDirectoryInternal(path);
    LOG_TRACE(log, "Iterate directory {} with {} files", path, dir_file_paths.size());
    return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));
}

std::pair<MetadataStorageFromStaticFilesWebServer::FileDataPtr, std::vector<String>>
MetadataStorageFromStaticFilesWebServer::loadFiles(const String & path, const std::unique_lock<SharedMutex> &) const
{
    std::vector<String> loaded_files;
    const String full_url = joinUrl(object_storage.getBaseURL(), path);

    LOG_TRACE(log, "Adding directory: {} ({})", path, full_url);

    FileDataPtr result;
    try
    {
        Poco::Net::HTTPBasicCredentials credentials{};

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
            object_storage.getContext()->getSettingsRef(),
            object_storage.getContext()->getServerSettings());

        auto metadata_buf = BuilderRWBufferFromHTTP(Poco::URI(joinUrl(full_url, ".index")))
                                .withConnectionGroup(HTTPConnectionGroupType::DISK)
                                .withSettings(object_storage.getContext()->getReadSettings())
                                .withTimeouts(timeouts)
                                .withHostFilter(&object_storage.getContext()->getRemoteHostFilter())
                                .withSkipNotFound(true)
                                .withHeaders(object_storage.getHeaders())
                                .create(credentials);

        String file_name;

        while (!metadata_buf->eof())
        {
            readText(file_name, *metadata_buf);
            assertChar('\t', *metadata_buf);

            bool is_directory = false;
            readBoolText(is_directory, *metadata_buf);
            size_t size = 0;
            if (!is_directory)
            {
                assertChar('\t', *metadata_buf);
                readIntText(size, *metadata_buf);
            }
            assertChar('\n', *metadata_buf);

            FileDataPtr file_data = is_directory
                ? FileData::createDirectoryInfo(false)
                : FileData::createFileInfo(size);

            /// A logical path on the web server, `/`-separated by definition.
            const String file_path = path.empty() || path.ends_with('/') ? path + file_name : path + "/" + file_name;
            const bool inserted = files.add(file_path, file_data).second;
            if (!inserted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Loading data for {} more than once", file_path);

            LOG_TRACE(log, "Adding file: {}, size: {}", file_path, size);
            loaded_files.emplace_back(file_path);
        }

        /// Check for not found url after read attempt, because of delayed initialization.
        if (metadata_buf->hasNotFoundURL())
            return {};

        auto [it, inserted] = files.add(path, FileData::createDirectoryInfo(true));
        if (!inserted)
        {
             if (it->second->loaded_children)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Loading data for {} more than once", path);

             it->second->loaded_children = true;
        }

        return std::pair(it->second, loaded_files);
    }
    catch (HTTPException & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }
    catch (Exception & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }
}

MetadataStorageFromStaticFilesWebServer::FileDataPtr MetadataStorageFromStaticFilesWebServer::tryGetFileInfo(const String & path) const
{
    /// `files` keys a directory as `name/` and a file as `name`, so both forms are probed rather than guessing
    /// the kind from the name: an extension says nothing here, a projection directory is `p.proj`.
    const String trimmed = trimTrailingSlashes(path);
    const String directory_key = trimmed + '/';

    {
        std::shared_lock shared_lock(metadata_mutex);
        if (auto it = files.find(trimmed); it != files.end())
            return it->second;
        if (auto it = files.find(directory_key); it != files.end())
            return it->second;

        /// A directory that has not been listed yet, but whose descendants already have.
        if (!trimmed.empty())
        {
            auto it = files.lower_bound(directory_key);
            if (it != files.end() && it->first.starts_with(directory_key))
            {
                shared_lock.unlock();
                std::unique_lock unique_lock(metadata_mutex);
                return files.add(directory_key, FileData::createDirectoryInfo(false)).first->second;
            }
        }
    }

    /// Every directory `.index` lists both the files and the subdirectories in it, so once the parent is listed
    /// it is authoritative. Only the directories above the table have no `.index`: for them the lookup below
    /// asks for the path's own `.index`.
    if (!trimmed.empty())
    {
        const String parent_path = logicalParentPath(trimmed);
        if (auto parent_info = tryGetFileInfo(parent_path))
        {
            if (!parent_info->loaded_children)
            {
                std::unique_lock unique_lock(metadata_mutex);
                if (!parent_info->loaded_children)
                    loadFiles(parent_path, unique_lock);
            }

            if (parent_info->loaded_children)
            {
                std::shared_lock shared_lock(metadata_mutex);
                if (auto it = files.find(trimmed); it != files.end())
                    return it->second;
                if (auto it = files.find(directory_key); it != files.end())
                    return it->second;
                return nullptr;
            }
        }
    }

    std::unique_lock unique_lock(metadata_mutex);
    if (auto it = files.find(directory_key); it != files.end())
        return it->second;
    return loadFiles(trimmed, unique_lock).first;
}

std::vector<String> MetadataStorageFromStaticFilesWebServer::listDirectoryInternal(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (!file_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path);

    if (file_info->type != FileType::Directory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} is not a directory", path);

    std::vector<String> result;
    if (!file_info->loaded_children)
    {
        std::unique_lock unique_lock(metadata_mutex);
        if (!file_info->loaded_children)
            return loadFiles(path, unique_lock).second;
    }
    const String directory = trimTrailingSlashes(path);
    std::shared_lock shared_lock(metadata_mutex);
    for (const auto & [file_path, _] : files)
    {
        const String entry = trimTrailingSlashes(file_path);
        /// The root directory is keyed as `/`, which is not its own child.
        if (!entry.empty() && logicalParentPath(entry) == directory)
            result.emplace_back(file_path);
    }
    return result;
}

MetadataStorageFromStaticFilesWebServer::FileDataPtr MetadataStorageFromStaticFilesWebServer::getFileInfo(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (!file_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path);
    return file_info;
}

}
