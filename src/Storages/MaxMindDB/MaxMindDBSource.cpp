#include <Storages/MaxMindDB/MaxMindDBSource.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/IDisk.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MaxMindDB/MaxMindDBSettings.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Web/Configuration.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/ErrnoException.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <Poco/URI.h>

#include "config.h"
#if USE_AWS_S3
#include <IO/S3Common.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_STAT;
extern const int DATABASE_ACCESS_DENIED;
extern const int LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
}

namespace MaxMindDBSetting
{
extern const MaxMindDBSettingsString disk;
extern const MaxMindDBSettingsUInt64 max_download_size;
}

namespace
{
template <typename Operation>
decltype(auto) remoteOperation(Operation && operation, std::string_view description)
{
    try
    {
        return operation();
    }
    catch (const HTTPException & exception)
    {
        throw Exception(exception.code(), "MaxMindDB {} failed with HTTP status {}", description, exception.getHTTPStatus());
    }
#if USE_AWS_S3
    catch (const S3Exception & exception)
    {
        throw Exception(
            static_cast<const Exception &>(exception).code(),
            "MaxMindDB {} failed with S3 error {}",
            description,
            static_cast<int>(exception.getS3ErrorCode()));
    }
#endif
}

String localVersion(const String & path)
{
    struct stat info{};
    if (::stat(path.c_str(), &info) != 0)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, path, "Cannot stat MaxMindDB file");
    if (!S_ISREG(info.st_mode))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB source must be a regular file");
#if defined(__APPLE__)
    const auto & modified = info.st_mtimespec;
#else
    const auto & modified = info.st_mtim;
#endif
    return fmt::format("{}:{}:{}:{}:{}", info.st_dev, info.st_ino, info.st_size, modified.tv_sec, modified.tv_nsec);
}

String remoteVersion(const ObjectMetadata & metadata)
{
    if (!metadata.etag.empty())
        return metadata.etag;
    if (metadata.is_last_modified_known)
        return fmt::format(
            "{}:{}", metadata.last_modified.epochMicroseconds(), metadata.is_size_known ? std::to_string(metadata.size_bytes) : "unknown");
    return {};
}

DiskPtr getCacheDisk(ContextPtr context, const MaxMindDBSettings & settings)
{
    auto disk = context->getDisk(settings[MaxMindDBSetting::disk].value);
    const auto description = disk->getDataSourceDescription();
    if (description.type != DataSourceType::Local || description.is_encrypted || disk->isReadOnly())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB cache disk must provide writable, unencrypted local files for mmap");
    return disk;
}

void extractArchive(MaxMindDBFile & file, ContextPtr context, const MaxMindDBSettings & settings)
{
    ReadBufferFromFile probe(file.path);
    std::array<char, 3> signature{};
    if (probe.read(signature.data(), signature.size()) != signature.size() || static_cast<unsigned char>(signature[0]) != 0x1f
        || static_cast<unsigned char>(signature[1]) != 0x8b || static_cast<unsigned char>(signature[2]) != 0x08)
        return;

    /// Download endpoints need not have an archive suffix, so detect gzip from the content.
    auto archive = createArchiveReader(file.path + ".tar.gz", [&] { return std::make_unique<ReadBufferFromFile>(file.path); }, 0);
    auto enumerator = archive->firstFile();
    std::unique_ptr<TemporaryFileOnDisk> extracted;
    const auto max_size = settings[MaxMindDBSetting::max_download_size].value;
    UInt64 unpacked = 0;
    size_t entries = 0;
    while (enumerator)
    {
        if (++entries > 10000)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "MaxMindDB archive contains more than 10000 entries");
        const std::filesystem::path member(enumerator->getFileName());
        if (member.is_absolute() || std::ranges::any_of(member, [](const auto & component) { return component == ".."; }))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB archive contains an unsafe entry path");
        const auto expected_size = enumerator->getFileInfo().uncompressed_size;
        if (max_size && expected_size > max_size - unpacked)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "MaxMindDB archive exceeds max_download_size after decompression");

        std::unique_ptr<WriteBufferFromFile> output;
        if (member.extension() == ".mmdb")
        {
            if (extracted)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB archive must contain exactly one .mmdb file");
            extracted = std::make_unique<TemporaryFileOnDisk>(getCacheDisk(context, settings), "tmp/maxminddb/");
            /// Never use an archive member name as a filesystem destination.
            output = std::make_unique<WriteBufferFromFile>(
                extracted->getAbsolutePath(), DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_EXCL, nullptr, 0600);
        }
        auto input = archive->readFile(std::move(enumerator));
        UInt64 member_size = 0;
        while (!input->eof())
        {
            const auto size = input->available();
            if (max_size && size > max_size - unpacked)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "MaxMindDB archive exceeds max_download_size after decompression");
            if (output)
                output->write(input->position(), size);
            input->position() += size;
            member_size += size;
            unpacked += size;
        }
        if (member_size != expected_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB archive entry size does not match its metadata");
        if (output)
            output->finalize();
        enumerator = archive->nextFile(std::move(input));
    }
    if (!extracted)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB archive must contain exactly one .mmdb file");

    archive.reset();
    file.path = extracted->getAbsolutePath();
    file.cache_file = std::move(extracted);
}
}

MaxMindDBFile::MaxMindDBFile() = default;
MaxMindDBFile::~MaxMindDBFile() = default;
MaxMindDBSource::~MaxMindDBSource() = default;

MaxMindDBSource::MaxMindDBSource(ASTs & arguments, ContextPtr context_, const StorageID & table_id, bool check_access)
    : WithContext(context_->getGlobalContext())
    , read_settings(context_->getReadSettings())
{
    const auto & context = context_;
    if (arguments.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB requires a file path, URL, or named collection");

    auto collection = tryGetNamedCollectionWithOverrides(arguments, context, true, nullptr, &table_id);
    String url;
    size_t positional_count = 0;
    ASTs positional_arguments;
    bool s3_credentials = false;
    if (collection)
    {
        url = collection->get<String>("url");
        s3_credentials = collection->has("access_key_id") || collection->has("secret_access_key") || collection->has("no_sign_request")
            || collection->has("session_token") || collection->has("use_environment_credentials") || collection->has("role_arn");
    }
    else
    {
        for (auto & argument : arguments)
        {
            const auto * function = argument->as<ASTFunction>();
            if (function && function->name == "headers")
                continue;
            argument = evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
            positional_arguments.push_back(argument);
            ++positional_count;
        }
        url = checkAndGetLiteralArgument<String>(arguments.front(), "source");
        s3_credentials = positional_count > 1;
    }

    if (url.contains('\0'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB source contains an embedded NUL byte");

    const bool is_http = url.starts_with("http://") || url.starts_with("https://");
    if ((is_http || url.starts_with("s3://")) && url.contains('#'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB URLs must not contain fragments");
    const bool is_s3 = url.starts_with("s3://") || (is_http && s3_credentials)
        || (is_http && Poco::URI(url).getHost().ends_with(".amazonaws.com") && Poco::URI(url).getRawQuery().empty());

    auto source_access = AccessTypeObjects::Source::FILE;
    if (is_http || is_s3)
    {
        const Poco::URI uri(url);
        if (!uri.getUserInfo().empty() || !uri.getFragment().empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "MaxMindDB URLs must not contain userinfo or fragments; use headers or named collections for "
                "authentication");
        if (uri.getAuthority().find_first_of("*{},|") != String::npos || uri.getPath().find_first_of("*{},|") != String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB requires exactly one source file; URL globs or lists are not supported");
        source_access = is_s3 ? AccessTypeObjects::Source::S3 : AccessTypeObjects::Source::URL;
    }

    if (check_access)
        context->checkAccess(AccessType::READ, toStringSource(source_access));

    if (is_s3)
    {
#if USE_AWS_S3
        if (!collection
            && (positional_count > 4
                || (positional_count == 2 && checkAndGetLiteralArgument<String>(positional_arguments[1], "NOSIGN") != "NOSIGN")))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "MaxMindDB S3 arguments are source [, NOSIGN] or source, access_key_id, secret_access_key [, session_token]");
        configuration = std::make_shared<StorageS3Configuration>();
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaxMindDB S3 sources require a build with S3 support");
#endif
    }
    else if (is_http)
    {
        if (!collection && positional_count != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB HTTP sources accept a URL and optional headers");
        if (collection)
            validateNamedCollection(
                *collection,
                {"url"},
                {"format", "structure", "compression", "compression_method"},
                {std::make_shared<re2::RE2>("headers\\.[^.]+\\.(name|value)")});
        configuration = std::make_shared<StorageWebConfiguration>();
    }
    else
    {
        if (url.contains("://") || (!collection && arguments.size() != 1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB local sources accept a single file path");
        if (collection)
            validateNamedCollection(*collection, {"url"}, {});
        std::filesystem::path path(url);
        if (context->getApplicationType() == Context::ApplicationType::SERVER && path.is_relative())
            path = std::filesystem::path(context->getUserFilesPath()) / path;
        local_path = std::filesystem::absolute(path).string();
        return;
    }

    ASTs configuration_arguments;
    if (is_s3 && !collection && positional_count >= 3)
    {
        /// The short `S3` signatures can mistake a credential for a format name.
        /// Explicit format and compression arguments make the credential positions unambiguous.
        for (const auto & argument : positional_arguments)
            configuration_arguments.push_back(argument->clone());
        configuration_arguments.push_back(make_intrusive<ASTLiteral>("auto"));
        configuration_arguments.push_back(make_intrusive<ASTLiteral>("none"));
        for (const auto & argument : arguments)
            if (const auto * function = argument->as<ASTFunction>(); function && function->name == "headers")
                configuration_arguments.push_back(argument->clone());
    }
    else
    {
        configuration_arguments.reserve(arguments.size());
        for (const auto & argument : arguments)
            configuration_arguments.push_back(argument->clone());
    }
    StorageObjectStorageConfiguration::initialize(*configuration, configuration_arguments, context, false, &table_id);
    const Poco::URI configured_uri(configuration->getRawURI());
    if (!configured_uri.getUserInfo().empty() || !configured_uri.getFragment().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB source configuration must not add URL userinfo or fragments");
    configuration->check(context);
    if (configuration->getRawPath().hasGlobs() || configuration->isArchive())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB requires exactly one source object without archive member syntax");
    object_storage = configuration->createObjectStorage(context, true, std::nullopt);
    if (!is_s3)
        assert_cast<WebObjectStorage &>(*object_storage).setDefaultRequestSettings(context->getSettingsRef());
}

std::unique_ptr<MaxMindDBFile> MaxMindDBSource::load(const String & current_version, const MaxMindDBSettings & settings) const
{
    const auto context = getContext();
    auto file = std::make_unique<MaxMindDBFile>();
    if (!local_path.empty())
    {
        if (context->getApplicationType() == Context::ApplicationType::SERVER && !pathStartsWith(local_path, context->getUserFilesPath()))
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "MaxMindDB file must be inside user_files_path");
        file->path = local_path;
        file->version = localVersion(local_path);
        if (file->version == current_version)
            return {};
        extractArchive(*file, context, settings);
        return file;
    }

    const auto path = configuration->getPathForRead().path;
    const auto metadata = remoteOperation([&] { return object_storage->getObjectMetadata(path, false); }, "metadata request");
    file->version = remoteVersion(metadata);
    if (!file->version.empty() && file->version == current_version)
        return {};

    const auto max_size = settings[MaxMindDBSetting::max_download_size].value;
    if (max_size && metadata.is_size_known && metadata.size_bytes > max_size)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "MaxMindDB download exceeds max_download_size");

    file->cache_file = std::make_unique<TemporaryFileOnDisk>(getCacheDisk(context, settings), "tmp/maxminddb/");
    file->path = file->cache_file->getAbsolutePath();
    StoredObject object(path, "", metadata.is_size_known ? metadata.size_bytes : StoredObject::UnknownSize);
    object.etag = metadata.etag;
    auto input = remoteOperation([&] { return object_storage->readObject(object, read_settings, {}, false, true); }, "download");
    WriteBufferFromFile output(file->path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_EXCL, {}, 0600);
    UInt64 downloaded = 0;
    while (remoteOperation([&] { return !input->eof(); }, "download"))
    {
        const size_t size = input->available();
        if (max_size && size > max_size - downloaded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "MaxMindDB download exceeds max_download_size");
        output.write(input->position(), size);
        input->position() += size;
        downloaded += size;
    }
    output.finalize();
    if (metadata.is_size_known && downloaded != metadata.size_bytes)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB download size does not match source metadata");
    const auto after = remoteVersion(remoteOperation([&] { return object_storage->getObjectMetadata(path, false); }, "metadata request"));
    if (after != file->version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB remote object changed during download; retry the refresh");
    extractArchive(*file, context, settings);
    return file;
}

void MaxMindDBSource::checkVersion(const MaxMindDBFile & file) const
{
    const auto current = !local_path.empty()
        ? localVersion(local_path)
        : remoteVersion(remoteOperation(
              [&] { return object_storage->getObjectMetadata(configuration->getPathForRead().path, false); }, "metadata request"));
    if (current != file.version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB source changed while preparing a generation; retry the operation");
}
}
