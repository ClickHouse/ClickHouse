#pragma once

#include "config.h"

#include <Common/parseRemoteDescription.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

struct WebStorageParsedArguments : private StorageParsedArguments
{
    friend class StorageWebConfiguration;

    static constexpr auto max_number_of_arguments_with_structure = 4;
    static constexpr auto signatures_with_structure
        = " - url\n"
          " - url, format\n"
          " - url, format, structure\n"
          " - url, format, structure, compression_method\n"
          "All signatures support optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static constexpr auto max_number_of_arguments_without_structure = max_number_of_arguments_with_structure - 1;
    static constexpr auto signatures_without_structure
        = " - url\n"
          " - url, format\n"
          " - url, format, compression_method\n"
          "All signatures support optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }

    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    String url;
    HTTPHeaderEntries headers_from_ast;

public:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);
};

class StorageWebConfiguration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::Web;
    static constexpr auto type_name = "web";
    static constexpr auto engine_name = "URL";

    /// The defaults reproduce the `url` table function; the `URL` engine passes its own caller
    /// descriptor so an over-limit wildcard expansion names the surface the user actually invoked.
    StorageWebConfiguration();
    explicit StorageWebConfiguration(RemoteDescriptionCaller glob_caller_);

    [[nodiscard]] ObjectStorageType getType() const override { return type; }
    [[nodiscard]] std::string getTypeName() const override { return type_name; }
    [[nodiscard]] std::string getEngineName() const override { return engine_name; }
    [[nodiscard]] std::string getNamespaceType() const override { return "host"; }

    [[nodiscard]] Path getRawPath() const override { return path; }
    void setRawPath(const Path & path_) override
    {
        path = path_;
        setPaths({path});
        setPathForRead(path);
    }
    [[nodiscard]] std::string_view getRawURI() const override { return raw_url; }

    [[nodiscard]] bool supportsPartialPathPrefix() const override { return false; }

    /// Whether the source is split at `::` into an archive and a path inside it. The reader of a table
    /// definition is built under the global context, so the decision is taken once by the session that
    /// parsed the arguments and recorded here, instead of being re-read from the context that happens to
    /// build the storage.
    void setAllowArchivePathSyntax(bool allow) { allow_archive_path_syntax = allow; }

    /// A definition stored by an earlier version may combine archive syntax with a failover group, which this
    /// version refuses. Reject it only while the definition is being written, so that such a table keeps
    /// loading from the catalog instead of disappearing on startup.
    void setRejectArchiveFailover(bool reject) { reject_archive_failover = reject; }

    [[nodiscard]] String getNamespace() const override { return namespace_prefix; }
    [[nodiscard]] String getDataSourceDescription() const override { return raw_url; }

    /// A hash of the request headers, for cache identities that have to tell two header sets apart
    /// (the headers select what the server returns for the same URL). The values are secret, so only
    /// the hash is exposed, never the header contents themselves.
    [[nodiscard]] std::string getHeadersHash() const;

    [[nodiscard]] StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr & context) const override;

    [[nodiscard]] bool isArchive() const override { return archive_pattern.has_value(); }
    [[nodiscard]] std::string getPathInArchive() const override;

    void check(ContextPtr context) override;

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure_,
        const String & format_,
        ContextPtr context,
        bool with_structure) override;

protected:
    const Paths & getPathsImpl() const override TSA_REQUIRES_SHARED(paths_mutex) { return paths; }
    void setPathsImpl(const Paths & paths_) override TSA_REQUIRES(paths_mutex) { paths = paths_; }

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;

private:
    void initializeFromParsedArguments(WebStorageParsedArguments && parsed_arguments, ContextPtr context);
    void setNamespaceFromURL(ContextPtr context);

    RemoteDescriptionCaller glob_caller;
    String raw_url;
    WebObjectStorage::URLShards url_shards;
    String namespace_prefix;
    Path path;
    Paths paths TSA_GUARDED_BY(paths_mutex);
    std::optional<String> archive_pattern;
    std::optional<bool> allow_archive_path_syntax;
    bool reject_archive_failover = true;
    HTTPHeaderEntries headers_from_ast;
};

}
