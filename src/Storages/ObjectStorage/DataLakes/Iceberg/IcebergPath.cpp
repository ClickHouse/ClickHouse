#include <filesystem>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Common/Exception.h>
#include <Common/FullyQualifiedObjectPath.h>
#include <Common/StringUtils.h>
#include <base/EnumReflection.h>

#include <Poco/String.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

namespace
{
std::string_view trimTrailingSlashes(std::string_view str)
{
    while (!str.empty() && str.back() == '/')
        str.remove_suffix(1);
    return str;
}

/// Length of `<scheme>` in a `<scheme>:/` prefix of `path`, 0 if there is none.
size_t uriSchemeLength(std::string_view path)
{
    if (path.empty() || !isAlphaASCII(path[0]))
        return 0;
    size_t i = 1;
    while (i < path.size() && (isAlphaNumericASCII(path[i]) || path[i] == '+' || path[i] == '.' || path[i] == '-'))
        ++i;
    return i + 1 < path.size() && path[i] == ':' && path[i + 1] == '/' ? i : 0;
}

using Components = std::vector<std::pair<size_t, size_t>>;

/// Splits `path` from `begin` on `/`. A filesystem drops empty and `.` components.
Components splitComponents(std::string_view path, size_t begin, bool is_filesystem)
{
    Components components;
    if (begin >= path.size())
        return components;
    while (true)
    {
        size_t end = std::min(path.find('/', begin), path.size());
        auto component = path.substr(begin, end - begin);
        if (!is_filesystem || (!component.empty() && component != "."))
            components.emplace_back(begin, end);
        if (end == path.size())
            break;
        begin = end + 1;
    }
    return components;
}

std::string_view componentOf(std::string_view path, std::pair<size_t, size_t> component)
{
    return path.substr(component.first, component.second - component.first);
}

IcebergPathResolver::ParsedPath parsePath(std::string_view path, bool is_filesystem)
{
    IcebergPathResolver::ParsedPath parsed;
    size_t pos = 0;
    if (size_t scheme_length = uriSchemeLength(path))
    {
        parsed.has_scheme = true;
        auto scheme = Poco::toLower(String(path.substr(0, scheme_length)));
        pos = scheme_length + 1;
        if (scheme == "file")
        {
            /// `file:/x`, `file://x` and `file:///x` all name `/x`.
            while (pos < path.size() && path[pos] == '/')
                ++pos;
        }
        else
        {
            parsed.is_local = false;
            auto storage_type = DataLake::tryParseStorageTypeFromString(scheme);
            parsed.family = storage_type ? String(magic_enum::enum_name(*storage_type)) : scheme;
            if (path.substr(pos).starts_with("//"))
            {
                size_t authority_end = std::min(path.find('/', pos + 2), path.size());
                parsed.authority = path.substr(pos + 2, authority_end - pos - 2);
                pos = authority_end;
            }
            if (pos < path.size() && path[pos] == '/')
                ++pos;
        }
    }
    else if (path.starts_with('/'))
    {
        pos = 1;
    }
    parsed.path_begin = pos;
    parsed.components = splitComponents(path, pos, is_filesystem);
    return parsed;
}

bool isSameSpace(const IcebergPathResolver::ParsedPath & lhs, const IcebergPathResolver::ParsedPath & rhs)
{
    if (lhs.is_local || rhs.is_local)
        return lhs.is_local && rhs.is_local;
    return lhs.family == rhs.family && lhs.authority == rhs.authority;
}

/// The raw path after its first `count` components, without one separator.
std::string_view remainderAfter(std::string_view path, const IcebergPathResolver::ParsedPath & parsed, size_t count)
{
    if (count == 0)
        return path.substr(std::min(parsed.path_begin, path.size()));
    auto rest = path.substr(parsed.components[count - 1].second);
    if (rest.starts_with('/'))
        rest.remove_prefix(1);
    return rest;
}

/// Appends `rest` to `base` with a `/` in between; a `rest` starting with `/` does not replace `base`.
String joinPath(std::string_view base, std::string_view rest)
{
    if (base.empty())
        return String(rest);
    if (base.ends_with('/'))
        return fmt::format("{}{}", base, rest);
    return fmt::format("{}/{}", base, rest);
}

}

bool hasIcebergURIScheme(std::string_view path)
{
    return uriSchemeLength(path) != 0;
}

String toIcebergURIScheme(const String & storage_type_name)
{
    if (storage_type_name == "local")
        return "file";
    return storage_type_name;
}

String makeIcebergLocationURI(const String & storage_type_name, const String & authority, const String & path)
{
    String uri = toIcebergURIScheme(storage_type_name) + "://" + authority;
    if (!authority.empty() || !path.starts_with('/'))
        uri += '/';
    uri += path;
    return uri;
}

BlobStorageDescription BlobStorageDescription::fromConfiguration(const DB::StorageObjectStorageConfiguration & configuration)
{
    return {
        .type_name = configuration.getTypeName(),
        .namespace_name = configuration.getNamespace(),
        .allow_foreign_namespaces = configuration.supportsFullyQualifiedPaths(),
        .is_filesystem = configuration.getType() == ObjectStorageType::Local || configuration.getType() == ObjectStorageType::HDFS,
    };
}

IcebergPathResolver::TableRootDerivation IcebergPathResolver::deriveTableRoot(
    const String & table_location, const String & queried_path, const String & metadata_file_key)
{
    static constexpr std::string_view metadata_dir = "/metadata/";

    auto queried = trimTrailingSlashes(queried_path);
    auto key = trimTrailingSlashes(metadata_file_key);

    /// Only a document directly inside the table's own `metadata` directory names the table root.
    auto metadata_dir_pos = key.rfind(metadata_dir);
    if (metadata_dir_pos == std::string_view::npos
        || key.find('/', metadata_dir_pos + metadata_dir.size()) != std::string_view::npos)
        return {queried_path, RootRelation::Unknown};
    auto candidate = key.substr(0, metadata_dir_pos);

    if (candidate == queried)
        return {queried_path, RootRelation::Same};

    /// A component-aligned proper descendant; an empty queried path is the storage root.
    const bool is_descendant = queried.empty()
        ? !candidate.empty()
        : candidate.size() > queried.size() && candidate.starts_with(queried) && candidate[queried.size()] == '/';
    if (!is_descendant)
        return {queried_path, RootRelation::Unknown};

    auto strip_leading_slash = [](std::string_view str) { return str.starts_with('/') ? str.substr(1) : str; };

    /// Adopt only when the declared location denotes that same directory. It may differ from the
    /// storage path by a leading slash and by a `<scheme>://<authority>/` (or `file:/`) prefix, both
    /// of which still name that directory; a prefix carrying a path component names a different one.
    auto location = strip_leading_slash(trimTrailingSlashes(table_location));
    auto tail = strip_leading_slash(candidate);
    if (tail.empty())
        return {queried_path, RootRelation::Unknown};
    bool location_agrees = location == tail;
    if (!location_agrees && location.size() > tail.size() && location.ends_with(tail)
        && location[location.size() - tail.size() - 1] == '/')
    {
        auto parsed = parsePath(location, /* is_filesystem */ false);
        location_agrees = parsed.has_scheme && parsed.path_begin == location.size() - tail.size();
    }
    if (!location_agrees)
        return {queried_path, RootRelation::Unknown};

    return {String(candidate), RootRelation::AdoptedDescendant};
}

String IcebergPathResolver::parseNamespace(std::string_view path)
{
    if (auto qualified = trySplitFullyQualifiedObjectPath(path))
        return String(qualified->object_namespace);
    return {};
}

bool IcebergPathResolver::isInForeignNamespace(const String & raw_path) const
{
    if (!blob_storage.allow_foreign_namespaces || blob_storage.namespace_name.empty())
        return false;

    auto qualified = trySplitFullyQualifiedObjectPath(raw_path);
    if (!qualified)
        return false;

    auto path_storage_type = DataLake::tryParseStorageTypeFromString(String(qualified->scheme));
    if (!path_storage_type || path_storage_type != DataLake::tryParseStorageTypeFromString(blob_storage.type_name))
        return false;

    const String path_namespace(qualified->object_namespace);
    return path_namespace != blob_storage.namespace_name && path_namespace != table_location_namespace;
}

IcebergPathResolver::IcebergPathResolver(String table_location_, String table_root_, BlobStorageDescription blob_storage_)
    : table_location(trimTrailingSlashes(table_location_))
    , table_root(trimTrailingSlashes(table_root_))
    , blob_storage(std::move(blob_storage_))
{
    /// Normalize: non-URI table_location should start with '/'
    const bool location_is_relative = !table_location.empty() && !hasIcebergURIScheme(table_location) && table_location[0] != '/';
    if (location_is_relative)
        table_location = "/" + table_location;

    table_location_namespace = parseNamespace(table_location);
    parsed_location = parse(table_location);
    table_root_is_absolute = table_root.starts_with('/');
    table_root_components = splitComponents(table_root, table_root_is_absolute ? 1 : 0, blob_storage.is_filesystem);

    const auto & location_components = parsed_location.components;
    relative_location_not_under_root = location_is_relative
        && (location_components.size() > table_root_components.size()
            || !std::equal(
                location_components.begin(),
                location_components.end(),
                table_root_components.end() - location_components.size(),
                [&](auto lhs, auto rhs) { return componentOf(table_location, lhs) == componentOf(table_root, rhs); }));
}

IcebergPathResolver::ParsedPath IcebergPathResolver::parse(std::string_view path) const
{
    return parsePath(path, blob_storage.is_filesystem);
}

std::optional<String> IcebergPathResolver::resolveImpl(const String & raw_path) const
{
    if (relative_location_not_under_root && !raw_path.starts_with('/') && !hasIcebergURIScheme(raw_path))
        return raw_path;

    const auto path = parse(raw_path);
    const auto & location_components = parsed_location.components;
    size_t common = 0;
    while (common < location_components.size() && common < path.components.size()
           && componentOf(table_location, location_components[common]) == componentOf(raw_path, path.components[common]))
        ++common;
    const bool in_location = common == location_components.size();
    const bool same_space = isSameSpace(parsed_location, path);

    if (in_location && same_space)
        return joinPath(table_root, remainderAfter(raw_path, path, common));

    if (isInForeignNamespace(raw_path))
        return raw_path;

    /// A path or `location` with no scheme is relative to the writer's default file system, which may be this storage.
    const bool compatible_space = same_space || !parsed_location.has_scheme || !path.has_scheme;
    if (in_location && compatible_space)
        return joinPath(table_root, remainderAfter(raw_path, path, common));

    if (!table_root.empty())
    {
        auto pos = raw_path.find(table_root);
        if (pos != std::string::npos)
        {
            size_t good_pos = std::string::npos;
            while (pos != std::string::npos)
            {
                auto potential_position = pos + table_root.size();
                if (std::string_view(raw_path).substr(potential_position).starts_with("/data/")
                    || std::string_view(raw_path).substr(potential_position).starts_with("/metadata/"))
                {
                    good_pos = pos;
                    break;
                }
                size_t new_pos = raw_path.find(table_root, pos + 1);
                if (new_pos == std::string::npos)
                    break;
                pos = new_pos;
            }
            return raw_path.substr(good_pos != std::string::npos ? good_pos : pos);
        }
    }

    /// Outside `location`: relative to the common parent directory of the table and the file.
    const size_t outside_components = location_components.size() - common;
    if (compatible_space && outside_components <= table_root_components.size())
    {
        const size_t parent_components = table_root_components.size() - outside_components;
        std::string_view parent = table_root_is_absolute ? "/" : "";
        if (parent_components)
            parent = std::string_view(table_root).substr(0, table_root_components[parent_components - 1].second);
        return joinPath(parent, remainderAfter(raw_path, path, common));
    }

    return std::nullopt;
}

void IcebergPathResolver::throwUnresolvable(const IcebergPathFromMetadata & metadata_path) const
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Cannot resolve the path '{}' of an Iceberg table file: it is outside the table location '{}' and is not under the table "
        "path '{}'",
        metadata_path.serialize(),
        table_location,
        table_root);
}

String IcebergPathResolver::resolve(const IcebergPathFromMetadata & metadata_path) const
{
    if (auto resolved = resolveImpl(metadata_path.serialize()))
        return std::move(*resolved);
    throwUnresolvable(metadata_path);
}

std::optional<String> IcebergPathResolver::storageIdentity(const IcebergPathFromMetadata & metadata_path) const
{
    const auto & raw_path = metadata_path.serialize();
    if (isInForeignNamespace(raw_path))
    {
        /// The storage ignores the scheme spelling of its family (`s3a://b/k` is `s3://b/k`).
        auto qualified = trySplitFullyQualifiedObjectPath(raw_path);
        auto storage_type = DataLake::tryParseStorageTypeFromString(String(qualified->scheme));
        return fmt::format("{}://{}/{}", magic_enum::enum_name(*storage_type), qualified->object_namespace, qualified->key);
    }

    auto key = resolveImpl(raw_path);
    if (key && blob_storage.is_filesystem)
        return std::filesystem::path(*key).lexically_normal().string();
    return key;
}

String IcebergPathResolver::storageIdentityOrThrow(const IcebergPathFromMetadata & metadata_path) const
{
    if (auto identity = storageIdentity(metadata_path))
        return std::move(*identity);
    throwUnresolvable(metadata_path);
}

std::optional<std::string_view> IcebergPathResolver::canonicalPrefix(std::string_view path)
{
    auto parsed = parsePath(path, /* is_filesystem */ false);
    if (parsed.path_begin == 0 || parsed.components.empty())
        return std::nullopt;
    for (const auto & component : parsed.components)
        if (auto name = componentOf(path, component); name.empty() || name == "." || name == "..")
            return std::nullopt;
    chassert(path[parsed.path_begin - 1] == '/');
    return path.substr(0, parsed.path_begin);
}

PositionDeleteMatchKey IcebergPathResolver::positionDeleteMatchKey(const IcebergPathFromMetadata & path) const
{
    PositionDeleteMatchKey key{.identity = storageIdentity(path), .canonical_prefix = std::nullopt};
    if (auto prefix = canonicalPrefix(path.serialize()))
        key.canonical_prefix = String(*prefix);
    return key;
}

PositionDeleteScope IcebergPathResolver::matchPositionDeleteBounds(
    const std::optional<IcebergPathFromMetadata> & lower,
    const PositionDeleteMatchKey & lower_key,
    const std::optional<IcebergPathFromMetadata> & upper,
    const PositionDeleteMatchKey & upper_key,
    const IcebergPathFromMetadata & data_file_path,
    const PositionDeleteMatchKey & data_file_key) const
{
    /// Always so for a deletion vector, whose bounds are its `referenced_data_file`.
    if (lower && upper && *lower == *upper)
        return *lower == data_file_path || (lower_key.identity && lower_key.identity == data_file_key.identity)
            ? PositionDeleteScope::ThisFile
            : PositionDeleteScope::None;

    /// A bound spelled otherwise than the data file path means rows in that spelling: read them all.
    auto spelled_like_data_file = [&](const PositionDeleteMatchKey & key) { return key.canonical_prefix == data_file_key.canonical_prefix; };
    if (!data_file_key.canonical_prefix || (lower && !spelled_like_data_file(lower_key)) || (upper && !spelled_like_data_file(upper_key)))
        return PositionDeleteScope::ThisFile;

    /// Two bounds prove a range only if the lower one continues the prefix with a byte above `/` (see `canonicalPrefix`).
    if (lower && upper && lower->serialize() >= *data_file_key.canonical_prefix + '0'
        && !(*lower <= data_file_path && data_file_path <= *upper))
        return PositionDeleteScope::None;
    return PositionDeleteScope::Range;
}

}
