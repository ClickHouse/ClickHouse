#include <Storages/TableZnodeInfo.h>

#include <Common/Macros.h>
#include <Common/quoteString.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Databases/DatabaseReplicatedHelpers.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <base/hex.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The path and the replica name are separate engine arguments, so a message names the one that failed.
enum class ZnodeString : UInt8
{
    Path,
    ReplicaName,
};

constexpr std::string_view toString(ZnodeString what)
{
    switch (what)
    {
        case ZnodeString::Path: return "ZooKeeper path";
        case ZnodeString::ReplicaName: return "replica name";
    }
}

constexpr std::string_view howToOverride(ZnodeString what)
{
    switch (what)
    {
        case ZnodeString::Path: return "specify the ZooKeeper path explicitly";
        case ZnodeString::ReplicaName: return "specify the replica name explicitly";
    }
}

/// A substituted {database}/{table} is spliced in verbatim, so the value itself must stay a single path
/// component: '/' adds components, and a brace is expanded again by the next macro pass (which can mint
/// components out of the value, or complete a brace opened by a configured macro).
void checkSubstitutedValues(ZnodeString what, const Macros::MacroExpansionInfo & info)
{
    auto check = [what](std::string_view macro_name, const String & value)
    {
        const size_t bad_pos = value.find_first_of("/{}");
        if (bad_pos == String::npos)
            return;
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Macro '{{{}}}' in the {} of a replicated table expands to {}, "
            "which contains '{}' and so is not a single ZooKeeper path component. "
            "Rename the {} or {}",
            macro_name, toString(what), quoteString(value), value[bad_pos], macro_name, howToOverride(what));
    };

    if (info.expanded_database)
        check("database", info.table_id.database_name);
    if (info.expanded_table)
        check("table", info.table_id.table_name);
}

/// '.', '..' and control characters are not valid znode names. Unlike the check above, this one runs on
/// the fully expanded string, because a legal component can be assembled from a substituted value plus
/// the surrounding template, possibly only in a later macro pass.
void checkPathComponents(ZnodeString what, const String & str)
{
    std::string_view remaining = str;
    while (true)
    {
        const size_t slash_pos = remaining.find('/');
        const std::string_view component = remaining.substr(0, slash_pos);

        if (component == "." || component == "..")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The {} of a replicated table expands to {}, which has '{}' as a ZooKeeper path component. "
                "Rename the table or the database, or {}",
                toString(what), quoteString(str), component, howToOverride(what));

        for (size_t i = 0; i < component.size(); ++i)
        {
            const auto byte = static_cast<unsigned char>(component[i]);
            const auto next_byte = i + 1 < component.size() ? static_cast<unsigned char>(component[i + 1]) : 0;
            std::optional<UInt16> code_point;
            if (byte < 0x20 || byte == 0x7F)
                code_point = byte;
            /// UTF-8 encodes U+0080-U+009F only as C2 80..C2 9F, so the pair test cannot match a
            /// continuation byte of some other character.
            else if (byte == 0xC2 && next_byte >= 0x80 && next_byte <= 0x9F)
                code_point = next_byte;
            if (!code_point)
                continue;

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The {} of a replicated table expands to a string containing the control character U+{}, "
                "which is not valid in a ZooKeeper path. Rename the table or the database, or {}",
                toString(what), getHexUIntUppercase(*code_point), howToOverride(what));
        }

        if (slash_pos == std::string_view::npos)
            break;

        remaining.remove_prefix(slash_pos + 1);
    }
}

/// Returns the position right after the path component containing the last canonical UUID (36 characters)
/// in the path, or npos if there is none. The UUID does not have to be a whole component: the `{uuid}`
/// macro may be surrounded by other text in the same component, as in "/foo/pika{uuid}chu/bar".
size_t findEndOfComponentWithLastUUID(const String & path)
{
    static constexpr size_t uuid_text_size = 36;
    if (path.size() < uuid_text_size)
        return String::npos;

    for (size_t begin = path.size() - uuid_text_size; ; --begin)
    {
        UUID uuid;
        if (tryParseUUID({reinterpret_cast<const UInt8 *>(path.data() + begin), uuid_text_size}, uuid))
        {
            size_t end = begin + uuid_text_size;
            while (end < path.size() && path[end] != '/')
                ++end;
            return end;
        }

        if (begin == 0)
            break;
    }
    return String::npos;
}

}

TableZnodeInfo TableZnodeInfo::resolve(
    const String & requested_path, const String & requested_replica_name,
    const StorageID & table_id, const ASTCreateQuery & query, LoadingStrictnessLevel mode,
    const ContextPtr & context, bool validate_substitutions)
{
    bool is_on_cluster = context->isDDLOrOnClusterInternal();
    bool is_replicated_database = context->isDDLOrOnClusterInternal() &&
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->getEngineName() == "Replicated";

    /// Allow implicit {uuid} macros only for zookeeper_path in ON CLUSTER queries
    /// and if UUID was explicitly passed in CREATE TABLE (like for ATTACH)
    bool allow_uuid_macro = is_on_cluster || is_replicated_database || query.attach || query.has_uuid;

    TableZnodeInfo res;
    res.full_path = requested_path;
    res.replica_name = requested_replica_name;

    /// Whether {database}/{table} reached this output, accumulated over both expansion passes below:
    /// the substitution and the resulting illegal component may appear in different passes.
    bool path_substituted = false;
    bool replica_substituted = false;
    auto note_substitution = [](const Macros::MacroExpansionInfo & info, bool & substituted)
    {
        substituted = substituted || info.expanded_database || info.expanded_table;
    };

    /// Unfold {database} and {table} macro on table creation, so table can be renamed.
    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        /// Each output gets its own info: the expansion flags are not reset for a string without macros,
        /// so a shared one would attribute the path's substitutions to the replica name too.
        Macros::MacroExpansionInfo path_info;
        /// NOTE: it's not recursive
        path_info.expand_special_macros_only = true;
        path_info.table_id = table_id;
        /// Avoid unfolding {uuid} macro on this step.
        /// We did unfold it in previous versions to make moving table from Atomic to Ordinary database work correctly,
        /// but now it's not allowed (and it was the only reason to unfold {uuid} macro).
        path_info.table_id.uuid = UUIDHelpers::Nil;
        res.full_path = context->getMacros()->expand(res.full_path, path_info);
        note_substitution(path_info, path_substituted);

        Macros::MacroExpansionInfo replica_info = path_info;
        replica_info.level = 0;
        replica_info.expanded_database = false;
        replica_info.expanded_table = false;
        replica_info.expanded_uuid = false;
        replica_info.expanded_other = false;
        replica_info.has_unknown = false;
        res.replica_name = context->getMacros()->expand(res.replica_name, replica_info);
        note_substitution(replica_info, replica_substituted);

        if (validate_substitutions)
        {
            checkSubstitutedValues(ZnodeString::Path, path_info);
            checkSubstitutedValues(ZnodeString::ReplicaName, replica_info);
        }
    }

    res.full_path_for_metadata = res.full_path;
    res.replica_name_for_metadata = res.replica_name;

    /// Expand other macros (such as {shard} and {replica}). We do not expand them on previous step
    /// to make possible copying metadata files between replicas.
    Macros::MacroExpansionInfo info;
    info.table_id = table_id;
    if (is_replicated_database)
    {
        auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
        info.shard = getReplicatedDatabaseShardName(database);
        info.replica = getReplicatedDatabaseReplicaName(database);
    }
    if (!allow_uuid_macro)
        info.table_id.uuid = UUIDHelpers::Nil;
    Macros::MacroExpansionInfo replica_info = info;
    res.full_path = context->getMacros()->expand(res.full_path, info);
    bool expanded_uuid_in_path = info.expanded_uuid;
    note_substitution(info, path_substituted);

    replica_info.table_id.uuid = UUIDHelpers::Nil;
    res.replica_name = context->getMacros()->expand(res.replica_name, replica_info);
    note_substitution(replica_info, replica_substituted);

    if (validate_substitutions)
    {
        checkSubstitutedValues(ZnodeString::Path, info);
        checkSubstitutedValues(ZnodeString::ReplicaName, replica_info);
        if (path_substituted)
            checkPathComponents(ZnodeString::Path, res.full_path);
        if (replica_substituted)
            checkPathComponents(ZnodeString::ReplicaName, res.replica_name);
    }

    /// We do not allow renaming table with these macros in metadata, because zookeeper_path will be broken after RENAME TABLE.
    /// NOTE: it may happen if table was created by older version of ClickHouse (< 20.10) and macros was not unfolded on table creation
    /// or if one of these macros is recursively expanded from some other macro.
    /// Also do not allow to move table from Atomic to Ordinary database if there's {uuid} macro
    if (info.expanded_database || info.expanded_table || replica_info.expanded_database || replica_info.expanded_table)
        res.renaming_restrictions = RenamingRestrictions::DO_NOT_ALLOW;
    else if (info.expanded_uuid || replica_info.expanded_uuid)
        res.renaming_restrictions = RenamingRestrictions::ALLOW_PRESERVING_UUID;

    res.zookeeper_name = zkutil::extractZooKeeperName(res.full_path);
    res.path = zkutil::extractZooKeeperPath(res.full_path, /* check_starts_with_slash */ mode <= LoadingStrictnessLevel::CREATE, getLogger(table_id.getNameForLogs()));
    res.path_prefix_for_drop = res.path;

    if (expanded_uuid_in_path)
    {
        /// When dropping table with znode path "/foo/{uuid}/bar/baz", delete not only
        /// "/foo/{uuid}/bar/baz" but also "/foo/{uuid}/bar" and "/foo/{uuid}" if they became empty.
        ///
        /// (We find the uuid substring by searching instead of keeping track of it when expanding
        ///  the macro. So in principle we may find a uuid substring that wasn't expanded from a
        ///  macro. This should be ok because we're searching for the *last* occurrence, so we'll get
        ///  a prefix at least as long as the correct one, so we won't delete znodes outside the
        ///  {uuid} path component. This sounds sketchy, but propagating string indices through macro
        ///  expansion passes is sketchy too (error-prone and more complex), and on balance this seems
        ///  better.)
        String uuid_str = toString(table_id.uuid);
        size_t i = res.path.rfind(uuid_str);
        if (i == String::npos)
            /// Possible if the macro is in the "<auxiliary_zookeeper_name>:/" prefix, but we probably
            /// don't want to allow that.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find uuid in zookeeper path after expanding {{uuid}} macro: {} (uuid {})", res.path, uuid_str);
        i += uuid_str.size();
        /// In case the path is "/foo/pika{uuid}chu/bar" (or "/foo/{uuid}{replica}/bar").
        while (i < res.path.size() && res.path[i] != '/')
            i += 1;
        res.path_prefix_for_drop = res.path.substr(0, i);
    }
    else
    {
        /// A table may live under a UUID-named znode without the {uuid} macro in its metadata: converting
        /// a table of an `Ordinary` database to a replicated engine mints a UUID for the {uuid} macro and
        /// stores the fully expanded path as a literal, because the metadata of such a table has no place
        /// for the UUID itself. That literal is the only record of the UUID, so the owned prefix is
        /// recovered from the path: it ends with the path component containing the last UUID, which may be
        /// surrounded by other text as in "/foo/pika{uuid}chu/bar" (the same rule as above). The recovery
        /// cannot be limited to tables with a Nil UUID: after `RENAME TABLE` from `Ordinary` into `Atomic`
        /// the table gets a fresh UUID of its own while the literal path keeps the minted one. (A UUID
        /// the user wrote by hand may be picked up as well; harmless, as only emptied znodes are removed.)
        if (const size_t i = findEndOfComponentWithLastUUID(res.path); i != String::npos)
            res.path_prefix_for_drop = res.path.substr(0, i);
    }

    return res;
}

void TableZnodeInfo::checkPrefixForDropRecoverableFromPath() const
{
    const size_t recovered_end = findEndOfComponentWithLastUUID(path);
    const std::string_view recovered_prefix = recovered_end == String::npos ? std::string_view(path) : std::string_view(path).substr(0, recovered_end);
    if (recovered_prefix == path_prefix_for_drop)
        return;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "The ZooKeeper path {} of the converted table has another UUID-shaped path component after the one expanded "
        "from the {{uuid}} macro. A table of an Ordinary database stores this path literally, without the macro, "
        "so on a later load it could not tell which znode it owns and would keep {} in ZooKeeper after DROP TABLE. "
        "Change the default_replica_path template or the macro that expands to a UUID (such as {{shard}}), "
        "or move the table to an Atomic database before converting it",
        quoteString(full_path), quoteString(path_prefix_for_drop));
}

void TableZnodeInfo::dropAncestorZnodesIfNeeded(const zkutil::ZooKeeperPtr & zookeeper) const
{
    chassert(path.starts_with(path_prefix_for_drop));
    if (path_prefix_for_drop.empty() || path_prefix_for_drop.size() == path.size())
        return;
    chassert(path[path_prefix_for_drop.size()] == '/');

    String path_to_remove = path;
    while (path_to_remove.size() > path_prefix_for_drop.size())
    {
        size_t i = path_to_remove.find_last_of('/');
        chassert(i != String::npos && i >= path_prefix_for_drop.size());
        path_to_remove = path_to_remove.substr(0, i);

        Coordination::Error rc = zookeeper->tryRemove(path_to_remove);
        if (rc != Coordination::Error::ZOK)
            /// Znode not empty or already removed by someone else.
            break;
    }
}

}
