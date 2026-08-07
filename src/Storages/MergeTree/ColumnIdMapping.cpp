#include <Storages/MergeTree/ColumnIdMapping.h>

#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <charconv>
#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto KEY_ACTIVE = "active";
constexpr auto KEY_NEXT_COLUMN_ID = "next_column_id";
constexpr auto KEY_MAPPING = "mapping";

/// Scans column names (or column IDs) to find the highest value that
/// parses as a valid UInt64, then returns max + 1.  This prevents collisions
/// when a table already has numeric column names like "2" or "10" — the
/// counter must start above them.
UInt64 safeIncrementColumnId(UInt64 max_id)
{
    if (max_id == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column ID counter overflow: the maximum value {} has been reached", max_id);
    return max_id + 1;
}

UInt64 getNextColumnId(const NamesAndTypesList & columns)
{
    UInt64 max_numeric_column_id = 0;
    for (const auto & column : columns)
        max_numeric_column_id = std::max(max_numeric_column_id, ColumnIdMapping::extractNumericCounter(column.name));
    return safeIncrementColumnId(max_numeric_column_id);
}

UInt64 getNextColumnId(const std::unordered_map<String, String> & logical_to_id)
{
    UInt64 max_numeric_column_id = 0;
    for (const auto & [_, column_id] : logical_to_id)
        max_numeric_column_id = std::max(max_numeric_column_id, ColumnIdMapping::extractNumericCounter(column_id));
    return safeIncrementColumnId(max_numeric_column_id);
}

[[noreturn]] void throwMissingLogicalName(const String & logical_name)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Logical column name '{}' is not found in `ColumnIdMapping`", logical_name);
}

[[noreturn]] void throwMissingColumnId(const String & column_id)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column ID '{}' is not found in `ColumnIdMapping`", column_id);
}

}

UInt64 ColumnIdMapping::extractNumericCounter(const String & s)
{
    UInt64 value = 0;
    const auto * begin = s.data();
    const auto * end = begin + s.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc())
        return 0;
    if (ptr == end)
        return value;
    if (*ptr == '.' && ptr + 1 != end)
        return value;
    return 0;
}

String ColumnIdMapping::getColumnId(const String & logical_name) const
{
    auto it = logical_to_id.find(logical_name);
    if (it == logical_to_id.end())
        throwMissingLogicalName(logical_name);

    return it->second;
}

String ColumnIdMapping::getColumnIdOrDefault(const String & logical_name) const
{
    auto it = logical_to_id.find(logical_name);
    return it == logical_to_id.end() ? logical_name : it->second;
}

String ColumnIdMapping::getLogicalName(const String & column_id) const
{
    auto it = id_to_logical.find(column_id);
    if (it == id_to_logical.end())
        throwMissingColumnId(column_id);

    return it->second;
}

bool ColumnIdMapping::hasLogicalName(const String & logical_name) const
{
    return logical_to_id.contains(logical_name);
}

bool ColumnIdMapping::hasColumnId(const String & column_id) const
{
    return id_to_logical.contains(column_id);
}

std::optional<ColumnId> ColumnIdMapping::tryGetColumnId(const String & logical_name) const
{
    if (!hasLogicalName(logical_name))
        return std::nullopt;
    return ColumnId{getColumnId(logical_name)};
}

std::optional<String> ColumnIdMapping::tryGetLogicalName(const ColumnId & column_id) const
{
    if (!hasColumnId(column_id.value()))
        return std::nullopt;
    return getLogicalName(column_id.value());
}

String ColumnIdMapping::allocateColumnId()
{
    /// The counter is monotonically increasing and never recycled.
    /// This guarantees that DROP column "x" followed by ADD column "x"
    /// always gets a different column ID, even if the logical name
    /// is reused.  Old parts still reference the old column ID,
    /// which is now orphaned — the reader's loadColumns remapping
    /// (column-ID-first algorithm) will correctly skip it.
    active = true;
    auto id = next_column_id;
    next_column_id = safeIncrementColumnId(next_column_id);
    return ::DB::toString(id);
}

void ColumnIdMapping::addColumn(const String & logical_name, const String & column_id)
{
    if (logical_to_id.contains(logical_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical column name '{}' is already registered in `ColumnIdMapping`", logical_name);

    if (id_to_logical.contains(column_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column ID '{}' is already registered in `ColumnIdMapping`", column_id);

    active = true;
    logical_to_id.emplace(logical_name, column_id);
    id_to_logical.emplace(column_id, logical_name);
}

void ColumnIdMapping::removeColumn(const String & logical_name)
{
    auto it = logical_to_id.find(logical_name);
    if (it == logical_to_id.end())
        throwMissingLogicalName(logical_name);

    const String column_id = it->second;
    logical_to_id.erase(it);

    for (const auto & [other_logical, other_column_id] : logical_to_id)
    {
        if (other_column_id == column_id)
        {
            id_to_logical[column_id] = other_logical;
            return;
        }
    }
    id_to_logical.erase(column_id);
}

void ColumnIdMapping::renameColumn(const String & old_logical_name, const String & new_logical_name)
{
    auto it = logical_to_id.find(old_logical_name);
    if (it == logical_to_id.end())
        throwMissingLogicalName(old_logical_name);

    if (logical_to_id.contains(new_logical_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical column name '{}' is already registered in `ColumnIdMapping`", new_logical_name);

    auto column_id = it->second;

    if (id_to_logical.contains(new_logical_name) && new_logical_name != column_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot rename column '{}' to '{}': the new name collides with an existing column ID",
            old_logical_name, new_logical_name);

    logical_to_id.erase(it);
    logical_to_id.emplace(new_logical_name, column_id);
    id_to_logical[column_id] = new_logical_name;
}

void ColumnIdMapping::beginRename(const String & old_logical_name, const String & new_logical_name)
{
    auto it = logical_to_id.find(old_logical_name);
    if (it == logical_to_id.end())
        throwMissingLogicalName(old_logical_name);

    if (logical_to_id.contains(new_logical_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical column name '{}' is already registered in `ColumnIdMapping`", new_logical_name);

    auto column_id = it->second;

    /// Reject renaming a column to a name equal to another active column's ID. On-disk
    /// artifacts (streams, minmax, sizes) are keyed by the column id, so a logical name that equals
    /// a foreign column's id makes name-vs-id resolution ambiguous -- reachable via a mutation that
    /// then reads/writes the wrong streams (silent data corruption). Allowing it safely would need
    /// id-vs-name disambiguation at every by-name resolution site; until then, reject it loudly.
    /// The self-case (a column adopting its own id as its name) is fine.
    if (id_to_logical.contains(new_logical_name) && new_logical_name != column_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot rename column '{}' to '{}': the new name collides with an existing column ID",
            old_logical_name, new_logical_name);

    logical_to_id.emplace(new_logical_name, column_id);
    /// Do NOT update id_to_logical here — keep it pointing to
    /// old_logical_name so the reverse map stays consistent with the
    /// still-uncommitted metadata.  `reconcileColumnIdMappingWithMetadata`
    /// resolves any ambiguity on restart.  `finishRename` updates the
    /// reverse map once the metadata commit is confirmed.
}

void ColumnIdMapping::finishRename(const String & old_logical_name)
{
    auto it = logical_to_id.find(old_logical_name);
    /// `beginRename` registered the old name and nothing between the two phases removes it: the
    /// planner only adds, and `AlterCommands::validate` rejects two renames of the same column.
    if (it == logical_to_id.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "`finishRename` of logical column name '{}' without a matching `beginRename`", old_logical_name);

    const String column_id = it->second;
    logical_to_id.erase(it);

    for (const auto & [logical, other_column_id] : logical_to_id)
    {
        if (other_column_id == column_id)
        {
            id_to_logical[column_id] = logical;
            return;
        }
    }
    id_to_logical.erase(column_id);
}

Names ColumnIdMapping::logicalNames() const
{
    Names names;
    names.reserve(logical_to_id.size());

    for (const auto & [logical_name, _] : logical_to_id)
        names.push_back(logical_name);

    return names;
}

ColumnIdMapping ColumnIdMapping::createIdentity(const NamesAndTypesList & columns)
{
    ColumnIdMapping mapping;
    mapping.active = true;
    mapping.next_column_id = getNextColumnId(columns);

    for (const auto & column : columns)
        mapping.addColumn(column.name, column.name);

    return mapping;
}

void ColumnIdMapping::serialize(WriteBuffer & buf) const
{
    writeString(toString(), buf);
}

ColumnIdMapping ColumnIdMapping::deserialize(ReadBuffer & buf)
{
    String json;
    readString(json, buf);
    return fromString(json);
}

String ColumnIdMapping::toString() const
{
    Poco::JSON::Object json;
    Poco::JSON::Object mapping_json;

    std::vector<std::pair<String, String>> mapping_entries(logical_to_id.begin(), logical_to_id.end());
    std::sort(mapping_entries.begin(), mapping_entries.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.first < rhs.first;
    });

    for (const auto & [logical_name, column_id] : mapping_entries)
        mapping_json.set(logical_name, column_id);

    json.set(KEY_ACTIVE, active);
    json.set(KEY_NEXT_COLUMN_ID, next_column_id);
    json.set(KEY_MAPPING, mapping_json);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

ColumnIdMapping ColumnIdMapping::fromString(const String & str)
{
    Poco::JSON::Parser parser;
    auto object = parser.parse(str).extract<Poco::JSON::Object::Ptr>();

    ColumnIdMapping mapping;

    if (object->has(KEY_ACTIVE))
        mapping.active = object->getValue<bool>(KEY_ACTIVE);

    if (object->has(KEY_NEXT_COLUMN_ID))
        mapping.next_column_id = object->getValue<UInt64>(KEY_NEXT_COLUMN_ID);

    if (!object->has(KEY_MAPPING))
        return mapping;

    auto mapping_object = object->getObject(KEY_MAPPING);
    for (const auto & [logical_name, column_id_value] : *mapping_object)
    {
        String column_id = column_id_value.convert<String>();
        mapping.logical_to_id.emplace(logical_name, column_id);
        mapping.id_to_logical[column_id] = logical_name;
    }

    /// During two-phase rename, both old and new logical names map to the
    /// same column ID.  The `operator[]` above may have picked either
    /// winner depending on JSON key iteration order.  Rebuild the reverse
    /// map deterministically: for each column ID with multiple logical
    /// names, prefer the lexicographically smallest one.  This is arbitrary
    /// but stable; `reconcileColumnIdMappingWithMetadata` will remove
    /// the stale entry immediately after startup anyway.
    if (mapping.id_to_logical.size() < mapping.logical_to_id.size())
    {
        mapping.id_to_logical.clear();
        for (const auto & [logical, column_id] : mapping.logical_to_id)
        {
            auto it = mapping.id_to_logical.find(column_id);
            if (it == mapping.id_to_logical.end() || logical < it->second)
                mapping.id_to_logical[column_id] = logical;
        }
    }

    mapping.active = mapping.active || !mapping.logical_to_id.empty();
    if (!mapping.logical_to_id.empty())
        mapping.next_column_id = std::max(mapping.next_column_id, getNextColumnId(mapping.logical_to_id));

    return mapping;
}

void ColumnIdMapping::stampColumnIds(NamesAndTypesList & columns) const
{
    if (!active)
        return;

    for (auto & column : columns)
    {
        /// Preserve an already-stamped part-local id. Some read callers (e.g.
        /// getListOfStreamsForColumn, for subcolumn sizes) pass columns already stamped with
        /// the part's real id; re-stamping from the (possibly live) mapping would clobber it
        /// after a DROP + re-ADD name reuse. On the write path columns arrive id-less, so
        /// this guard is a no-op there.
        if (!column.column_id.empty())
            continue;

        const auto name_in_storage = column.getNameInStorage();
        if (auto id = tryGetColumnId(name_in_storage))
            column.setColumnId(*id);
        /// Virtual columns are not id-managed: persistent ones are stored by name, ephemeral
        /// ones (e.g. `_part_offset`, `_part_data_version`) are materialized by the reader.
        /// Leave them UNSTAMPED (empty id ≡ name-keyed on disk) for the name-resolution path.
        else if (!isVirtualColumn(name_in_storage))
            /// A real physical column absent from the active mapping is a schema/mapping
            /// desync — a torn snapshot would otherwise stamp ids that no reader resolves
            /// (write) or mis-resolve by a stale name (read). Fail loud instead.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column '{}' is absent from the active column-ID mapping while stamping a part; "
                "the table schema and column-ID mapping have desynced", name_in_storage);
    }
}

void ColumnIdMapping::stampColumnIdsLenient(NamesAndTypesList & columns) const
{
    if (!active)
        return;

    for (auto & column : columns)
    {
        const auto name_in_storage = column.getNameInStorage();
        if (auto id = tryGetColumnId(name_in_storage))
            column.setColumnId(*id);
        /// Everything else (synthetic projection aggregates, not-yet-applied ALTER columns,
        /// a projection part's parent-mapping-stamped columns) is left UNSTAMPED
        /// (empty id ≡ name-keyed on disk). No throw.
    }
}

ColumnId getColumnIdInPart(const NamesAndTypesList & part_columns, const String & column_name)
{
    auto column = part_columns.tryGetByName(column_name);
    return column ? column->getColumnId() : ColumnId{column_name};
}

String getColumnNameByIdInPart(const NamesAndTypesList & part_columns, const ColumnId & column_id)
{
    for (const auto & column : part_columns)
        if (column.getColumnId() == column_id)
            return column.name;
    return column_id.value();
}

}
