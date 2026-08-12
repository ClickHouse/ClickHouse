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

UInt64 safeIncrementColumnId(UInt64 max_id)
{
    if (max_id == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column ID counter overflow: the maximum value {} has been reached", max_id);
    return max_id + 1;
}

/// The counter starts above every numeric column NAME as well, or a table that already has a column
/// literally named "2" collides with the ID a later `ADD COLUMN` allocates.
UInt64 getNextColumnId(const NamesAndTypesList & columns)
{
    UInt64 max_numeric_column_id = 0;
    for (const auto & column : columns)
        max_numeric_column_id = std::max(max_numeric_column_id, ColumnIdMapping::extractNumericCounter(column.name));
    return safeIncrementColumnId(max_numeric_column_id);
}

UInt64 getNextColumnId(const std::unordered_map<String, String> & name_to_id)
{
    UInt64 max_numeric_column_id = 0;
    for (const auto & [_, column_id] : name_to_id)
        max_numeric_column_id = std::max(max_numeric_column_id, ColumnIdMapping::extractNumericCounter(column_id));
    return safeIncrementColumnId(max_numeric_column_id);
}

[[noreturn]] void throwMissingColumnName(const String & column_name)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column name '{}' is not found in `ColumnIdMapping`", column_name);
}

[[noreturn]] void throwMissingColumnId(const String & column_id)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column ID '{}' is not found in `ColumnIdMapping`", column_id);
}

}

UInt64 ColumnIdMapping::extractNumericCounter(const String & s)
{
    /// Every dot-separated numeric component counts, not just the first: a flattened Nested child id
    /// is "<parent counter>.<child counter>" ("5.7"), and both halves come from this counter, so the
    /// counter has to end up above both -- for a numeric column NAME at activation just the same.
    UInt64 max_component = 0;
    size_t component_begin = 0;
    while (true)
    {
        const auto separator = s.find('.', component_begin);
        const auto component_size = (separator == String::npos ? s.size() : separator) - component_begin;

        UInt64 value = 0;
        const auto * begin = s.data() + component_begin;
        const auto * end = begin + component_size;
        auto [ptr, ec] = std::from_chars(begin, end, value);
        if (ec == std::errc() && ptr == end)
            max_component = std::max(max_component, value);

        if (separator == String::npos)
            return max_component;
        component_begin = separator + 1;
    }
}

String ColumnIdMapping::getColumnId(const String & column_name) const
{
    auto it = name_to_id.find(column_name);
    if (it == name_to_id.end())
        throwMissingColumnName(column_name);

    return it->second;
}

String ColumnIdMapping::getColumnIdOrDefault(const String & column_name) const
{
    auto it = name_to_id.find(column_name);
    return it == name_to_id.end() ? column_name : it->second;
}

String ColumnIdMapping::getColumnName(const String & column_id) const
{
    auto it = id_to_name.find(column_id);
    if (it == id_to_name.end())
        throwMissingColumnId(column_id);

    return it->second;
}

bool ColumnIdMapping::hasColumnName(const String & column_name) const
{
    return name_to_id.contains(column_name);
}

bool ColumnIdMapping::hasColumnId(const String & column_id) const
{
    return id_to_name.contains(column_id);
}

std::optional<ColumnId> ColumnIdMapping::tryGetColumnId(const String & column_name) const
{
    if (!hasColumnName(column_name))
        return std::nullopt;
    return ColumnId{getColumnId(column_name)};
}

std::optional<String> ColumnIdMapping::tryGetColumnName(const ColumnId & column_id) const
{
    if (!hasColumnId(column_id.value()))
        return std::nullopt;
    return getColumnName(column_id.value());
}

String ColumnIdMapping::allocateColumnId()
{
    /// The counter is monotonically increasing and never recycled.
    /// This guarantees that DROP column "x" followed by ADD column "x"
    /// always gets a different column ID, even if the column name
    /// is reused.  Old parts still reference the old column ID,
    /// which is now orphaned — the reader's loadColumns remapping
    /// (column-ID-first algorithm) will correctly skip it.
    active = true;
    auto id = next_column_id;
    next_column_id = safeIncrementColumnId(next_column_id);
    return ::DB::toString(id);
}

void ColumnIdMapping::addColumn(const String & column_name, const String & column_id)
{
    if (name_to_id.contains(column_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column name '{}' is already registered in `ColumnIdMapping`", column_name);

    if (id_to_name.contains(column_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column ID '{}' is already registered in `ColumnIdMapping`", column_id);

    active = true;
    name_to_id.emplace(column_name, column_id);
    id_to_name.emplace(column_id, column_name);
}

void ColumnIdMapping::detachColumnName(std::unordered_map<String, String>::iterator it)
{
    const String column_id = it->second;
    name_to_id.erase(it);

    /// A second name can still hold the id -- that is the transient state `beginRename` leaves --
    /// so the reverse entry is re-pointed at the survivor rather than erased.
    for (const auto & [other_name, other_column_id] : name_to_id)
    {
        if (other_column_id == column_id)
        {
            id_to_name[column_id] = other_name;
            return;
        }
    }
    id_to_name.erase(column_id);
}

void ColumnIdMapping::removeColumn(const String & column_name)
{
    auto it = name_to_id.find(column_name);
    if (it == name_to_id.end())
        throwMissingColumnName(column_name);

    detachColumnName(it);
}

/// Two phases, because `column_ids.json` and the catalog's metadata cannot be written atomically.
/// Phase 1 adds the new name beside the old one, both pointing at the same id, and persists; the
/// metadata commit follows; `finishRename` then drops the old name.
///     phase 1:  { "b":"b", "name":"b" }    metadata still says "b"
///     phase 2:  { "name":"b" }             metadata says "name"
/// On restart, reconciliation drops mapping entries metadata does not name, which lands on the right
/// state from either crash window: before the commit it keeps "b", after it keeps "name". A
/// single-step rename would lose the id "b" outright if the crash fell between the two writes.
void ColumnIdMapping::beginRename(const String & old_column_name, const String & new_column_name)
{
    auto it = name_to_id.find(old_column_name);
    if (it == name_to_id.end())
        throwMissingColumnName(old_column_name);

    if (name_to_id.contains(new_column_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column name '{}' is already registered in `ColumnIdMapping`", new_column_name);

    auto column_id = it->second;

    /// Reject renaming a column to a name equal to another active column's ID. On-disk
    /// artifacts (streams, minmax, sizes) are keyed by the column id, so a column name that equals
    /// a foreign column's id makes name-vs-id resolution ambiguous -- reachable via a mutation that
    /// then reads/writes the wrong streams (silent data corruption). Allowing it safely would need
    /// id-vs-name disambiguation at every by-name resolution site; until then, reject it loudly.
    /// The self-case (a column adopting its own id as its name) is fine.
    if (id_to_name.contains(new_column_name) && new_column_name != column_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot rename column '{}' to '{}': the new name collides with an existing column ID",
            old_column_name, new_column_name);

    /// The reverse map stays on the old name, which is what the uncommitted metadata still says;
    /// `finishRename` moves it once the commit is confirmed.
    name_to_id.emplace(new_column_name, column_id);
}

void ColumnIdMapping::finishRename(const String & old_column_name)
{
    auto it = name_to_id.find(old_column_name);
    /// `beginRename` registered the old name and nothing between the two phases removes it: the
    /// planner only adds, and `AlterCommands::validate` rejects two renames of the same column.
    if (it == name_to_id.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "`finishRename` of column name '{}' without a matching `beginRename`", old_column_name);

    detachColumnName(it);
}

Names ColumnIdMapping::columnNames() const
{
    Names names;
    names.reserve(name_to_id.size());

    for (const auto & [column_name, _] : name_to_id)
        names.push_back(column_name);

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

    std::vector<std::pair<String, String>> mapping_entries(name_to_id.begin(), name_to_id.end());
    std::sort(mapping_entries.begin(), mapping_entries.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.first < rhs.first;
    });

    for (const auto & [column_name, column_id] : mapping_entries)
        mapping_json.set(column_name, column_id);

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
    for (const auto & [column_name, column_id_value] : *mapping_object)
    {
        String column_id = column_id_value.convert<String>();
        mapping.name_to_id.emplace(column_name, column_id);
        mapping.id_to_name[column_id] = column_name;
    }

    /// During two-phase rename, both old and new names map to the
    /// same column ID.  The `operator[]` above may have picked either
    /// winner depending on JSON key iteration order.  Rebuild the reverse
    /// map deterministically: for each column ID with multiple names,
    /// prefer the lexicographically smallest one.  This is arbitrary
    /// but stable; `reconcileColumnIdMappingWithMetadata` will remove
    /// the stale entry immediately after startup anyway.
    if (mapping.id_to_name.size() < mapping.name_to_id.size())
    {
        mapping.id_to_name.clear();
        for (const auto & [column_name, column_id] : mapping.name_to_id)
        {
            auto it = mapping.id_to_name.find(column_id);
            if (it == mapping.id_to_name.end() || column_name < it->second)
                mapping.id_to_name[column_id] = column_name;
        }
    }

    mapping.active = mapping.active || !mapping.name_to_id.empty();
    if (!mapping.name_to_id.empty())
        mapping.next_column_id = std::max(mapping.next_column_id, getNextColumnId(mapping.name_to_id));

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

}
