#include <Storages/MergeTree/PhysicalNameMapping.h>

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
constexpr auto KEY_NEXT_PHYSICAL_COLUMN_ID = "next_physical_column_id";
constexpr auto KEY_MAPPING = "mapping";

/// Scans column names (or physical names) to find the highest value that
/// parses as a valid UInt64, then returns max + 1.  This prevents collisions
/// when a table already has numeric column names like "2" or "10" — the
/// counter must start above them.
UInt64 getNextPhysicalColumnId(const NamesAndTypesList & columns)
{
    UInt64 max_numeric_physical_name = 0;

    for (const auto & column : columns)
    {
        const auto & name = column.name;
        UInt64 value = 0;
        const auto * begin = name.data();
        const auto * end = begin + name.size();
        auto [ptr, ec] = std::from_chars(begin, end, value);
        if (ec == std::errc() && ptr == end)
            max_numeric_physical_name = std::max(max_numeric_physical_name, value);
    }

    return max_numeric_physical_name + 1;
}

UInt64 getNextPhysicalColumnId(const std::unordered_map<String, String> & logical_to_physical)
{
    UInt64 max_numeric_physical_name = 0;

    for (const auto & [_, physical_name] : logical_to_physical)
    {
        UInt64 value = 0;
        const auto * begin = physical_name.data();
        const auto * end = begin + physical_name.size();
        auto [ptr, ec] = std::from_chars(begin, end, value);
        if (ec == std::errc() && ptr == end)
            max_numeric_physical_name = std::max(max_numeric_physical_name, value);
    }

    return max_numeric_physical_name + 1;
}

[[noreturn]] void throwMissingLogicalName(const String & logical_name)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Logical column name '{}' is not found in `PhysicalNameMapping`", logical_name);
}

[[noreturn]] void throwMissingPhysicalName(const String & physical_name)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Physical column name '{}' is not found in `PhysicalNameMapping`", physical_name);
}

}

String PhysicalNameMapping::getPhysicalName(const String & logical_name) const
{
    auto it = logical_to_physical.find(logical_name);
    if (it == logical_to_physical.end())
        throwMissingLogicalName(logical_name);

    return it->second;
}

String PhysicalNameMapping::getPhysicalNameOrDefault(const String & logical_name) const
{
    auto it = logical_to_physical.find(logical_name);
    return it == logical_to_physical.end() ? logical_name : it->second;
}

String PhysicalNameMapping::getLogicalName(const String & physical_name) const
{
    auto it = physical_to_logical.find(physical_name);
    if (it == physical_to_logical.end())
        throwMissingPhysicalName(physical_name);

    return it->second;
}

bool PhysicalNameMapping::hasLogicalName(const String & logical_name) const
{
    return logical_to_physical.contains(logical_name);
}

bool PhysicalNameMapping::hasPhysicalName(const String & physical_name) const
{
    return physical_to_logical.contains(physical_name);
}

String PhysicalNameMapping::allocatePhysicalName()
{
    /// The counter is monotonically increasing and never recycled.
    /// This guarantees that DROP column "x" followed by ADD column "x"
    /// always gets a different physical name, even if the logical name
    /// is reused.  Old parts still reference the old physical name,
    /// which is now orphaned — the reader's loadColumns remapping
    /// (physical-name-first algorithm) will correctly skip it.
    active = true;
    return ::DB::toString(next_physical_column_id++);
}

void PhysicalNameMapping::addColumn(const String & logical_name, const String & physical_name)
{
    if (logical_to_physical.contains(logical_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical column name '{}' is already registered in `PhysicalNameMapping`", logical_name);

    if (physical_to_logical.contains(physical_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Physical column name '{}' is already registered in `PhysicalNameMapping`", physical_name);

    active = true;
    logical_to_physical.emplace(logical_name, physical_name);
    physical_to_logical.emplace(physical_name, logical_name);
}

void PhysicalNameMapping::removeColumn(const String & logical_name)
{
    auto it = logical_to_physical.find(logical_name);
    if (it == logical_to_physical.end())
        throwMissingLogicalName(logical_name);

    physical_to_logical.erase(it->second);
    logical_to_physical.erase(it);
}

void PhysicalNameMapping::renameColumn(const String & old_logical_name, const String & new_logical_name)
{
    auto it = logical_to_physical.find(old_logical_name);
    if (it == logical_to_physical.end())
        throwMissingLogicalName(old_logical_name);

    if (logical_to_physical.contains(new_logical_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical column name '{}' is already registered in `PhysicalNameMapping`", new_logical_name);

    auto physical_name = it->second;
    logical_to_physical.erase(it);
    logical_to_physical.emplace(new_logical_name, physical_name);
    physical_to_logical[physical_name] = new_logical_name;
}

Names PhysicalNameMapping::logicalNames() const
{
    Names names;
    names.reserve(logical_to_physical.size());

    for (const auto & [logical_name, _] : logical_to_physical)
        names.push_back(logical_name);

    return names;
}

PhysicalNameMapping PhysicalNameMapping::createForExistingTable(const NamesAndTypesList & columns)
{
    PhysicalNameMapping mapping;
    mapping.active = true;
    /// Identity mapping: physical_name == column_name for every existing column.
    /// This is safe because files on disk were written using the column name,
    /// so the identity mapping matches the actual file layout.
    mapping.next_physical_column_id = getNextPhysicalColumnId(columns);

    for (const auto & column : columns)
        mapping.addColumn(column.name, column.name);

    return mapping;
}

PhysicalNameMapping PhysicalNameMapping::createForNewTable(const NamesAndTypesList & columns)
{
    /// Same as existing-table activation: initial columns use identity mapping.
    /// A separate entry point exists so that future work can diverge
    /// (e.g. assign numeric names from the start for new tables).
    return createForExistingTable(columns);
}

void PhysicalNameMapping::serialize(WriteBuffer & buf) const
{
    writeString(toString(), buf);
}

PhysicalNameMapping PhysicalNameMapping::deserialize(ReadBuffer & buf)
{
    String json;
    readString(json, buf);
    return fromString(json);
}

String PhysicalNameMapping::toString() const
{
    Poco::JSON::Object json;
    Poco::JSON::Object mapping_json;

    std::vector<std::pair<String, String>> mapping_entries(logical_to_physical.begin(), logical_to_physical.end());
    std::sort(mapping_entries.begin(), mapping_entries.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.first < rhs.first;
    });

    for (const auto & [logical_name, physical_name] : mapping_entries)
        mapping_json.set(logical_name, physical_name);

    json.set(KEY_ACTIVE, active);
    json.set(KEY_NEXT_PHYSICAL_COLUMN_ID, next_physical_column_id);
    json.set(KEY_MAPPING, mapping_json);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

PhysicalNameMapping PhysicalNameMapping::fromString(const String & str)
{
    Poco::JSON::Parser parser;
    auto object = parser.parse(str).extract<Poco::JSON::Object::Ptr>();

    PhysicalNameMapping mapping;

    if (object->has(KEY_ACTIVE))
        mapping.active = object->getValue<bool>(KEY_ACTIVE);

    if (object->has(KEY_NEXT_PHYSICAL_COLUMN_ID))
        mapping.next_physical_column_id = object->getValue<UInt64>(KEY_NEXT_PHYSICAL_COLUMN_ID);

    if (!object->has(KEY_MAPPING))
        return mapping;

    auto mapping_object = object->getObject(KEY_MAPPING);
    for (const auto & [logical_name, physical_name_value] : *mapping_object)
        mapping.addColumn(logical_name, physical_name_value.convert<String>());

    mapping.active = mapping.active || !mapping.logical_to_physical.empty();
    if (mapping.next_physical_column_id == 1 && mapping.active)
        mapping.next_physical_column_id = getNextPhysicalColumnId(mapping.logical_to_physical);

    return mapping;
}

void populatePhysicalNames(NamesAndTypesList & columns, const PhysicalNameMapping & mapping)
{
    if (!mapping.isActive())
        return;

    for (auto & column : columns)
        column.setPhysicalName(mapping.getPhysicalNameOrDefault(column.getNameInStorage()));
}

}
