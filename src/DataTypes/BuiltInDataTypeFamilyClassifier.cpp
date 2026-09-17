#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>

#include <array>
#include <limits>

namespace DB
{
namespace
{

using CreatorInputClass = BuiltInDataTypeCreatorInputClass;
using FamilyInfo = BuiltInDataTypeFamilyInfo;
using Classification = BuiltInDataTypeFamilyClassification;
using FamilyIndex = std::uint16_t;

constexpr FamilyIndex empty_family_index = std::numeric_limits<FamilyIndex>::max();

constexpr unsigned char asciiLower(unsigned char value) noexcept
{
    if (value >= static_cast<unsigned char>('A') && value <= static_cast<unsigned char>('Z'))
        return static_cast<unsigned char>(value + ('a' - 'A'));
    return value;
}

constexpr bool asciiCaseInsensitiveEqual(std::string_view lhs, std::string_view rhs) noexcept
{
    if (lhs.size() != rhs.size())
        return false;

    for (std::size_t index = 0; index < lhs.size(); ++index)
    {
        if (asciiLower(static_cast<unsigned char>(lhs[index])) != asciiLower(static_cast<unsigned char>(rhs[index])))
            return false;
    }
    return true;
}

constexpr CreatorInputClass creatorInputClass(std::string_view canonical_creator_name) noexcept
{
    if (canonical_creator_name == "Enum" || canonical_creator_name == "Enum8" || canonical_creator_name == "Enum16")
        return CreatorInputClass::CanonicalizeGenericEnumArguments;
    return CreatorInputClass::ReadOnly;
}

consteval FamilyInfo
makeCreator(std::string_view registered_name, bool registration_case_insensitive = false, bool case_insensitive_via_alias = false)
{
    return {
        registered_name,
        registered_name,
        registration_case_insensitive || case_insensitive_via_alias,
        registration_case_insensitive,
        false,
        creatorInputClass(registered_name),
    };
}

consteval FamilyInfo makeAlias(std::string_view registered_name, std::string_view canonical_creator_name, bool case_insensitive = false)
{
    return {
        registered_name,
        canonical_creator_name,
        case_insensitive,
        case_insensitive,
        true,
        creatorInputClass(canonical_creator_name),
    };
}

/// Frozen DataTypeFactory inventory at 8f2995c11826ffd6d5a16cd7622f9cd7b10b38d5.
/// The sorted, newline-terminated registered names have SHA-256
/// e0e4d118d3b323ad2f1b383fb9a19724d01ec278740bf25965f856eb48afb4ff.
constexpr std::array families{
    makeCreator("AggregateFunction"),
    makeCreator("Array"),
    makeCreator("BFloat16"),
    makeAlias("BIGINT", "Int64", true),
    makeAlias("BIGINT SIGNED", "Int64", true),
    makeAlias("BIGINT UNSIGNED", "UInt64", true),
    makeAlias("BINARY", "FixedString", true),
    makeAlias("BINARY LARGE OBJECT", "String", true),
    makeAlias("BINARY VARYING", "String", true),
    makeAlias("BIT", "UInt64", true),
    makeAlias("BLOB", "String", true),
    makeAlias("BYTE", "Int8", true),
    makeAlias("BYTEA", "String", true),
    /// isCaseInsensitive() observes the folded `bool` alias for this exact creator.
    makeCreator("Bool", false, true),
    makeAlias("CHAR", "String", true),
    makeAlias("CHAR LARGE OBJECT", "String", true),
    makeAlias("CHAR VARYING", "String", true),
    makeAlias("CHARACTER", "String", true),
    makeAlias("CHARACTER LARGE OBJECT", "String", true),
    makeAlias("CHARACTER VARYING", "String", true),
    makeAlias("CLOB", "String", true),
    makeAlias("DEC", "Decimal", true),
    makeAlias("DOUBLE", "Float64", true),
    makeAlias("DOUBLE PRECISION", "Float64", true),
    makeCreator("Date", true),
    makeCreator("Date32", true),
    makeCreator("DateTime", true),
    makeCreator("DateTime32", true),
    makeCreator("DateTime64", true),
    makeCreator("Decimal", true),
    makeCreator("Decimal128", true),
    makeCreator("Decimal256", true),
    makeCreator("Decimal32", true),
    makeCreator("Decimal64", true),
    makeCreator("Dynamic"),
    makeAlias("ENUM", "Enum", true),
    /// isCaseInsensitive() observes the folded `ENUM` alias for this exact creator.
    makeCreator("Enum", false, true),
    makeCreator("Enum16"),
    makeCreator("Enum8"),
    makeAlias("FIXED", "Decimal", true),
    makeAlias("FLOAT", "Float32", true),
    makeCreator("FixedString"),
    makeCreator("Float32"),
    makeCreator("Float64"),
    makeAlias("GEOMETRY", "Geometry"),
    makeCreator("Geometry"),
    makeAlias("INET4", "IPv4", true),
    makeAlias("INET6", "IPv6", true),
    makeAlias("INT", "Int32", true),
    makeAlias("INT SIGNED", "Int32", true),
    makeAlias("INT UNSIGNED", "UInt32", true),
    makeAlias("INT1", "Int8", true),
    makeAlias("INT1 SIGNED", "Int8", true),
    makeAlias("INT1 UNSIGNED", "UInt8", true),
    makeAlias("INTEGER", "Int32", true),
    makeAlias("INTEGER SIGNED", "Int32", true),
    makeAlias("INTEGER UNSIGNED", "UInt32", true),
    makeCreator("IPv4"),
    makeCreator("IPv6"),
    makeCreator("Int128"),
    makeCreator("Int16"),
    makeCreator("Int256"),
    makeCreator("Int32"),
    makeCreator("Int64"),
    makeCreator("Int8"),
    makeCreator("IntervalDay"),
    makeCreator("IntervalHour"),
    makeCreator("IntervalMicrosecond"),
    makeCreator("IntervalMillisecond"),
    makeCreator("IntervalMinute"),
    makeCreator("IntervalMonth"),
    makeCreator("IntervalNanosecond"),
    makeCreator("IntervalQuarter"),
    makeCreator("IntervalSecond"),
    makeCreator("IntervalWeek"),
    makeCreator("IntervalYear"),
    makeCreator("JSON", true),
    makeAlias("LONGBLOB", "String", true),
    makeAlias("LONGTEXT", "String", true),
    makeCreator("LineString"),
    makeCreator("LowCardinality"),
    makeAlias("MEDIUMBLOB", "String", true),
    makeAlias("MEDIUMINT", "Int32", true),
    makeAlias("MEDIUMINT SIGNED", "Int32", true),
    makeAlias("MEDIUMINT UNSIGNED", "UInt32", true),
    makeAlias("MEDIUMTEXT", "String", true),
    makeCreator("Map"),
    makeCreator("MultiLineString"),
    makeCreator("MultiPoint"),
    makeCreator("MultiPolygon"),
    makeAlias("NATIONAL CHAR", "String", true),
    makeAlias("NATIONAL CHAR VARYING", "String", true),
    makeAlias("NATIONAL CHARACTER", "String", true),
    makeAlias("NATIONAL CHARACTER LARGE OBJECT", "String", true),
    makeAlias("NATIONAL CHARACTER VARYING", "String", true),
    makeAlias("NCHAR", "String", true),
    makeAlias("NCHAR LARGE OBJECT", "String", true),
    makeAlias("NCHAR VARYING", "String", true),
    makeAlias("NUMERIC", "Decimal", true),
    makeAlias("NVARCHAR", "String", true),
    makeCreator("Nested"),
    makeCreator("Nothing"),
    makeCreator("Nullable"),
    makeCreator("Point"),
    makeCreator("Polygon"),
    makeCreator("QBit"),
    makeAlias("REAL", "Float32", true),
    makeCreator("Ring"),
    makeAlias("SET", "UInt64", true),
    makeAlias("SIGNED", "Int64", true),
    makeAlias("SINGLE", "Float32", true),
    makeAlias("SMALLINT", "Int16", true),
    makeAlias("SMALLINT SIGNED", "Int16", true),
    makeAlias("SMALLINT UNSIGNED", "UInt16", true),
    makeCreator("SimpleAggregateFunction"),
    makeCreator("String"),
    makeAlias("TEXT", "String", true),
    makeAlias("TIMESTAMP", "DateTime", true),
    makeAlias("TINYBLOB", "String", true),
    makeAlias("TINYINT", "Int8", true),
    makeAlias("TINYINT SIGNED", "Int8", true),
    makeAlias("TINYINT UNSIGNED", "UInt8", true),
    makeAlias("TINYTEXT", "String", true),
    makeCreator("Time", true),
    makeCreator("Time64", true),
    makeCreator("Tuple"),
    makeCreator("UInt128"),
    makeCreator("UInt16"),
    makeCreator("UInt256"),
    makeCreator("UInt32"),
    makeCreator("UInt64"),
    makeCreator("UInt8"),
    makeAlias("UNSIGNED", "UInt64", true),
    makeCreator("UUID"),
    makeAlias("VARBINARY", "String", true),
    makeAlias("VARCHAR", "String", true),
    makeAlias("VARCHAR2", "String", true),
    makeCreator("Variant"),
    makeAlias("YEAR", "UInt16", true),
    makeAlias("bool", "Bool", true),
    makeAlias("boolean", "Bool", true),
};

consteval bool inventoryIsStrictlySortedAndUnique()
{
    for (std::size_t index = 1; index < families.size(); ++index)
    {
        if (!(families[index - 1].registered_name < families[index].registered_name))
            return false;
    }
    return true;
}

consteval std::size_t inventoryMaximumNameSize()
{
    std::size_t maximum = 0;
    for (const auto & family : families)
    {
        if (family.registered_name.size() > maximum)
            maximum = family.registered_name.size();
    }
    return maximum;
}

consteval FamilyIndex findExactInventoryIndex(std::string_view registered_name)
{
    for (std::size_t index = 0; index < families.size(); ++index)
    {
        if (families[index].registered_name == registered_name)
            return static_cast<FamilyIndex>(index);
    }
    return empty_family_index;
}

consteval bool aliasesReferenceCreators()
{
    for (const auto & family : families)
    {
        if (!family.alias)
            continue;

        const FamilyIndex canonical_index = findExactInventoryIndex(family.canonical_creator_name);
        if (canonical_index == empty_family_index || families[canonical_index].alias)
            return false;
    }
    return true;
}

constexpr std::uint64_t hashFamilyName(std::string_view value, bool fold_ascii_case) noexcept
{
    std::uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char byte : value)
    {
        hash ^= fold_ascii_case ? asciiLower(byte) : byte;
        hash *= 1099511628211ULL;
    }

    /// Avalanche the FNV-1a result before masking into the small power-of-two
    /// table. For this frozen inventory, both tables require at most six probes.
    hash ^= hash >> 30;
    hash *= 0xbf58476d1ce4e5b9ULL;
    hash ^= hash >> 27;
    hash *= 0x94d049bb133111ebULL;
    hash ^= hash >> 31;
    return hash;
}

template <std::size_t capacity>
struct TableBuildResult
{
    std::array<FamilyIndex, capacity> slots{};
    bool capacity_sufficient = true;
    bool metadata_compatible = true;
    std::size_t maximum_probe_count = 0;
};

template <std::size_t capacity>
struct CollisionTableBuildResult
{
    std::array<FamilyIndex, capacity> slots{};
    bool capacity_sufficient = true;
    std::size_t maximum_probe_count = 0;
};

constexpr const FamilyInfo & resolveFoldedCollision(const FamilyInfo & existing, const FamilyInfo & candidate) noexcept
{
    /// DataTypeFactory resolves folded aliases before folded creators.
    if (candidate.alias != existing.alias)
        return candidate.alias ? candidate : existing;
    return candidate.registered_name < existing.registered_name ? candidate : existing;
}

template <std::size_t capacity, bool fold_ascii_case>
consteval TableBuildResult<capacity> buildTable()
{
    static_assert(capacity != 0 && (capacity & (capacity - 1)) == 0, "classifier table capacity must be a power of two");

    TableBuildResult<capacity> result;
    result.slots.fill(empty_family_index);

    for (std::size_t index = 0; index < families.size(); ++index)
    {
        const auto & candidate = families[index];
        if constexpr (fold_ascii_case)
        {
            if (!candidate.case_insensitive)
                continue;
        }

        const std::size_t initial_slot = hashFamilyName(candidate.registered_name, fold_ascii_case) & (capacity - 1);
        bool inserted = false;
        for (std::size_t probe = 0; probe < capacity; ++probe)
        {
            const std::size_t slot = (initial_slot + probe) & (capacity - 1);
            const FamilyIndex existing_index = result.slots[slot];
            if (existing_index == empty_family_index)
            {
                result.slots[slot] = static_cast<FamilyIndex>(index);
                if (probe + 1 > result.maximum_probe_count)
                    result.maximum_probe_count = probe + 1;
                inserted = true;
                break;
            }

            const auto & existing = families[existing_index];
            const bool same_key = fold_ascii_case ? asciiCaseInsensitiveEqual(existing.registered_name, candidate.registered_name)
                                                  : existing.registered_name == candidate.registered_name;
            if (!same_key)
                continue;

            if constexpr (fold_ascii_case)
            {
                if (existing.canonical_creator_name != candidate.canonical_creator_name || existing.input_class != candidate.input_class)
                {
                    result.metadata_compatible = false;
                }
                else
                {
                    const auto & selected = resolveFoldedCollision(existing, candidate);
                    if (&selected == &candidate)
                        result.slots[slot] = static_cast<FamilyIndex>(index);
                }
            }
            else
            {
                result.metadata_compatible = false;
            }

            inserted = true;
            break;
        }

        if (!inserted)
            result.capacity_sufficient = false;
    }

    return result;
}

template <std::size_t capacity>
consteval CollisionTableBuildResult<capacity> buildCollisionTable()
{
    static_assert(capacity != 0 && (capacity & (capacity - 1)) == 0, "classifier table capacity must be a power of two");

    CollisionTableBuildResult<capacity> result;
    result.slots.fill(empty_family_index);

    for (std::size_t index = 0; index < families.size(); ++index)
    {
        const auto & candidate = families[index];
        const std::size_t initial_slot = hashFamilyName(candidate.registered_name, true) & (capacity - 1);
        bool inserted = false;
        for (std::size_t probe = 0; probe < capacity; ++probe)
        {
            const std::size_t slot = (initial_slot + probe) & (capacity - 1);
            const FamilyIndex existing_index = result.slots[slot];
            if (existing_index == empty_family_index)
            {
                result.slots[slot] = static_cast<FamilyIndex>(index);
                if (probe + 1 > result.maximum_probe_count)
                    result.maximum_probe_count = probe + 1;
                inserted = true;
                break;
            }

            if (asciiCaseInsensitiveEqual(families[existing_index].registered_name, candidate.registered_name))
            {
                /// Collision admission needs only set membership. Unlike the
                /// factory's folded table, equal folded keys may legitimately
                /// have different creator metadata.
                inserted = true;
                break;
            }
        }

        if (!inserted)
            result.capacity_sufficient = false;
    }

    return result;
}

constexpr std::size_t table_capacity = 256;
constexpr std::size_t maximum_table_probe_count = 6;
constexpr auto exact_table_build = buildTable<table_capacity, false>();
constexpr auto folded_table_build = buildTable<table_capacity, true>();
constexpr auto collision_table_build = buildCollisionTable<table_capacity>();
constexpr auto exact_table = exact_table_build.slots;
constexpr auto folded_table = folded_table_build.slots;
constexpr auto collision_table = collision_table_build.slots;

static_assert(families.size() == BuiltInDataTypeFamilyClassifier::registeredFamilyCount());
static_assert(families.size() < empty_family_index);
static_assert(inventoryIsStrictlySortedAndUnique(), "duplicate or unsorted built-in type family inventory");
static_assert(aliasesReferenceCreators(), "built-in type alias does not reference a frozen canonical creator");
static_assert(inventoryMaximumNameSize() == BuiltInDataTypeFamilyClassifier::maximumFamilyNameSize());
static_assert(exact_table_build.capacity_sufficient, "exact built-in type classifier table is too small");
static_assert(exact_table_build.metadata_compatible, "duplicate exact built-in type family");
static_assert(folded_table_build.capacity_sufficient, "ASCII-folded built-in type classifier table is too small");
static_assert(folded_table_build.metadata_compatible, "conflicting ASCII-folded built-in type family metadata");
static_assert(collision_table_build.capacity_sufficient, "ASCII-folded built-in collision table is too small");
static_assert(exact_table_build.maximum_probe_count <= maximum_table_probe_count);
static_assert(folded_table_build.maximum_probe_count <= maximum_table_probe_count);
static_assert(collision_table_build.maximum_probe_count <= maximum_table_probe_count);

constexpr FamilyIndex enum_family_index = findExactInventoryIndex("Enum");
constexpr FamilyIndex enum8_family_index = findExactInventoryIndex("Enum8");
constexpr FamilyIndex enum16_family_index = findExactInventoryIndex("Enum16");
constexpr FamilyIndex tuple_family_index = findExactInventoryIndex("Tuple");

static_assert(enum_family_index != empty_family_index && !families[enum_family_index].alias);
static_assert(enum8_family_index != empty_family_index && !families[enum8_family_index].alias);
static_assert(enum16_family_index != empty_family_index && !families[enum16_family_index].alias);
static_assert(tuple_family_index != empty_family_index && !families[tuple_family_index].alias);
static_assert(families[enum_family_index].input_class == CreatorInputClass::CanonicalizeGenericEnumArguments);
static_assert(families[enum8_family_index].input_class == CreatorInputClass::CanonicalizeGenericEnumArguments);
static_assert(families[enum16_family_index].input_class == CreatorInputClass::CanonicalizeGenericEnumArguments);

template <bool fold_ascii_case>
FamilyIndex lookupInTable(std::string_view family_name, const std::array<FamilyIndex, table_capacity> & table) noexcept
{
    const std::size_t initial_slot = hashFamilyName(family_name, fold_ascii_case) & (table_capacity - 1);
    /// Every inserted key is statically proven reachable within this bound.
    /// An absent, adversarial name therefore cannot force a full-table scan.
    for (std::size_t probe = 0; probe < maximum_table_probe_count; ++probe)
    {
        const FamilyIndex family_index = table[(initial_slot + probe) & (table_capacity - 1)];
        if (family_index == empty_family_index)
            return empty_family_index;

        const auto registered_name = families[family_index].registered_name;
        const bool matches = fold_ascii_case ? asciiCaseInsensitiveEqual(registered_name, family_name) : registered_name == family_name;
        if (matches)
            return family_index;
    }
    return empty_family_index;
}

Classification classifyRegisteredFamily(FamilyIndex family_index, BuiltInDataTypeFamilyMatch match) noexcept
{
    const auto & family = families[family_index];
    return {
        &family,
        match,
        BuiltInDataTypeAdmissionPath::RegisteredGeneric,
        family.input_class,
    };
}

Classification
classifySpecializedFamily(FamilyIndex family_index, BuiltInDataTypeFamilyMatch match, BuiltInDataTypeAdmissionPath admission) noexcept
{
    return {
        &families[family_index],
        match,
        admission,
        CreatorInputClass::SpecializedASTReadOnly,
    };
}

}

BuiltInDataTypeFamilyClassification BuiltInDataTypeFamilyClassifier::classifyGeneric(std::string_view family_name) noexcept
{
    if (family_name.empty() || family_name.size() > maximumFamilyNameSize())
        return {};

    if (const FamilyIndex exact = lookupInTable<false>(family_name, exact_table); exact != empty_family_index)
        return classifyRegisteredFamily(exact, BuiltInDataTypeFamilyMatch::Exact);

    if (const FamilyIndex folded = lookupInTable<true>(family_name, folded_table); folded != empty_family_index)
        return classifyRegisteredFamily(folded, BuiltInDataTypeFamilyMatch::AsciiCaseInsensitive);

    return {};
}

bool BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(std::string_view family_name) noexcept
{
    if (family_name.empty() || family_name.size() > maximumFamilyNameSize())
        return false;

    return lookupInTable<true>(family_name, collision_table) != empty_family_index;
}

BuiltInDataTypeFamilyClassification BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(std::string_view family_name) noexcept
{
    FamilyIndex family_index = enum8_family_index;
    BuiltInDataTypeFamilyMatch match = BuiltInDataTypeFamilyMatch::SpecializedASTKind;

    if (family_name.size() == 4 && asciiCaseInsensitiveEqual(family_name, "Enum"))
    {
        family_index = enum_family_index;
        match = family_name == families[family_index].registered_name ? BuiltInDataTypeFamilyMatch::Exact
                                                                      : BuiltInDataTypeFamilyMatch::AsciiCaseInsensitive;
    }
    else if (family_name.size() == 5 && asciiCaseInsensitiveEqual(family_name, "Enum8"))
    {
        family_index = enum8_family_index;
        match = family_name == families[family_index].registered_name ? BuiltInDataTypeFamilyMatch::Exact
                                                                      : BuiltInDataTypeFamilyMatch::AsciiCaseInsensitive;
    }
    else if (family_name.size() == 6 && asciiCaseInsensitiveEqual(family_name, "Enum16"))
    {
        family_index = enum16_family_index;
        match = family_name == families[family_index].registered_name ? BuiltInDataTypeFamilyMatch::Exact
                                                                      : BuiltInDataTypeFamilyMatch::AsciiCaseInsensitive;
    }

    return classifySpecializedFamily(family_index, match, BuiltInDataTypeAdmissionPath::SpecializedEnum);
}

BuiltInDataTypeFamilyClassification BuiltInDataTypeFamilyClassifier::classifySpecializedTuple(std::string_view family_name) noexcept
{
    const auto match = family_name == families[tuple_family_index].registered_name ? BuiltInDataTypeFamilyMatch::Exact
                                                                                   : BuiltInDataTypeFamilyMatch::SpecializedASTKind;
    return classifySpecializedFamily(tuple_family_index, match, BuiltInDataTypeAdmissionPath::SpecializedTuple);
}

BuiltInDataTypeFamilyClassification BuiltInDataTypeFamilyClassifier::classifyQualifiedReference() noexcept
{
    return {
        nullptr,
        BuiltInDataTypeFamilyMatch::QualifiedReference,
        BuiltInDataTypeAdmissionPath::QualifiedUserType,
        BuiltInDataTypeCreatorInputClass::ReadOnly,
    };
}

}
