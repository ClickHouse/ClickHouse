#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <tuple>


namespace DB::UDT
{

/// Frozen V1 wire registry. Values may only be extended through a new
/// explicitly versioned codec contract.
enum class SchemaObjectKind : UInt8
{
    Table = 1,
    View = 2,
    Dictionary = 3,
    TypeDefinition = 4,
    SyntheticTestObject = 254,
};

/// Frozen V1 wire registry. `dependent` is an edge's `from` identity and
/// `dependency` is its `to` identity.
enum class SchemaObjectDependencyEdgeKind : UInt8
{
    DefinitionDependsOnDefinition = 1,
    ObjectDependsOnDefinition = 2,
    ObjectDependsOnObject = 3,
};

[[nodiscard]] constexpr bool isKnownSchemaObjectKind(SchemaObjectKind kind) noexcept
{
    switch (kind)
    {
        case SchemaObjectKind::Table:
        case SchemaObjectKind::View:
        case SchemaObjectKind::Dictionary:
        case SchemaObjectKind::TypeDefinition:
        case SchemaObjectKind::SyntheticTestObject: return true;
    }
    return false;
}

[[nodiscard]] constexpr bool isKnownSchemaObjectDependencyEdgeKind(SchemaObjectDependencyEdgeKind kind) noexcept
{
    switch (kind)
    {
        case SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition:
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition:
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnObject: return true;
    }
    return false;
}

/// Stable identity shared by definitions and dependent schema objects.
/// Display names never participate in identity or ordering.
struct SchemaObjectID
{
    SchemaObjectKind kind{};
    UUID database_uuid = UUIDHelpers::Nil;
    UUID object_uuid = UUIDHelpers::Nil;

    [[nodiscard]] bool isValid() const noexcept
    {
        return isKnownSchemaObjectKind(kind) && database_uuid != UUIDHelpers::Nil && object_uuid != UUIDHelpers::Nil;
    }

    bool operator==(const SchemaObjectID &) const = default;

    friend bool operator<(const SchemaObjectID & lhs, const SchemaObjectID & rhs) noexcept
    {
        return std::tuple{
                   static_cast<UInt8>(lhs.kind),
                   UUIDHelpers::getHighBytes(lhs.database_uuid),
                   UUIDHelpers::getLowBytes(lhs.database_uuid),
                   UUIDHelpers::getHighBytes(lhs.object_uuid),
                   UUIDHelpers::getLowBytes(lhs.object_uuid)}
        < std::tuple{
            static_cast<UInt8>(rhs.kind),
            UUIDHelpers::getHighBytes(rhs.database_uuid),
            UUIDHelpers::getLowBytes(rhs.database_uuid),
            UUIDHelpers::getHighBytes(rhs.object_uuid),
            UUIDHelpers::getLowBytes(rhs.object_uuid)};
    }
};

/// `dependent` depends on `dependency`. Cycles and self-edges are represented
/// exactly; admission policy belongs to the database schema transaction.
struct SchemaObjectDependencyEdge
{
    SchemaObjectID dependent;
    SchemaObjectID dependency;
    SchemaObjectDependencyEdgeKind kind{};

    bool operator==(const SchemaObjectDependencyEdge &) const = default;

    /// Matches unsigned lexicographic ordering of the frozen V1 wire bytes:
    /// dependent identity, dependency identity, then edge-kind tag.
    friend bool operator<(const SchemaObjectDependencyEdge & lhs, const SchemaObjectDependencyEdge & rhs) noexcept
    {
        if (lhs.dependent != rhs.dependent)
            return lhs.dependent < rhs.dependent;
        if (lhs.dependency != rhs.dependency)
            return lhs.dependency < rhs.dependency;
        return static_cast<UInt8>(lhs.kind) < static_cast<UInt8>(rhs.kind);
    }
};

static_assert(static_cast<UInt8>(SchemaObjectKind::Table) == 1);
static_assert(static_cast<UInt8>(SchemaObjectKind::View) == 2);
static_assert(static_cast<UInt8>(SchemaObjectKind::Dictionary) == 3);
static_assert(static_cast<UInt8>(SchemaObjectKind::TypeDefinition) == 4);
static_assert(static_cast<UInt8>(SchemaObjectKind::SyntheticTestObject) == 254);
static_assert(static_cast<UInt8>(SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition) == 1);
static_assert(static_cast<UInt8>(SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition) == 2);
static_assert(static_cast<UInt8>(SchemaObjectDependencyEdgeKind::ObjectDependsOnObject) == 3);

}
