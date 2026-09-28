#pragma once

#include <Access/OPA/OpaConfiguration.h>
#include <Core/Names.h>
#include <base/types.h>

#include <cstdint>
#include <optional>
#include <vector>


namespace DB
{

/// Identity and query metadata sent with every request, mirroring `input.context`.
struct OpaRequestContext
{
    String user;
    /// The user's enabled role names. A policy sees them as groups, which is what lets one rule
    /// text match a Trino group and a ClickHouse role without an adapter.
    Names groups;
    String query_id;
};


/// One object a decision is asked about, mirroring `input.action.resource`.
struct OpaResource
{
    enum class Kind : uint8_t
    {
        Catalog,
        Schema,
        Table,
    };

    Kind kind = Kind::Table;
    OpaTableName name;
    /// Only meaningful for a table resource. Empty means the operation is not column scoped.
    Names columns;

    static OpaResource forCatalog(String catalog);
    static OpaResource forSchema(OpaTableName name);
    static OpaResource forTable(OpaTableName name, Names columns = {});

    /// Orders and compares resources so they can key a decision cache.
    auto toTuple() const { return std::tie(kind, name.catalog, name.schema, name.table, columns); }
    friend bool operator==(const OpaResource & left, const OpaResource & right) { return left.toTuple() == right.toTuple(); }
};


/// A whole request body, mirroring `{"input": {"context": ..., "action": ...}}`.
struct OpaRequest
{
    /// A ClickHouse grant keyword, such as `SELECT` or `CREATE TABLE`. Using the names ClickHouse
    /// already has avoids inventing a second vocabulary; a policy adapter maps them to whatever
    /// the shared rules expect.
    String operation;

    std::optional<OpaResource> resource;
    /// The object being created, for an operation that produces a new name, such as `RENAME TABLE`.
    std::optional<OpaResource> target_resource;
    /// The objects of a batched request. Mutually exclusive with `resource` in practice.
    std::vector<OpaResource> filter_resources;

    /// Renders the request body that goes on the wire.
    String serialize(const OpaRequestContext & request_context) const;
};

}
