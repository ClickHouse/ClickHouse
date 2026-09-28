#pragma once

#include <Core/Names.h>
#include <base/types.h>

#include <optional>
#include <tuple>
#include <vector>


namespace DB
{

/// Identity and query metadata sent with every request, mirroring `input.context`.
struct OpaRequestContext
{
    String user;
    /// The user's enabled roles.
    Names roles;
    String query_id;
};


/** One object a decision is asked about, mirroring `input.action.resource`.
  *
  * The fields are ClickHouse's own: a database, a table inside it, and columns of that table.
  * A field left empty is omitted from the request rather than sent as a blank, so a policy can tell
  * "this check covers the whole database" from "this check is about a table", and "this check is not
  * about particular columns" from "these columns are involved".
  */
struct OpaResource
{
    String database;
    String table;
    Names columns;

    static OpaResource forDatabase(String database);
    static OpaResource forTable(String database, String table, Names columns = {});

    /// Orders and compares resources so they can key a decision cache.
    auto toTuple() const { return std::tie(database, table, columns); }
    friend bool operator==(const OpaResource & left, const OpaResource & right) { return left.toTuple() == right.toTuple(); }
};


/// A whole request body, mirroring `{"input": {"context": ..., "action": ...}}`.
struct OpaRequest
{
    /// The ClickHouse grant keywords the check requires, such as `SELECT` or `CREATE TABLE`.
    ///
    /// This is a list rather than a single name because one ClickHouse check can require several
    /// privileges at once, and all of them have to be allowed. Sending only the first would let a
    /// policy authorize a check it never saw in full.
    Names operations;

    std::optional<OpaResource> resource;
    /// The object being created, for an operation that produces a new name, such as `RENAME TABLE`.
    std::optional<OpaResource> target_resource;
    /// The objects of a batched request. Mutually exclusive with `resource` in practice.
    std::vector<OpaResource> filter_resources;

    /// Renders the request body that goes on the wire.
    String serialize(const OpaRequestContext & request_context) const;
};

}
