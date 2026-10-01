#pragma once

#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTCreateQuery;

/// The UUID of a table or a database defined with a CREATE QUERY along with the UUIDs of its inner targets.
struct CreateQueryUUIDs
{
    CreateQueryUUIDs() = default;

    /// Collect UUIDs from ASTCreateQuery.
    /// Parameters:
    /// `generate_random` - if it's true then unspecified in the query UUIDs will be generated randomly;
    /// `force_random` - if it's true then all UUIDs (even specified in the query) will be (re)generated randomly.
    explicit CreateQueryUUIDs(const ASTCreateQuery & query, bool generate_random = false, bool force_random = false);

    bool empty() const;
    explicit operator bool() const { return !empty(); }

    String toString() const;
    static CreateQueryUUIDs fromString(const String & str);

    void setTargetInnerUUID(ViewTarget::Kind kind, const UUID & new_inner_uuid);
    UUID getTargetInnerUUID(ViewTarget::Kind kind) const;

    /// Copies UUIDs to ASTCreateQuery.
    void copyToQuery(ASTCreateQuery & query) const;

    /// UUID of the table.
    UUID uuid = UUIDHelpers::Nil;

    /// UUIDs of its target table (or tables).
    std::vector<std::pair<ViewTarget::Kind, UUID>> targets_inner_uuids;
};

}
