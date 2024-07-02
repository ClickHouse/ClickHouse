#pragma once

#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTCreateQuery;

/// The UUID of a table or a database defined with a CREATE QUERY along with the UUIDs of its inner targets.
struct CreateQueryUUIDs
{
    CreateQueryUUIDs() = default;
    explicit CreateQueryUUIDs(const ASTCreateQuery & query);

    bool empty() const;
    explicit operator bool() const { return !empty(); }

    String toString() const;
    static CreateQueryUUIDs fromString(const String & str);

    void setTargetInnerUUID(ViewTarget::Kind kind, const UUID & new_inner_uuid);
    void setTargetInnerUUID(const UUID & new_inner_uuid) { setTargetInnerUUID(ViewTarget::Kind::Default, new_inner_uuid); }
    UUID getTargetInnerUUID(ViewTarget::Kind kind = ViewTarget::Kind::Default) const;

    /// UUID of the table.
    UUID uuid = UUIDHelpers::Nil;

    /// UUIDs of its target table (or tables).
    std::vector<std::pair<ViewTarget::Kind, UUID>> targets_inner_uuids;
};

}
