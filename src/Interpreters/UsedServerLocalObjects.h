#pragma once

#include <base/types.h>

#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// The objects of this server that a query resolved by name while it was analyzed: dictionaries, the embedded
/// dictionaries and `Join` tables.  Every sub-context of a query points at the same query context, and
/// the resolvers may run on several threads at once (a scalar subquery executes during analysis), hence the mutex.
struct UsedServerLocalObjects
{
    enum class Kind : UInt8
    {
        Dictionary,
        JoinTable,
        EmbeddedDictionaries,
    };

    struct Entry
    {
        Kind kind;
        String name;
    };

    void add(Kind kind, const String & name);

    /// The first object resolved, for the reason text; insertion order.
    std::optional<Entry> first() const;

    size_t size() const;

    /// The entry added at `position`, for a caller that wants to know what a step it just ran resolved.
    std::optional<Entry> at(size_t position) const;

    static std::string_view kindName(Kind kind);

private:
    std::vector<Entry> entries;
    mutable std::mutex mutex;
};

using UsedServerLocalObjectsPtr = std::shared_ptr<UsedServerLocalObjects>;

}
