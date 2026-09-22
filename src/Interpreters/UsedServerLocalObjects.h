#pragma once

#include <base/types.h>

#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// The objects of this server that a query resolved by name while it was analyzed: dictionaries, the embedded
/// dictionaries and `Join` tables today; the named collections of the AI functions and executable user defined
/// functions are the next candidates (kinds reserved, no writer yet). A worker of `make_distributed_plan` shares the
/// table data but not these objects, so a plan that needs one has to run locally.
/// The resolvers write here (unconditionally, unlike `QueryFactoriesInfo`, which only feeds `system.query_log`), the
/// fallback decision reads. One record per query: every sub-context of a query points at the same query context.
struct UsedServerLocalObjects
{
    enum class Kind : UInt8
    {
        Dictionary,
        JoinTable,
        EmbeddedDictionaries,
        NamedCollection, /// reserved: the AI functions read their credentials only at execution
        ExecutableUserDefinedFunction, /// reserved: a fragment cannot even deserialize one yet
    };

    struct Entry
    {
        Kind kind;
        String name;
    };

    void add(Kind kind, const String & name)
    {
        std::lock_guard lock(mutex);
        entries.push_back({kind, name});
    }

    /// The first object resolved, for the reason text; insertion order.
    std::optional<Entry> first() const
    {
        std::lock_guard lock(mutex);
        if (entries.empty())
            return std::nullopt;
        return entries.front();
    }

    size_t size() const
    {
        std::lock_guard lock(mutex);
        return entries.size();
    }

    /// The entry added at `position`, for a caller that wants to know what a step it just ran resolved.
    std::optional<Entry> at(size_t position) const
    {
        std::lock_guard lock(mutex);
        if (position >= entries.size())
            return std::nullopt;
        return entries[position];
    }

    static const char * kindName(Kind kind)
    {
        switch (kind)
        {
            case Kind::Dictionary: return "dictionary";
            case Kind::JoinTable: return "Join table";
            case Kind::EmbeddedDictionaries: return "the embedded dictionaries";
            case Kind::NamedCollection: return "named collection";
            case Kind::ExecutableUserDefinedFunction: return "executable user defined function";
        }
    }

private:
    std::vector<Entry> entries;
    mutable std::mutex mutex;
};

using UsedServerLocalObjectsPtr = std::shared_ptr<UsedServerLocalObjects>;

}
