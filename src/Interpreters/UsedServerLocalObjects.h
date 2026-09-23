#pragma once

#include <base/types.h>

#include <mutex>
#include <optional>

namespace DB
{

/// The first object of this server that a query resolved by name while it was analyzed: a dictionary, the embedded
/// dictionaries or a `Join` table. A worker of `make_distributed_plan` shares the table data but not these objects, and
/// one of them is enough to run the query locally, so only the first is kept: the resolvers run at execution as well,
/// once per block for `dictGet`, and every call after the first costs a single load of the flag. Written by the
/// resolvers through `Context::addUsedServerLocalObject`, read by the fallback decision once analysis is done; the
/// writers of a query finish before its planning starts, so the read needs no synchronization of its own.
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

    /// Stores the first call's object; later calls are no-ops.
    void add(Kind kind, const String & name);

    /// The recorded object, or nothing.
    const std::optional<Entry> & get() const { return entry; }

    static std::string_view kindName(Kind kind);

private:
    std::once_flag once;
    std::optional<Entry> entry;
};

using UsedServerLocalObjectsPtr = std::shared_ptr<UsedServerLocalObjects>;

}
