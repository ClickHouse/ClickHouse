#include <Formats/makeColumnNamesUnique.h>

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <fmt/format.h>

namespace DB
{

void makeColumnNamesUnique(Names & names)
{
    UnorderedSetWithMemoryTracking<String> used(names.begin(), names.end());
    if (used.size() == names.size())
        return;

    /// The last suffix tried for each name, so every candidate is tried at most once
    /// (a header with many equal names would otherwise take quadratic time).
    UnorderedMapWithMemoryTracking<String, size_t> last_suffix;
    for (auto & name : names)
    {
        auto [it, inserted] = last_suffix.try_emplace(name, 0);
        if (inserted)
            continue;

        size_t & suffix = it->second;
        String candidate;
        do
        {
            candidate = fmt::format("{}_{}", name, ++suffix);
        } while (used.contains(candidate));

        used.insert(candidate);
        name = std::move(candidate);
    }
}

}
