#pragma once

/// Compile-time coverage proof for EnumWireTable: SET EQUALITY with the enum's declared values.
/// Size-plus-uniqueness is not enough (an invalid casted value satisfies both while an enumerator
/// goes missing). This header pulls in magic_enum and therefore MUST be included only from .cpp
/// files and tests, never from another header. The proof assumes every enumerator is in
/// magic_enum's reflectable range (by default -128..127), because `enum_values` sees only that range.

#include <magic_enum.hpp>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTable.h>

namespace DB::Cas
{

template <const auto & Table, typename Enum>
consteval bool casEnumTableCoversEnum()
{
    /// One assert per table carries all three obligations: a table author cannot forget density
    /// or word uniqueness, because coverage subsumes them.
    if (!Table.denseAndOrdered() || !Table.wordsUnique())
        return false;
    constexpr auto declared = magic_enum::enum_values<Enum>();
    if (declared.size() != Table.entries.size())
        return false;
    for (size_t i = 0; i < declared.size(); ++i)
    {
        bool found = false;
        for (const auto & entry : Table.entries)
            if (entry.value == declared[i])
                found = true;
        if (!found)
            return false;
    }
    return true;
}

}
