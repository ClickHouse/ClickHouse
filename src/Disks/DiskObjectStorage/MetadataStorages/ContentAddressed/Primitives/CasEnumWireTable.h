#pragma once

#include <array>
#include <string_view>

#include <base/types.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::Cas
{

/// One persisted enum <-> wire-word vocabulary: the single carrier the encoder, the decoder, the
/// introspection renderer, and the tests all read. Every persisted enum is dense, so `toWord` is a
/// direct indexed lookup; `fromWord` is a linear pass over a handful of words. Coverage is proven
/// at each table's definition site by `casEnumTableCoversEnum` (CasEnumWireTableAsserts.h — .cpp
/// and tests only) together with the `denseAndOrdered`/`wordsUnique` predicates below.
template <typename Enum, size_t N>
struct EnumWireTable
{
    struct Entry
    {
        Enum value;
        std::string_view word;
    };

    std::array<Entry, N> entries;

    /// An empty table would make `denseAndOrdered`/`wordsUnique` vacuously true and `toWord`'s
    /// index arithmetic read past the array — no wire vocabulary is empty, so reject at compile time.
    static_assert(N > 0, "EnumWireTable must hold at least one entry");

    constexpr bool denseAndOrdered() const
    {
        for (size_t i = 0; i < N; ++i)
            if (static_cast<uint64_t>(entries[i].value) != static_cast<uint64_t>(entries[0].value) + i)
                return false;
        return true;
    }

    constexpr bool wordsUnique() const
    {
        for (size_t i = 0; i < N; ++i)
            for (size_t j = i + 1; j < N; ++j)
                if (entries[i].word == entries[j].word)
                    return false;
        return true;
    }

    std::string_view toWord(Enum value, std::string_view what) const
    {
        const uint64_t index = static_cast<uint64_t>(value) - static_cast<uint64_t>(entries.front().value);
        if (index >= entries.size() || entries[index].value != value)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "{}: value {} is outside the wire vocabulary", what, static_cast<uint64_t>(value));
        return entries[index].word;
    }

    Enum fromWord(std::string_view word, std::string_view what) const
    {
        for (const auto & entry : entries)
            if (entry.word == word)
                return entry.value;
        throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: unknown word '{}'", what, word);
    }
};

}
