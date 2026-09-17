#pragma once

#include <string_view>

namespace DB::UDT
{

/// Returns true when a SQL type spelling contains a syntactically plausible
/// qualified family name, or when an unknown token prevents proving that no
/// such name exists. The scan is allocation-free and visits the supplied bytes
/// exactly once up to trivia/quoted-token skips. It deliberately does not parse
/// or resolve the candidate and never treats input size alone as proof.
[[nodiscard]] bool hasQualifiedTypeReferenceCandidate(std::string_view type_spelling) noexcept;

}
