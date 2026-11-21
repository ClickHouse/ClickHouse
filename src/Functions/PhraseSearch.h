#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

namespace DB
{

/// Builds the KMP "failure" (partial-match) table over a phrase's token sequence.
///
/// Shared by `hasPhrase` and `hasAnyPhrases`/`hasAllPhrases` so that the per-phrase
/// matching semantics of all three functions are byte-for-byte identical. That identity
/// is what makes the `hasPhrase(c, p0) OR hasPhrase(c, p1)` -> `hasAnyPhrases(c, [p0, p1])`
/// (and the `AND` -> `hasAllPhrases`) analyzer rewrite a sound transformation.
///
/// For example, phrase "a a b" in input "a a a b" correctly matches at positions 1-3.
VectorWithMemoryTracking<size_t> buildPhraseFailureFunction(const VectorWithMemoryTracking<String> & phrase_tokens);

}
