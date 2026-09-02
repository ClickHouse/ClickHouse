#pragma once

#include <string>
#include <unordered_set>
#include <unordered_map>
#include <string_view>
#include <functional>

#include <Common/SipHash.h>


namespace DB
{

class WriteBuffer;

using WordMap = std::unordered_map<std::string_view, std::string_view>;
using WordSet = std::unordered_set<std::string_view>;
using KnownIdentifierFunc = std::function<bool(std::string_view)>;

/** Takes one or multiple queries and obfuscates them by replacing identifiers to pseudorandom words
  * and replacing literals to random values, while preserving the structure of the queries and the general sense.
  *
  * Its intended use case is when the user wants to share their queries for testing and debugging
  * but is afraid to disclose the details about their column names, domain area and values of constants.
  *
  * It can obfuscate multiple queries in consistent fashion - identical names will be transformed to identical results.
  *
  * The function is not guaranteed to always give correct result or to be secure. It's implemented in "best effort" fashion.
  *
  * @param src - a string with source queries.
  * @param result - where the obfuscated queries will be written.
  * @param obfuscate_map - information about substituted identifiers
  *  (pass empty map at the beginning or reuse it from previous invocation to get consistent result)
  * @param used_nouns - information about words used for substitution
  *  (pass empty set at the beginning or reuse it from previous invocation to get consistent result)
  * @param hash_func - hash function that will be used as a pseudorandom source,
  *  it's recommended to preseed the function before passing here.
  * @param known_identifier_func - a function that returns true if identifier is known name
  *  (of function, aggregate function, etc. that should be kept as is). If it returns false, identifier will be obfuscated.
  */
void obfuscateQueries(
    std::string_view src,
    WriteBuffer & result,
    WordMap & obfuscate_map,
    WordSet & used_nouns,
    SipHash hash_func,
    KnownIdentifierFunc known_identifier_func);

}
