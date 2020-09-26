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

void obfuscateQueries(
    std::string_view src,
    WriteBuffer & result,
    WordMap & obfuscate_map,
    WordSet & used_nouns,
    SipHash hash_func,
    KnownIdentifierFunc known_identifier_func);

}
