#pragma once
#include <Parsers/IAST_fwd.h>
#include <Storages/NamedCollections/NamedCollections_fwd.h>
#include <unordered_set>
#include <string_view>


namespace DB
{

NamedCollectionPtr tryGetNamedCollectionWithOverrides(ASTs asts);

void validateNamedCollection(
    const NamedCollection & collection,
    const std::unordered_set<std::string_view> & required_keys,
    const std::unordered_set<std::string_view> & optional_keys);

}
