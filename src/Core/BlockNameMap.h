#pragma once

#include <base/StringViewHash.h>
#include <sparsehash/dense_hash_map>

namespace DB
{

class Block;

using BlockNameMap = ::google::dense_hash_map<std::string_view, size_t, StringViewHash>;

BlockNameMap getNamesToIndexesMap(const Block & block);

}
