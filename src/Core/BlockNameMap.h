#pragma once

#include <base/StringRef.h>

#include <sparsehash/dense_hash_map>

namespace DB
{

class Block;

using BlockNameMap = ::google::dense_hash_map<StringRef, size_t, StringRefHash>;

BlockNameMap getNamesToIndexesMap(const Block & block);

}
