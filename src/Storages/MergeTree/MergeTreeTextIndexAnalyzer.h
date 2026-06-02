#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <unordered_map>

namespace DB
{

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1;
    size_t posting_list_block_size = 1024 * 1024;
    ASTPtr tokenizer;
    ASTPtr preprocessor;
    String posting_list_codec;
};

std::unordered_map<String, ASTPtr> textIndexConvertArgumentsToOptionsMap(const ASTPtr & arguments);

/// Parses a text index config string like "tokenizer = splitByNonAlpha, preprocessor = lower(column)"
/// into the same options map used by the text index.
std::unordered_map<String, ASTPtr> textIndexParseConfigString(const String & config);

/// Validates and extracts all config-level options from the options map.
/// Consumes recognized options from the map. Throws on unknown options.
MergeTreeIndexTextParams textIndexValidateOptions(std::unordered_map<String, ASTPtr> & options);

}
