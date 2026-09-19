#pragma once

#include <base/types.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace DB
{

class ColumnString;

/// Extracts `key<delimiter>value` pairs separated by pair delimiters from a string that may
/// contain arbitrary noise between the pairs. Keys and values may be quoted. This is the
/// implementation of the `extractKeyValuePairs` function; the grammar is described in its
/// documentation. It is also used to parse Hive-style partitioning paths.
class KeyValuePairExtractor
{
public:
    /// What to do with a quoting character in the middle of an unquoted key or value.
    enum class UnexpectedQuotingCharacterStrategy
    {
        /// Discard the key or value being read.
        INVALID,
        /// Treat it as a regular character.
        ACCEPT,
        /// Discard what was read so far and continue as a quoted key or value.
        PROMOTE,
    };

    struct Configuration
    {
        char key_value_delimiter = ':';
        std::string pair_delimiters = " ,;";
        char quoting_character = '"';
        UnexpectedQuotingCharacterStrategy unexpected_quoting_character_strategy = UnexpectedQuotingCharacterStrategy::PROMOTE;
        /// Decode escape sequences; the backslash becomes a reserved character.
        bool with_escaping = false;
        /// 0 means unlimited.
        UInt64 max_number_of_pairs = 0;
    };

    /// Throws `BAD_ARGUMENTS` if the configuration is inconsistent.
    explicit KeyValuePairExtractor(const Configuration & configuration_);
    ~KeyValuePairExtractor();

    /// Appends the extracted keys and values to the columns. Returns the number of pairs.
    size_t extract(std::string_view data, ColumnString & keys, ColumnString & values) const;

    using PairCallback = std::function<void(std::string_view key, std::string_view value)>;

    /// Calls `on_pair` for every extracted pair. Without escaping the views point into `data`;
    /// with escaping they may point into a scratch buffer and are valid only during the call.
    size_t forEachPair(std::string_view data, const PairCallback & on_pair) const;

private:
    struct Impl;
    std::unique_ptr<const Impl> impl;
};

}
