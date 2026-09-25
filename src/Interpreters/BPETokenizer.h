#pragma once

#include <base/types.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>

#include <memory>
#include <span>
#include <string_view>
#include <vector>


namespace DB
{

class WriteBuffer;

/** Byte pair encoding, as used by the tokenizers of the OpenAI models (the `tiktoken` format).
  *
  * A vocabulary is a set of byte strings, each with a rank. Text is first cut into pieces by a
  * pre-tokenizer, and every piece is then encoded by repeatedly merging the adjacent pair of parts
  * whose concatenation has the lowest rank, until no pair is in the vocabulary. The ranks double as
  * the token ids, so a piece becomes a list of ids, and a list of ids becomes text again by
  * concatenating the byte strings the ranks stand for.
  *
  * The pre-tokenizer is what keeps a token from spanning a word boundary, and it differs between
  * vocabularies, so it is named by the vocabulary rather than derived from it. The three shapes
  * below cover the published OpenAI vocabularies; they are the regular expressions of `tiktoken`,
  * written out by hand because they need lookahead, which RE2 does not support.
  */
enum class BPEPretokenizer : uint8_t
{
    /// `r50k_base`, `p50k_base`, and the GPT-2 vocabulary.
    R50k,
    /// `cl100k_base`.
    Cl100k,
    /// `o200k_base`.
    O200k,
};

BPEPretokenizer parseBPEPretokenizer(std::string_view name);

/// The length in bytes of the piece the pre-tokenizer takes from `text` at `pos`, never zero:
/// every shape ends with an alternative that matches a single character.
size_t nextBPEPiece(BPEPretokenizer pretokenizer, std::string_view text, size_t pos);


/** A loaded vocabulary. Immutable after construction, so it is shared between queries.
  */
class BPEVocabulary
{
public:
    /// The `.tiktoken` format: one `base64(token) SP rank` line per token.
    static std::shared_ptr<const BPEVocabulary> parse(std::string_view contents, BPEPretokenizer pretokenizer);

    /// Ids of `text`, appended to `result`. Text is encoded as text: a vocabulary has no special
    /// tokens, so a piece of the text that reads like one is encoded the way any other text is.
    void encode(std::string_view text, PaddedPODArray<UInt32> & result) const;

    /// The text of `ids`, appended to `out`. Throws if an id is not in the vocabulary.
    void decode(std::span<const UInt32> ids, WriteBuffer & out) const;

    size_t size() const { return by_rank.size(); }
    BPEPretokenizer getPretokenizer() const { return pretokenizer; }

private:
    /// The rank of a piece, or `no_rank` when the piece is not a token of the vocabulary.
    static constexpr UInt32 no_rank = std::numeric_limits<UInt32>::max();

    /// Ranks are indices into `by_rank`, so a vocabulary with an absurd rank in it would ask for an
    /// absurd allocation. The largest published vocabulary has two hundred thousand tokens.
    static constexpr UInt32 max_rank = 100'000'000;
    UInt32 rankOf(std::string_view piece) const;

    /// Encodes one piece that is not a token of the vocabulary by merging.
    void encodePiece(std::string_view piece, PaddedPODArray<UInt32> & result) const;

    BPEPretokenizer pretokenizer = BPEPretokenizer::Cl100k;

    /// The token bytes, referenced by both maps below.
    Arena arena;

    HashMapWithSavedHash<std::string_view, UInt32> ranks;

    /// Indexed by rank. An entry is empty for a rank the vocabulary skips.
    std::vector<std::string_view> by_rank;
};

using BPEVocabularyPtr = std::shared_ptr<const BPEVocabulary>;

}
