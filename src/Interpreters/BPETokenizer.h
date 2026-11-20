#pragma once

#include <base/types.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>

#include <array>
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
  *
  * A vocabulary can also come from a Hugging Face `tokenizer.json` with a byte-level BPE model whose
  * pre-tokenizer is one of the shapes below. There a pair of tokens is merged by the position of the
  * pair in the list of merges rather than by the rank of the token it makes.
  */
enum class BPEPretokenizer : uint8_t
{
    /// `r50k_base`, `p50k_base`, and the GPT-2 vocabulary.
    R50k,
    /// `cl100k_base`.
    Cl100k,
    /// `o200k_base`.
    O200k,
    /// `cl100k` with every digit a piece of its own: Qwen 2 and 3.
    Qwen2,
    /// `o200k` with every digit a piece of its own and without the `'s`-style suffixes: Mistral Nemo.
    Tekken,
    /// Every digit split off first, then `r50k`: StarCoder 2, SmolLM 2.
    StarCoder2,
    /// Numbers of up to three digits and runs of CJK ideographs and kana split off first, then a
    /// pattern of its own: DeepSeek V3 and R1.
    DeepSeekV3,
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

    /// A Hugging Face `tokenizer.json`. Its special tokens are not matched in the text, for the same
    /// reason `.tiktoken` has none, but its other added tokens are.
    static std::shared_ptr<const BPEVocabulary> parseHuggingFace(std::string_view contents);

    /// Ids of `text`, appended to `result`. Text is encoded as text: a vocabulary has no special
    /// tokens, so a piece of the text that reads like one is encoded the way any other text is.
    void encode(std::string_view text, PaddedPODArray<UInt32> & result) const;

    /// The text of `ids`, appended to `out`. Throws if an id is not in the vocabulary.
    void decode(std::span<const UInt32> ids, WriteBuffer & out) const;


private:
    /// The rank of a piece, or `no_rank` when the piece is not a token of the vocabulary.
    static constexpr UInt32 no_rank = std::numeric_limits<UInt32>::max();

    [[nodiscard]] UInt32 rankOf(std::string_view piece) const;

    /// The rank of merging the adjacent parts `left` and `right`, or `no_rank` when they do not merge.
    [[nodiscard]] UInt32 pairRank(std::string_view left, std::string_view right) const;

    /// Encodes one piece that is not a token of the vocabulary by merging.
    void encodePiece(std::string_view piece, PaddedPODArray<UInt32> & result) const;

    /// `encode` for text without added tokens.
    void encodeText(std::string_view text, PaddedPODArray<UInt32> & result) const;

    BPEPretokenizer pretokenizer = BPEPretokenizer::Cl100k;

    /// Of a `tokenizer.json` with an NFC normalizer: text is brought to NFC before it is pre-tokenized.
    bool normalize_nfc = false;

    /// Of a `tokenizer.json`: the bytes that are not tokens, such as 0xC0, which valid UTF-8 never has.
    std::array<bool, 256> unknown_bytes{};
    bool has_unknown_bytes = false;

    /// A piece that is a token is taken whole, without merging. That holds for every `.tiktoken`
    /// vocabulary, and for a `tokenizer.json` that says so with `ignore_merges`.
    bool take_whole_tokens = true;

    /// Of a `tokenizer.json`: the rank of merging two tokens, keyed by their ids. Empty for a
    /// `.tiktoken` vocabulary, where the rank of a pair is the rank of the token it makes.
    HashMap<UInt64, UInt32> merge_ranks;

    /// Of a `tokenizer.json`: the added tokens that are not special, by their first byte, the longest
    /// first. They are found in the text before it is pre-tokenized.
    struct AddedToken
    {
        String content;
        UInt32 id;
        /// The whitespace after the token is taken with it.
        bool rstrip;
    };
    std::array<std::vector<AddedToken>, 256> added_tokens;
    bool has_added_tokens = false;

    /// The token bytes, referenced by both maps below.
    Arena arena;

    HashMapWithSavedHash<std::string_view, UInt32> ranks;

    /// Indexed by rank. An entry is empty for a rank the vocabulary skips.
    std::vector<std::string_view> by_rank;
};

using BPEVocabularyPtr = std::shared_ptr<const BPEVocabulary>;

}
