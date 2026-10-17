#include <Interpreters/BPETokenizer.h>

#include <Common/Exception.h>
#include <Common/UTF8Helpers.h>
#include <Common/isValidUTF8.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <array>
#include <optional>

#include "config.h"

#if USE_ICU
#    include <unicode/bytestream.h>
#    include <unicode/normalizer2.h>

/// ICU wraps every entry point in a `U_ICU_ENTRY_POINT_RENAME(name)` macro that re-uses the original
/// name during expansion, so every ICU call below triggers `-Wdisabled-macro-expansion`.
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

using JSONObject = Poco::JSON::Object::Ptr;

/// The pre-tokenizer of GPT-2, which `ByteLevel` applies when its `use_regex` is set.
constexpr std::string_view gpt2_pattern = R"('s|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+)";

[[noreturn]] void unsupported(const String & what)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Cannot load a `tokenizer.json`: {}. Only byte-level BPE models are supported", what);
}

String typeOf(const JSONObject & object)
{
    return object->has("type") ? object->getValue<String>("type") : String{};
}

/// `ByteLevel` writes every byte of the text as a printable character, so that the tokens of a
/// `tokenizer.json` are strings: a byte that prints stands for itself, and the others are moved to
/// U+0100 and on, in the order of their values. This maps those characters back to their bytes.
struct ByteLevelAlphabet
{
    static constexpr size_t size = 0x144;
    std::array<Int16, size> byte_of{};

    ByteLevelAlphabet()
    {
        byte_of.fill(-1);
        UInt32 moved = 0x100;
        for (UInt32 byte = 0; byte < 256; ++byte)
        {
            const bool prints = (byte >= '!' && byte <= '~') || (byte >= 0xA1 && byte <= 0xAC) || (byte >= 0xAE && byte <= 0xFF);
            byte_of[prints ? byte : moved++] = static_cast<Int16>(byte);
        }
    }

    /// The bytes a token stands for, or nothing when it is not written in the alphabet: a special
    /// token such as `<｜begin▁of▁sentence｜>`, which merging never produces.
    std::optional<String> decode(const String & token) const
    {
        String bytes;
        bytes.reserve(token.size());
        for (size_t pos = 0; pos < token.size();)
        {
            const size_t length = UTF8::seqLength(static_cast<UInt8>(token[pos]));
            if (length > token.size() - pos)
                return {};
            const auto code_point = UTF8::convertUTF8ToCodePoint(token.data() + pos, length);
            if (!code_point || *code_point >= size || byte_of[*code_point] < 0)
                return {};
            bytes.push_back(static_cast<char>(byte_of[*code_point]));
            pos += length;
        }
        return bytes;
    }
};

}

std::shared_ptr<const BPEVocabulary> BPEVocabulary::parseHuggingFace(std::string_view contents)
{
    auto vocabulary = std::make_shared<BPEVocabulary>();
    vocabulary->pretokenizer = BPEPretokenizer::HuggingFace;

    JSONObject root;
    try
    {
        root = Poco::JSON::Parser().parse(String(contents)).extract<JSONObject>();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse a `tokenizer.json`: {}", e.displayText());
    }

    const JSONObject model = root->getObject("model");
    if (!model)
        unsupported("it has no `model`");

    /// A model written before the type was recorded is a BPE model, and `tokenizers` reads it as one.
    if (const String type = typeOf(model); !type.empty() && type != "BPE")
        unsupported(fmt::format("its model is `{}`", type));
    if (model->has("byte_fallback") && !model->isNull("byte_fallback") && model->getValue<bool>("byte_fallback"))
        unsupported("its model falls back to byte tokens, which is a SentencePiece model");
    for (const auto * affix : {"continuing_subword_prefix", "end_of_word_suffix"})
        if (model->has(affix) && !model->isNull(affix) && !model->getValue<String>(affix).empty())
            unsupported(fmt::format("its model has a `{}`", affix));
    if (model->has("dropout") && !model->isNull("dropout"))
        unsupported("its model has a `dropout`");

    vocabulary->take_whole_tokens
        = model->has("ignore_merges") && !model->isNull("ignore_merges") && model->getValue<bool>("ignore_merges");

    /// The normalizer: none, or NFC, possibly in a `Sequence`.
    std::function<void(const JSONObject &)> read_normalizer = [&](const JSONObject & normalizer)
    {
        const String type = typeOf(normalizer);
        if (type == "NFC")
            vocabulary->normalize_nfc = true;
        else if (type == "Sequence")
        {
            const auto steps = normalizer->getArray("normalizers");
            for (size_t i = 0; steps && i < steps->size(); ++i)
                read_normalizer(steps->getObject(static_cast<unsigned>(i)));
        }
        else
            unsupported(fmt::format("its normalizer is `{}`", type));
    };
    if (root->has("normalizer") && !root->isNull("normalizer"))
        read_normalizer(root->getObject("normalizer"));

    /// The pre-tokenizer: `Split` and `Digits` steps, and the `ByteLevel` step that makes the model
    /// a byte-level one.
    bool byte_level = false;
    std::function<void(const JSONObject &)> read_pretokenizer = [&](const JSONObject & step)
    {
        const String type = typeOf(step);
        if (type == "Sequence")
        {
            const auto steps = step->getArray("pretokenizers");
            for (size_t i = 0; steps && i < steps->size(); ++i)
                read_pretokenizer(steps->getObject(static_cast<unsigned>(i)));
        }
        else if (type == "ByteLevel")
        {
            byte_level = true;
            if (step->has("add_prefix_space") && step->getValue<bool>("add_prefix_space"))
                unsupported("its `ByteLevel` step adds a prefix space");
            if (!step->has("use_regex") || step->getValue<bool>("use_regex"))
                vocabulary->split_steps.push_back({std::make_shared<BPEPattern>(gpt2_pattern), true, true});
        }
        else if (type == "Digits")
        {
            const bool individual = step->has("individual_digits") && step->getValue<bool>("individual_digits");
            vocabulary->split_steps.push_back({std::make_shared<BPEPattern>(individual ? R"(\p{N})" : R"(\p{N}+)"), true, true});
        }
        else if (type == "Split")
        {
            const JSONObject pattern_object = step->getObject("pattern");
            String pattern;
            if (pattern_object && pattern_object->has("Regex"))
                pattern = pattern_object->getValue<String>("Regex");
            else if (pattern_object && pattern_object->has("String"))
            {
                /// A literal string, which the pattern language needs escaped where it is not a
                /// letter or a digit.
                for (const char c : pattern_object->getValue<String>("String"))
                {
                    if (!isAlphaNumericASCII(c) && static_cast<UInt8>(c) < 0x80)
                        pattern.push_back('\\');
                    pattern.push_back(c);
                }
            }
            else
                unsupported("it has a `Split` step without a pattern");

            const String behavior = step->getValue<String>("behavior");
            const bool invert = step->has("invert") && step->getValue<bool>("invert");
            SplitStep split{std::make_shared<BPEPattern>(pattern), true, true};
            if (behavior == "Removed")
            {
                split.keep_matches = invert;
                split.keep_gaps = !invert;
            }
            else if (behavior != "Isolated")
                unsupported(fmt::format("it has a `Split` step with the behavior `{}`", behavior));
            vocabulary->split_steps.push_back(std::move(split));
        }
        else
            unsupported(fmt::format("its pre-tokenizer has a `{}` step", type));
    };
    if (root->has("pre_tokenizer") && !root->isNull("pre_tokenizer"))
        read_pretokenizer(root->getObject("pre_tokenizer"));
    if (!byte_level)
        unsupported("its pre-tokenizer has no `ByteLevel` step, so the model is not a byte-level one");

    /// The vocabulary. The ids of a `tokenizer.json` are not its merge ranks, but they are what the
    /// model is given, so they take the place of the ranks of a `.tiktoken` vocabulary.
    const ByteLevelAlphabet alphabet;
    const JSONObject vocab = model->getObject("vocab");
    if (!vocab || vocab->size() == 0)
        unsupported("its model has no `vocab`");

    for (const auto & [token, id_value] : *vocab)
    {
        const Int64 id = id_value.convert<Int64>();
        if (id < 0 || id > max_rank)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A `tokenizer.json` has the token id {}, which is out of range", id);

        const String bytes = alphabet.decode(token).value_or(token);
        const std::string_view stored(vocabulary->arena.insert(bytes.data(), bytes.size()), bytes.size());

        decltype(vocabulary->ranks)::LookupResult inserted_id = nullptr;
        bool inserted = false;
        vocabulary->ranks.emplace(stored, inserted_id, inserted);
        if (!inserted)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A `tokenizer.json` has two tokens for the same bytes, the second with id {}", id);
        inserted_id->getMapped() = static_cast<UInt32>(id);

        if (static_cast<size_t>(id) >= vocabulary->by_rank.size())
            vocabulary->by_rank.resize(id + 1);
        if (!vocabulary->by_rank[id].empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A `tokenizer.json` has the token id {} twice", id);
        vocabulary->by_rank[id] = stored;
    }

    /// The added tokens. Every one of them can be decoded, and those that are not special are found in
    /// the text. An added token that is in `vocab` as well keeps the bytes it has there.
    if (root->has("added_tokens") && !root->isNull("added_tokens"))
    {
        const auto added_tokens = root->getArray("added_tokens");
        for (size_t i = 0; i < added_tokens->size(); ++i)
        {
            const JSONObject token = added_tokens->getObject(static_cast<unsigned>(i));
            const Int64 id = token->getValue<Int64>("id");
            String content = token->getValue<String>("content");
            if (id < 0 || id > max_rank || content.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A `tokenizer.json` has an added token with the id {} and the content '{}'", id, content);

            if (static_cast<size_t>(id) >= vocabulary->by_rank.size())
                vocabulary->by_rank.resize(id + 1);
            if (vocabulary->by_rank[id].empty())
                vocabulary->by_rank[id] = std::string_view(vocabulary->arena.insert(content.data(), content.size()), content.size());

            if (token->has("special") && token->getValue<bool>("special"))
                continue;
            for (const auto * flag : {"single_word", "lstrip", "rstrip"})
                if (token->has(flag) && token->getValue<bool>(flag))
                    unsupported(fmt::format("its added token '{}' has `{}` set", content, flag));
            const bool normalized = !token->has("normalized") || token->getValue<bool>("normalized");
            (normalized ? vocabulary->added_normalized : vocabulary->added_raw).add(std::move(content), static_cast<UInt32>(id));
        }
    }

    /// The merges, in the order of their rank: either `"a b"` or `["a", "b"]`.
    const auto merges = model->getArray("merges");
    if (!merges)
        unsupported("its model has no `merges`");

    const auto id_of = [&](const String & token) -> UInt32
    {
        const String bytes = alphabet.decode(token).value_or(token);
        const UInt32 id = vocabulary->rankOf(bytes);
        if (id == no_rank)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A merge of a `tokenizer.json` names '{}', which is not in its `vocab`", token);
        return id;
    };

    for (size_t rank = 0; rank < merges->size(); ++rank)
    {
        String left;
        String right;
        const auto & merge = merges->get(static_cast<unsigned>(rank));
        if (merge.isString())
        {
            const String text = merge.extract<String>();
            const size_t space = text.find(' ');
            if (space == String::npos || text.find(' ', space + 1) != String::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A merge of a `tokenizer.json` is '{}', which is not two tokens", text);
            left = text.substr(0, space);
            right = text.substr(space + 1);
        }
        else
        {
            const auto pair = merges->getArray(static_cast<unsigned>(rank));
            if (!pair || pair->size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A merge of a `tokenizer.json` at position {} is not two tokens", rank);
            left = pair->getElement<String>(0);
            right = pair->getElement<String>(1);
        }

        const UInt32 left_id = id_of(left);
        const UInt32 right_id = id_of(right);
        id_of(left + right);

        /// A pair merged twice keeps its first rank, which is the one that is ever used.
        decltype(vocabulary->merge_ranks)::LookupResult inserted_rank = nullptr;
        bool inserted = false;
        vocabulary->merge_ranks.emplace((UInt64{left_id} << 32U) | right_id, inserted_rank, inserted);
        if (inserted)
            inserted_rank->getMapped() = static_cast<UInt32>(rank);
    }

    return vocabulary;
}

void BPEVocabulary::AddedTokens::add(String content, UInt32 id)
{
    by_first_byte[static_cast<UInt8>(content.front())].push_back(static_cast<UInt32>(tokens.size()));
    tokens.emplace_back(std::move(content), id);
}

void BPEVocabulary::AddedTokens::split(
    std::string_view text, PaddedPODArray<UInt32> & result, const std::function<void(std::string_view)> & encode_gap) const
{
    if (tokens.empty())
    {
        encode_gap(text);
        return;
    }

    size_t gap_start = 0;
    size_t pos = 0;
    while (pos < text.size())
    {
        /// The longest added token that starts here, if any.
        const std::pair<String, UInt32> * longest = nullptr;
        for (const UInt32 index : by_first_byte[static_cast<UInt8>(text[pos])])
        {
            const auto & token = tokens[index];
            if (text.substr(pos).starts_with(token.first) && (!longest || token.first.size() > longest->first.size()))
                longest = &token;
        }

        if (!longest)
        {
            ++pos;
            continue;
        }

        if (gap_start < pos)
            encode_gap(text.substr(gap_start, pos - gap_start));
        result.push_back(longest->second);
        pos += longest->first.size();
        gap_start = pos;
    }

    if (gap_start < text.size())
        encode_gap(text.substr(gap_start));
}

void BPEVocabulary::encodeHuggingFace(std::string_view text, PaddedPODArray<UInt32> & result) const
{
    /// As `tokenizers` does it: the added tokens that are matched in the text as it is come first,
    /// then every piece between them is normalized on its own, then the added tokens that are matched
    /// in the normalized text, and what is left is pre-tokenized and merged.
    std::vector<std::string_view> pieces;
    added_raw.split(text, result, [&](std::string_view raw)
    {
        String normalized;
        if (normalize_nfc && normalizeNFC(raw, normalized))
            raw = normalized;

        added_normalized.split(raw, result, [&](std::string_view gap)
        {
            splitPieces(gap, pieces);
            for (const std::string_view piece : pieces)
                encodePieceOrToken(piece, result);
        });
    });
}

void BPEVocabulary::splitPieces(std::string_view text, std::vector<std::string_view> & pieces) const
{
    pieces.clear();
    if (!text.empty())
        pieces.push_back(text);

    std::vector<std::string_view> cut;
    for (const auto & step : split_steps)
    {
        cut.clear();
        for (const std::string_view piece : pieces)
        {
            const auto keep = [&](size_t begin, size_t end, bool is_match)
            {
                if (begin < end && (is_match ? step.keep_matches : step.keep_gaps))
                    cut.push_back(piece.substr(begin, end - begin));
            };

            size_t gap_start = 0;
            size_t pos = 0;
            size_t match_begin = 0;
            size_t match_end = 0;
            while (step.pattern->find(piece, pos, match_begin, match_end))
            {
                keep(gap_start, match_begin, false);
                keep(match_begin, match_end, true);
                gap_start = match_end;
                if (match_end > match_begin)
                    pos = match_end;
                else if (match_end == piece.size())
                    break;
                else
                    /// An empty match cuts the text too, and the search goes on from the next character.
                    pos = match_end + std::max<size_t>(1, UTF8::seqLength(static_cast<UInt8>(piece[match_end])));
                if (pos > piece.size())
                    break;
            }
            keep(gap_start, piece.size(), false);
        }
        pieces.swap(cut);
    }
}

#if USE_ICU

bool BPEVocabulary::normalizeNFC(std::string_view text, String & normalized)
{
    if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(text.data()), text.size()))
        return false;

    UErrorCode status = U_ZERO_ERROR;
    const icu::Normalizer2 * nfc = icu::Normalizer2::getNFCInstance(status);
    if (U_FAILURE(status))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot get the NFC normalizer: {}", u_errorName(status));

    const icu::StringPiece input(text.data(), static_cast<int32_t>(text.size()));
    if (nfc->isNormalizedUTF8(input, status) && U_SUCCESS(status))
        return false;

    status = U_ZERO_ERROR;
    normalized.clear();
    icu::StringByteSink<String> sink(&normalized);
    nfc->normalizeUTF8(0, input, sink, nullptr, status);
    if (U_FAILURE(status))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot normalize the text to NFC: {}", u_errorName(status));
    return true;
}

#else

bool BPEVocabulary::normalizeNFC(std::string_view, String &)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "NFC normalization requires ClickHouse to be built with ICU");
}

#endif

}

#if USE_ICU
#    pragma clang diagnostic pop
#endif
