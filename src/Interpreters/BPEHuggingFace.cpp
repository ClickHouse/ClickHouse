#include <Interpreters/BPETokenizer.h>

#include <Common/Exception.h>
#include <Common/UTF8Helpers.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <optional>
#include <ranges>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using JSONObject = Poco::JSON::Object::Ptr;

constexpr std::string_view r50k_pattern = R"('s|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+)";

/// The pre-tokenizers of `tokenizer.json` files that split text the way one of the hand-written ones
/// does, as the patterns their steps split by, in order. A `ByteLevel` step with `use_regex` splits by
/// the `r50k` pattern, and a `Digits` step with `individual_digits` by `\p{N}`.
struct KnownPretokenizer
{
    std::array<std::string_view, 3> patterns;
    BPEPretokenizer shape = BPEPretokenizer::R50k;
};

constexpr KnownPretokenizer known_pretokenizers[] = {
    {{r50k_pattern}, BPEPretokenizer::R50k},
    {{R"((?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+)"},
        BPEPretokenizer::Cl100k},
    {{R"([^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+(?i:'s|'t|'re|'ve|'m|'ll|'d)?|)"
      R"([^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*(?i:'s|'t|'re|'ve|'m|'ll|'d)?|)"
      R"(\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n/]*|\s*[\r\n]+|\s+(?!\S)|\s+)"},
        BPEPretokenizer::O200k},
    {{R"((?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+)"},
        BPEPretokenizer::Qwen2},
    {{R"([^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+|)"
      R"([^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*|)"
      R"(\p{N}| ?[^\s\p{L}\p{N}]+[\r\n/]*|\s*[\r\n]+|\s+(?!\S)|\s+)"},
        BPEPretokenizer::Tekken},
    {{R"(\p{N})", r50k_pattern}, BPEPretokenizer::StarCoder2},
    {{R"(\p{N}{1,3})",
      "[\u4e00-\u9fa5\u3040-\u309f\u30a0-\u30ff]+",
      R"([!"#$%&'()*+,\-./:;<=>?@\[\\\]^_`{|}~][A-Za-z]+|[^)" "\r\n" R"(\p{L}\p{P}\p{S}]?[\p{L}\p{M}]+| ?[\p{P}\p{S}]+[)" "\r\n"
      R"(]*|\s*[)" "\r\n" R"(]+|\s+(?!\S)|\s+)"},
        BPEPretokenizer::DeepSeekV3},
};

[[noreturn]] void unsupported(const String & what)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot load a `tokenizer.json`: {}", what);
}

String typeOf(const JSONObject & object)
{
    return object && object->has("type") ? object->getValue<String>("type") : String{};
}

bool flag(const JSONObject & object, const String & name, bool default_value)
{
    return object->has(name) && !object->isNull(name) ? object->getValue<bool>(name) : default_value;
}

/// The pre-tokenizer: `Split` and `Digits` steps followed by a `ByteLevel` step, which together are one
/// of the known pre-tokenizers.
BPEPretokenizer readPretokenizer(const JSONObject & root)
{
    std::vector<JSONObject> steps;
    const JSONObject pretokenizer = root->getObject("pre_tokenizer");
    if (typeOf(pretokenizer) == "Sequence")
    {
        const auto list = pretokenizer->getArray("pretokenizers");
        for (size_t i = 0; list && i < list->size(); ++i)
            steps.push_back(list->getObject(static_cast<unsigned>(i)));
    }
    else if (pretokenizer)
        steps.push_back(pretokenizer);

    if (steps.empty() || typeOf(steps.back()) != "ByteLevel" || flag(steps.back(), "add_prefix_space", false))
        unsupported("only byte-level BPE models are supported");

    std::vector<String> patterns;
    bool drops_unmatched = false;
    for (size_t i = 0; i + 1 < steps.size(); ++i)
    {
        const JSONObject & step = steps[i];
        const String type = typeOf(step);
        const JSONObject pattern = type == "Split" ? step->getObject("pattern") : nullptr;
        const String behavior = step->has("behavior") ? step->getValue<String>("behavior") : String{};
        const bool invert = flag(step, "invert", false);
        if (type == "Digits" && flag(step, "individual_digits", false))
            patterns.emplace_back(R"(\p{N})");
        else if (pattern && pattern->has("Regex") && ((behavior == "Isolated" && !invert) || (behavior == "Removed" && invert)))
        {
            patterns.push_back(pattern->getValue<String>("Regex"));
            drops_unmatched |= invert;
        }
        else
            unsupported(fmt::format("its pre-tokenizer has a `{}` step that is not supported", type));
    }
    if (flag(steps.back(), "use_regex", true))
        patterns.emplace_back(r50k_pattern);

    /// Dropping the text a pattern does not match changes nothing when the pattern matches every
    /// character, as each of the known patterns that is alone does.
    for (const auto & [known, shape] : known_pretokenizers)
        if (std::ranges::equal(patterns, known | std::views::filter([](std::string_view known_pattern) { return !known_pattern.empty(); }))
            && (!drops_unmatched || patterns.size() == 1))
            return shape;
    unsupported(fmt::format("its pre-tokenizer splits by '{}', which is not one of the supported pre-tokenizers", fmt::join(patterns, "', then by '")));
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

    /// The bytes a token stands for, or nothing when it is not written in the alphabet, as a special
    /// token such as `<｜begin▁of▁sentence｜>` is not.
    [[nodiscard]] std::optional<String> decode(const String & token) const
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
try
{
    auto vocabulary = std::make_shared<BPEVocabulary>();
    const JSONObject root = Poco::JSON::Parser().parse(String(contents)).extract<JSONObject>();

    const JSONObject model = root->getObject("model");
    if (!model)
        unsupported("it has no `model`");
    /// A model written before the type was recorded is a BPE model, and `tokenizers` reads it as one.
    if (const String type = typeOf(model); !type.empty() && type != "BPE")
        unsupported(fmt::format("its model is `{}`, only byte-level BPE models are supported", type));
    if (flag(model, "byte_fallback", false))
        unsupported("its model falls back to byte tokens, which is a SentencePiece model");
    for (const auto * key : {"continuing_subword_prefix", "end_of_word_suffix", "dropout"})
        if (model->has(key) && !model->isNull(key) && !model->get(key).toString().empty())
            unsupported(fmt::format("its model has a `{}`", key));
    vocabulary->take_whole_tokens = flag(model, "ignore_merges", false);

    /// An empty `Sequence` of normalizers is no normalizer. Added tokens are found before the text is
    /// normalized, which is what `tokenizers` does with those that are not `normalized`.
    if (const JSONObject normalizer = root->getObject("normalizer"))
    {
        const auto list = normalizer->getArray("normalizers");
        if (typeOf(normalizer) == "NFC")
            vocabulary->normalize_nfc = true;
        else if (typeOf(normalizer) != "Sequence" || (list && list->size() != 0))
            unsupported(fmt::format("its normalizer `{}` is not supported, only NFC is", typeOf(normalizer)));
    }

    vocabulary->pretokenizer = readPretokenizer(root);

    /// The ids of a `tokenizer.json` are not its merge ranks, but they are what the model is given,
    /// so they take the place of the ranks of a `.tiktoken` vocabulary.
    const ByteLevelAlphabet alphabet;
    const JSONObject vocab = model->getObject("vocab");
    const auto added_tokens = root->getArray("added_tokens");
    if (!vocab)
        unsupported("its model has no `vocab`");
    const size_t max_id = 2 * (vocab->size() + (added_tokens ? added_tokens->size() : 0)) + 256;

    const auto set_token = [&](Int64 id, std::string_view bytes)
    {
        if (id < 0 || static_cast<size_t>(id) > max_id || bytes.empty())
            unsupported(fmt::format("it has a token with the id {} and the content '{}'", id, bytes));
        if (static_cast<size_t>(id) >= vocabulary->by_rank.size())
            vocabulary->by_rank.resize(id + 1);
        auto & stored = vocabulary->by_rank[id];
        if (stored.empty())
            stored = std::string_view(vocabulary->arena.insert(bytes.data(), bytes.size()), bytes.size());
        return stored;
    };

    for (const auto & [token, id_value] : *vocab)
    {
        const Int64 id = id_value.convert<Int64>();
        const String bytes = alphabet.decode(token).value_or(token);
        if (id >= 0 && static_cast<size_t>(id) < vocabulary->by_rank.size() && !vocabulary->by_rank[id].empty())
            unsupported(fmt::format("it has the token id {} twice", id));

        decltype(vocabulary->ranks)::LookupResult inserted_id = nullptr;
        bool inserted = false;
        vocabulary->ranks.emplace(set_token(id, bytes), inserted_id, inserted);
        if (!inserted)
            unsupported(fmt::format("it has two tokens for the same bytes, the second with id {}", id));
        inserted_id->getMapped() = static_cast<UInt32>(id);
    }

    /// Merging starts from the individual bytes of a piece. Without an `unk_token` to stand for them,
    /// the bytes that are not tokens are left out.
    const bool has_unknown_token = model->has("unk_token") && !model->isNull("unk_token");
    for (size_t byte = 0; byte < 256; ++byte)
    {
        if (const char c = static_cast<char>(byte); vocabulary->rankOf({&c, 1}) != no_rank)
            continue;
        if (has_unknown_token)
            unsupported(fmt::format("it has no token for the single byte {}", byte));
        vocabulary->unknown_bytes[byte] = true;
        vocabulary->has_unknown_bytes = true;
    }

    /// Every added token can be decoded, and those that are not special are found in the text.
    /// An added token that is in `vocab` as well keeps the bytes it has there.
    for (size_t i = 0; added_tokens && i < added_tokens->size(); ++i)
    {
        const JSONObject token = added_tokens->getObject(static_cast<unsigned>(i));
        const Int64 id = token->getValue<Int64>("id");
        const String content = token->getValue<String>("content");
        set_token(id, content);
        if (flag(token, "special", false))
            continue;
        for (const auto * key : {"single_word", "lstrip"})
            if (flag(token, key, false))
                unsupported(fmt::format("its added token '{}' has `{}` set", content, key));
        vocabulary->added_tokens[static_cast<UInt8>(content.front())].push_back({content, static_cast<UInt32>(id), flag(token, "rstrip", false)});
        vocabulary->has_added_tokens = true;
    }
    /// The longest added token that matches is the one taken.
    for (auto & tokens : vocabulary->added_tokens)
        std::ranges::sort(tokens, std::greater{}, [](const auto & token) { return token.content.size(); });

    /// The merges, in the order of their rank: either `"a b"` or `["a", "b"]`.
    const auto merges = model->getArray("merges");
    if (!merges || merges->size() == 0)
        unsupported("its model has no `merges`");

    const auto id_of = [&](const String & token)
    {
        const UInt32 id = vocabulary->rankOf(alphabet.decode(token).value_or(token));
        if (id == no_rank)
            unsupported(fmt::format("a merge names '{}', which is not in its `vocab`", token));
        return id;
    };

    for (size_t rank = 0; rank < merges->size(); ++rank)
    {
        String left;
        String right;
        if (const auto & merge = merges->get(static_cast<unsigned>(rank)); merge.isString())
        {
            const String text = merge.extract<String>();
            const size_t space = text.find(' ');
            if (space == String::npos || text.find(' ', space + 1) != String::npos)
                unsupported(fmt::format("its merge '{}' is not two tokens", text));
            left = text.substr(0, space);
            right = text.substr(space + 1);
        }
        else
        {
            const auto pair = merges->getArray(static_cast<unsigned>(rank));
            if (!pair || pair->size() != 2)
                unsupported(fmt::format("its merge at position {} is not two tokens", rank));
            left = pair->getElement<String>(0);
            right = pair->getElement<String>(1);
        }

        id_of(left + right);
        /// As in `tokenizers`, a pair that is listed twice has the rank of the later one.
        vocabulary->merge_ranks[(UInt64{id_of(left)} << 32U) | id_of(right)] = static_cast<UInt32>(rank);
    }

    return vocabulary;
}
catch (const Exception &)
{
    throw;
}
catch (const Poco::Exception & e)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot load a `tokenizer.json`: {}", e.displayText());
}

}
