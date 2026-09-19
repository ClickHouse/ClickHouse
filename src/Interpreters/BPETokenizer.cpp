#include <Interpreters/BPETokenizer.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/UTF8Helpers.h>
#include <IO/WriteBuffer.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <queue>

#include "config.h"

#if USE_ICU
#    include <unicode/uchar.h>

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

BPEPretokenizer parseBPEPretokenizer(std::string_view name)
{
    if (name == "r50k")
        return BPEPretokenizer::R50k;
    if (name == "cl100k")
        return BPEPretokenizer::Cl100k;
    if (name == "o200k")
        return BPEPretokenizer::O200k;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown BPE pre-tokenizer '{}', expected one of 'r50k', 'cl100k', 'o200k'", name);
}


#if USE_ICU

namespace
{

/// Stands for a byte that is not the start of a valid UTF-8 sequence. It belongs to none of the
/// classes the pre-tokenizers test for except `[^\s\p{L}\p{N}]`, so it groups with punctuation.
/// A ClickHouse string is a byte string, and this keeps the tokenizer total over all of them,
/// while valid UTF-8 is cut exactly the way `tiktoken` cuts it.
constexpr UInt32 invalid_code_point = 0xFFFFFFFFu;

UInt32 codePointAt(std::string_view text, size_t pos, size_t & length)
{
    if (const auto byte = static_cast<UInt8>(text[pos]); byte < 0x80)
    {
        length = 1;
        return byte;
    }

    const size_t sequence_length = UTF8::seqLength(static_cast<UInt8>(text[pos]));
    if (sequence_length <= text.size() - pos)
    {
        if (auto code_point = UTF8::convertUTF8ToCodePoint(text.data() + pos, sequence_length))
        {
            length = sequence_length;
            return *code_point;
        }
    }
    length = 1;
    return invalid_code_point;
}

/// The classes the pre-tokenizers test. They are asked of every character of the text, several
/// at a time, so for the Basic Multilingual Plane they are taken from ICU once and kept in a table;
/// a class read from the table is the class ICU gives.
namespace CharacterClass
{
    constexpr UInt32 letter = 1;        /// `\p{L}`
    constexpr UInt32 number = 2;        /// `\p{N}`
    constexpr UInt32 whitespace = 4;    /// `\s`, the Unicode `White_Space` property: NBSP is in it, a zero width space is not
    constexpr UInt32 uppercaseish = 8;  /// `[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]` of the `o200k` shape
    constexpr UInt32 lowercaseish = 16; /// `[\p{Ll}\p{Lm}\p{Lo}\p{M}]` of the `o200k` shape
}

UInt32 classesFromICU(UInt32 code_point)
{
    const auto c = static_cast<UChar32>(code_point);
    const uint32_t mask = U_GET_GC_MASK(c);
    UInt32 classes = 0;
    if (mask & U_GC_L_MASK)
        classes |= CharacterClass::letter;
    if (mask & U_GC_N_MASK)
        classes |= CharacterClass::number;
    if (u_hasBinaryProperty(c, UCHAR_WHITE_SPACE))
        classes |= CharacterClass::whitespace;
    if (mask & (U_GC_LU_MASK | U_GC_LT_MASK | U_GC_LM_MASK | U_GC_LO_MASK | U_GC_M_MASK))
        classes |= CharacterClass::uppercaseish;
    if (mask & (U_GC_LL_MASK | U_GC_LM_MASK | U_GC_LO_MASK | U_GC_M_MASK))
        classes |= CharacterClass::lowercaseish;
    return classes;
}

UInt32 classesOf(UInt32 code_point)
{
    static constexpr UInt32 table_size = 0x10000;
    static const auto table = []
    {
        std::array<UInt8, table_size> result{};
        for (UInt32 c = 0; c < table_size; ++c)
            result[c] = static_cast<UInt8>(classesFromICU(c));
        return result;
    }();

    if (code_point < table_size)
        return table[code_point];
    if (code_point == invalid_code_point)
        return 0;
    return classesFromICU(code_point);
}

bool isLetter(UInt32 code_point) { return classesOf(code_point) & CharacterClass::letter; }
bool isNumber(UInt32 code_point) { return classesOf(code_point) & CharacterClass::number; }
bool isWhitespace(UInt32 code_point) { return classesOf(code_point) & CharacterClass::whitespace; }

/// The two overlap in `\p{Lm}`, `\p{Lo}` and `\p{M}`, which is what makes the `o200k` shape need
/// backtracking.
bool isUppercaseish(UInt32 code_point) { return classesOf(code_point) & CharacterClass::uppercaseish; }
bool isLowercaseish(UInt32 code_point) { return classesOf(code_point) & CharacterClass::lowercaseish; }

/// `[^\s\p{L}\p{N}]`.
bool isPunctuationish(UInt32 code_point)
{
    return !(classesOf(code_point) & (CharacterClass::whitespace | CharacterClass::letter | CharacterClass::number));
}

/// `[^\r\n\p{L}\p{N}]`, the optional first character of a word in the `cl100k` and `o200k` shapes.
bool isWordPrefix(UInt32 code_point)
{
    return code_point != '\r' && code_point != '\n' && !(classesOf(code_point) & (CharacterClass::letter | CharacterClass::number));
}

bool isNewline(UInt32 code_point) { return code_point == '\r' || code_point == '\n'; }

template <typename Predicate>
size_t scanWhile(std::string_view text, size_t pos, Predicate && predicate)
{
    while (pos < text.size())
    {
        size_t length = 0;
        if (!predicate(codePointAt(text, pos, length)))
            break;
        pos += length;
    }
    return pos;
}

/// The length of a `'s`/`'ll`-style suffix at `pos`, or zero. Every shape accepts the same set;
/// `cl100k` and `o200k` wrap it in `(?i:)`, the `r50k` one is lower case only.
size_t contractionLength(std::string_view text, size_t pos, bool case_insensitive)
{
    if (pos >= text.size() || text[pos] != '\'')
        return 0;

    const auto at = [&](size_t offset) -> char
    {
        if (pos + offset >= text.size())
            return '\0';
        const char c = text[pos + offset];
        return case_insensitive && c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
    };

    switch (at(1))
    {
        case 's': case 'd': case 'm': case 't':
            return 2;
        case 'l':
            return at(2) == 'l' ? 3 : 0;
        case 'v':
            return at(2) == 'e' ? 3 : 0;
        case 'r':
            return at(2) == 'e' ? 3 : 0;
        default:
            return 0;
    }
}

/// A run of whitespace, which every shape ends by taking apart in its own way.
struct WhitespaceRun
{
    /// Past the last byte of the run.
    size_t end;
    /// The first byte of its last character, so `\s+(?!\S)` can leave that one character behind.
    size_t last_start;
    /// Past the last CR or LF of the run, or zero when it holds none, for `\s*[\r\n]`.
    size_t newline_end;
};

WhitespaceRun scanWhitespace(std::string_view text, size_t pos)
{
    WhitespaceRun run{pos, pos, 0};
    while (run.end < text.size())
    {
        size_t length = 0;
        const UInt32 code_point = codePointAt(text, run.end, length);
        if (!isWhitespace(code_point))
            break;
        run.last_start = run.end;
        run.end += length;
        if (isNewline(code_point))
            run.newline_end = run.end;
    }
    return run;
}

/// `'(?:[sdmt]|ll|ve|re)| ?\p{L}++| ?\p{N}++| ?[^\s\p{L}\p{N}]++|\s++$|\s+(?!\S)|\s`
size_t nextPieceR50k(std::string_view text, size_t pos)
{
    if (size_t length = contractionLength(text, pos, /*case_insensitive=*/false))
        return length;

    /// The three runs share an optional leading space. Without it the character at `pos` decides
    /// which run applies, and with it the character after the space does; when that character is
    /// whitespace none of them applies, because a space is not in any of the three classes.
    const size_t start = text[pos] == ' ' ? pos + 1 : pos;
    if (start < text.size())
    {
        size_t length = 0;
        const UInt32 code_point = codePointAt(text, start, length);
        if (isLetter(code_point))
            return scanWhile(text, start, isLetter) - pos;
        if (isNumber(code_point))
            return scanWhile(text, start, isNumber) - pos;
        if (isPunctuationish(code_point))
            return scanWhile(text, start, isPunctuationish) - pos;
    }

    const WhitespaceRun run = scanWhitespace(text, pos);
    if (run.end == text.size())
        return run.end - pos; /// `\s++$`
    if (run.last_start > pos)
        return run.last_start - pos; /// `\s+(?!\S)`

    size_t length = 0;
    codePointAt(text, pos, length);
    return length; /// `\s`
}

/// `'(?i:[sdmt]|ll|ve|re)|[^\r\n\p{L}\p{N}]?+\p{L}++|\p{N}{1,3}+| ?[^\s\p{L}\p{N}]++[\r\n]*+|\s++$|\s*[\r\n]|\s+(?!\S)|\s`
size_t nextPieceCl100k(std::string_view text, size_t pos)
{
    if (size_t length = contractionLength(text, pos, /*case_insensitive=*/true))
        return length;

    size_t first_length = 0;
    const UInt32 first = codePointAt(text, pos, first_length);

    /// `[^\r\n\p{L}\p{N}]?+\p{L}++`. The optional character is possessive: once it is taken and no
    /// letter follows, this alternative is over rather than retried without it.
    {
        const size_t start = isWordPrefix(first) ? pos + first_length : pos;
        if (start < text.size())
        {
            size_t length = 0;
            if (isLetter(codePointAt(text, start, length)))
                return scanWhile(text, start, isLetter) - pos;
        }
    }

    /// `\p{N}{1,3}+`
    if (isNumber(first))
    {
        size_t end = pos;
        for (size_t digit = 0; digit < 3 && end < text.size(); ++digit)
        {
            size_t length = 0;
            if (!isNumber(codePointAt(text, end, length)))
                break;
            end += length;
        }
        return end - pos;
    }

    /// ` ?[^\s\p{L}\p{N}]++[\r\n]*+`
    {
        const size_t start = first == ' ' ? pos + 1 : pos;
        if (start < text.size())
        {
            size_t length = 0;
            if (isPunctuationish(codePointAt(text, start, length)))
            {
                const size_t end = scanWhile(text, start, isPunctuationish);
                return scanWhile(text, end, isNewline) - pos;
            }
        }
    }

    const WhitespaceRun run = scanWhitespace(text, pos);
    if (run.end == text.size())
        return run.end - pos; /// `\s++$`
    if (run.newline_end != 0)
        return run.newline_end - pos; /// `\s*[\r\n]`
    if (run.last_start > pos)
        return run.last_start - pos; /// `\s+(?!\S)`
    return first_length; /// `\s`
}

/// One of the two word alternatives of the `o200k` shape:
///   `[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+(?i:'s|…)?`
///   `[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*(?i:'s|…)?`
/// Neither quantifier is possessive, so both the optional first character and the upper case run
/// are given back when what follows them does not match; the first match found that way wins.
size_t matchO200kWord(std::string_view text, size_t pos)
{
    size_t prefix_length = 0;
    {
        size_t length = 0;
        if (isWordPrefix(codePointAt(text, pos, length)))
            prefix_length = length;
    }

    for (int lower_case_required = 1; lower_case_required >= 0; --lower_case_required)
    {
        for (int take_prefix = 1; take_prefix >= 0; --take_prefix)
        {
            if (take_prefix && prefix_length == 0)
                continue;

            const size_t start = pos + (take_prefix ? prefix_length : 0);
            const size_t upper_end = scanWhile(text, start, isUppercaseish);

            if (lower_case_required)
            {
                /// The upper case run is given back one character at a time until the character it
                /// gives back starts a lower case run, which the first alternative needs.
                for (size_t candidate = upper_end; candidate >= start;)
                {
                    size_t length = 0;
                    if (candidate < text.size() && isLowercaseish(codePointAt(text, candidate, length)))
                    {
                        const size_t end = scanWhile(text, candidate, isLowercaseish);
                        return end + contractionLength(text, end, /*case_insensitive=*/true) - pos;
                    }

                    if (candidate == start)
                        break;
                    /// Back to the start of the character before `candidate`.
                    do
                        --candidate;
                    while (candidate > start && UTF8::isContinuationOctet(static_cast<UInt8>(text[candidate])));
                }
            }
            else if (upper_end > start)
            {
                /// The lower case run is optional here, and so is the suffix, so the longest upper
                /// case run always leads to a match.
                const size_t end = scanWhile(text, upper_end, isLowercaseish);
                return end + contractionLength(text, end, /*case_insensitive=*/true) - pos;
            }
        }
    }

    return 0;
}

/// The seven alternatives listed in `tiktoken`, joined by `|`.
size_t nextPieceO200k(std::string_view text, size_t pos)
{
    if (size_t length = matchO200kWord(text, pos))
        return length;

    size_t first_length = 0;
    const UInt32 first = codePointAt(text, pos, first_length);

    /// `\p{N}{1,3}`
    if (isNumber(first))
    {
        size_t end = pos;
        for (size_t digit = 0; digit < 3 && end < text.size(); ++digit)
        {
            size_t length = 0;
            if (!isNumber(codePointAt(text, end, length)))
                break;
            end += length;
        }
        return end - pos;
    }

    /// ` ?[^\s\p{L}\p{N}]+[\r\n/]*`
    {
        const size_t start = first == ' ' ? pos + 1 : pos;
        if (start < text.size())
        {
            size_t length = 0;
            if (isPunctuationish(codePointAt(text, start, length)))
            {
                const size_t end = scanWhile(text, start, isPunctuationish);
                const auto is_newline_or_slash = [](UInt32 code_point) { return isNewline(code_point) || code_point == '/'; };
                return scanWhile(text, end, is_newline_or_slash) - pos;
            }
        }
    }

    const WhitespaceRun run = scanWhitespace(text, pos);
    if (run.newline_end != 0)
        return run.newline_end - pos; /// `\s*[\r\n]+`
    if (run.end == text.size())
        return run.end - pos; /// `\s+(?!\S)` at the end of the text takes the whole run
    if (run.last_start > pos)
        return run.last_start - pos; /// `\s+(?!\S)` leaves the last character to the next piece
    return run.end - pos; /// `\s+`
}

}

size_t nextBPEPiece(BPEPretokenizer pretokenizer, std::string_view text, size_t pos)
{
    chassert(pos < text.size());

    switch (pretokenizer)
    {
        case BPEPretokenizer::R50k: return nextPieceR50k(text, pos);
        case BPEPretokenizer::Cl100k: return nextPieceCl100k(text, pos);
        case BPEPretokenizer::O200k: return nextPieceO200k(text, pos);
    }
}

#else

size_t nextBPEPiece(BPEPretokenizer, std::string_view, size_t)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "BPE tokenization requires ClickHouse to be built with ICU");
}

#endif


std::shared_ptr<const BPEVocabulary> BPEVocabulary::parse(std::string_view contents, BPEPretokenizer pretokenizer)
{
    auto vocabulary = std::make_shared<BPEVocabulary>();
    vocabulary->pretokenizer = pretokenizer;

    size_t line_number = 0;
    std::vector<bool> single_bytes(256, false);

    for (size_t pos = 0; pos < contents.size();)
    {
        const size_t line_end = std::min(contents.find('\n', pos), contents.size());
        std::string_view line = contents.substr(pos, line_end - pos);
        pos = line_end + 1;
        ++line_number;

        /// Surrounding whitespace is trimmed so that a vocabulary can be written out indented in a
        /// configuration file, where every line carries the indentation of the element it is in.
        while (!line.empty() && (line.front() == ' ' || line.front() == '\t'))
            line.remove_prefix(1);
        while (!line.empty() && (line.back() == ' ' || line.back() == '\t' || line.back() == '\r'))
            line.remove_suffix(1);
        if (line.empty())
            continue;

        const size_t separator = line.rfind(' ');
        if (separator == std::string_view::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Line {} of a BPE vocabulary has no space between the token and its rank", line_number);

        UInt32 rank = 0;
        const std::string_view rank_field = line.substr(separator + 1);
        const auto parsed = std::from_chars(rank_field.data(), rank_field.data() + rank_field.size(), rank);
        if (parsed.ec != std::errc{} || parsed.ptr != rank_field.data() + rank_field.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Line {} of a BPE vocabulary has '{}' where its rank is expected", line_number, rank_field);

        std::string token;
        try
        {
            token = base64Decode(std::string(line.substr(0, separator)));
        }
        catch (...)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Line {} of a BPE vocabulary has a token that is not base64 encoded", line_number);
        }

        if (token.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Line {} of a BPE vocabulary has an empty token", line_number);

        if (rank > max_rank)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Line {} of a BPE vocabulary has the rank {}, which is above the limit of {}", line_number, rank, max_rank);

        const std::string_view stored(vocabulary->arena.insert(token.data(), token.size()), token.size());

        decltype(vocabulary->ranks)::LookupResult inserted_rank = nullptr;
        bool inserted = false;
        vocabulary->ranks.emplace(stored, inserted_rank, inserted);
        if (!inserted)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "A BPE vocabulary has the same token twice, at line {}", line_number);
        inserted_rank->getMapped() = rank;

        if (rank >= vocabulary->by_rank.size())
            vocabulary->by_rank.resize(rank + 1);
        if (!vocabulary->by_rank[rank].empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "A BPE vocabulary has rank {} twice, at line {}", rank, line_number);
        vocabulary->by_rank[rank] = stored;

        if (token.size() == 1)
            single_bytes[static_cast<UInt8>(token[0])] = true;
    }

    if (vocabulary->ranks.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A BPE vocabulary is empty");

    /// Encoding starts from the individual bytes of a piece, so every byte has to be a token of its
    /// own, and text that is not valid UTF-8 has to be encodable too.
    for (size_t byte = 0; byte < single_bytes.size(); ++byte)
        if (!single_bytes[byte])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "A BPE vocabulary has no token for the single byte {}, so it cannot encode arbitrary text", byte);

    return vocabulary;
}

UInt32 BPEVocabulary::rankOf(std::string_view piece) const
{
    const auto * it = ranks.find(piece);
    return it == nullptr ? no_rank : it->getMapped();
}

void BPEVocabulary::encodePiece(std::string_view piece, PaddedPODArray<UInt32> & result) const
{
    /// A short piece, which is what ordinary text is made of, is merged the way `tiktoken` merges:
    /// after every merge the parts are scanned for the lowest ranked pair, the leftmost one on a tie.
    /// The scan is quadratic in the length of the piece, but it runs on the stack, where the heap
    /// below would ask the allocator for memory on every piece.
    static constexpr size_t max_short_piece = 64;
    if (piece.size() <= max_short_piece)
    {
        struct Part
        {
            UInt32 start;
            /// The rank of the merge of this part with the next one.
            UInt32 rank;
        };

        /// A piece of a single byte is a token, so it never gets here.
        chassert(piece.size() >= 2);
        const size_t size = piece.size();
        std::array<Part, max_short_piece + 1> parts{};
        size_t count = size + 1;
        for (size_t i = 0; i + 1 < size; ++i)
            parts[i] = {static_cast<UInt32>(i), rankOf(piece.substr(i, 2))};
        parts[size - 1] = {static_cast<UInt32>(size - 1), no_rank};
        parts[size] = {static_cast<UInt32>(size), no_rank};

        /// The rank of the merge of part `i` with the two parts after it, taken before the second of
        /// the three is removed.
        const auto rank_after_merge = [&](size_t i) -> UInt32
        {
            if (i + 3 >= count)
                return no_rank;
            return rankOf(piece.substr(parts[i].start, parts[i + 3].start - parts[i].start));
        };

        while (true)
        {
            UInt32 min_rank = no_rank;
            size_t min_index = 0;
            for (size_t i = 0; i + 1 < count; ++i)
            {
                if (parts[i].rank < min_rank)
                {
                    min_rank = parts[i].rank;
                    min_index = i;
                }
            }
            if (min_rank == no_rank)
                break;

            if (min_index > 0)
                parts[min_index - 1].rank = rank_after_merge(min_index - 1);
            parts[min_index].rank = rank_after_merge(min_index);
            std::copy(parts.begin() + min_index + 2, parts.begin() + count, parts.begin() + min_index + 1);
            --count;
        }

        for (size_t i = 0; i + 1 < count; ++i)
        {
            const UInt32 rank = rankOf(piece.substr(parts[i].start, parts[i + 1].start - parts[i].start));
            chassert(rank != no_rank);
            result.push_back(rank);
        }
        return;
    }

    /// A long piece is taken apart into its bytes, and the adjacent pair whose concatenation has the
    /// lowest rank is merged, until no pair is a token of the vocabulary. A merge only changes the
    /// two pairs that touch it, so the candidates live in a heap rather than being rescanned; a
    /// piece can be as long as the text itself, which a quadratic scan would not survive.
    const size_t size = piece.size();

    /// Part `i` starts at `start[i]` and ends where the next living part starts. Index `size` is a
    /// sentinel that is always the last, so `next` of a living part is always a valid index.
    std::vector<size_t> next(size + 1);
    std::vector<size_t> previous(size + 1);
    std::vector<UInt32> pair_rank(size + 1, no_rank);
    for (size_t i = 0; i <= size; ++i)
    {
        next[i] = i + 1;
        previous[i] = i == 0 ? size + 1 : i - 1;
    }

    using Candidate = std::pair<UInt32, size_t>;
    std::priority_queue<Candidate, std::vector<Candidate>, std::greater<>> candidates;

    /// The rank of the pair that starts at part `i`, where `end` bounds the part after it.
    const auto pairRankAt = [&](size_t i) -> UInt32
    {
        const size_t after = next[i];
        if (after >= size)
            return no_rank;
        return rankOf(piece.substr(i, next[after] - i));
    };

    const auto offer = [&](size_t i)
    {
        pair_rank[i] = pairRankAt(i);
        if (pair_rank[i] != no_rank)
            candidates.emplace(pair_rank[i], i);
    };

    for (size_t i = 0; i < size; ++i)
        offer(i);

    while (!candidates.empty())
    {
        const auto [rank, i] = candidates.top();
        candidates.pop();

        /// A merge leaves behind the entries of the pairs it changed. An entry is still the current
        /// one when the rank recorded for its part has not moved; equal ranks are interchangeable.
        if (pair_rank[i] != rank)
            continue;

        /// Absorb the part after `i`, then re-rank the pair that ends at `i` and the one after it.
        /// The absorbed part keeps its entries in the heap, so its rank is cleared to retire them.
        const size_t absorbed = next[i];
        pair_rank[absorbed] = no_rank;
        next[i] = next[absorbed];
        previous[next[i]] = i;

        offer(i);
        if (previous[i] <= size)
            offer(previous[i]);
    }

    for (size_t i = 0; i < size; i = next[i])
    {
        const UInt32 rank = rankOf(piece.substr(i, next[i] - i));
        chassert(rank != no_rank);
        result.push_back(rank);
    }
}

void BPEVocabulary::encode(std::string_view text, PaddedPODArray<UInt32> & result) const
{
    size_t pos = 0;
    while (pos < text.size())
    {
        const size_t length = nextBPEPiece(pretokenizer, text, pos);
        const std::string_view piece = text.substr(pos, length);

        /// Most pieces of ordinary text are a token in their own right.
        if (const UInt32 rank = rankOf(piece); rank != no_rank)
            result.push_back(rank);
        else
            encodePiece(piece, result);

        pos += length;
    }
}

void BPEVocabulary::decode(std::span<const UInt32> ids, WriteBuffer & out) const
{
    for (const UInt32 id : ids)
    {
        if (id >= by_rank.size() || by_rank[id].empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no token with id {} in the BPE vocabulary", id);
        out.write(by_rank[id].data(), by_rank[id].size());
    }
}

}

#if USE_ICU
#    pragma clang diagnostic pop
#endif
