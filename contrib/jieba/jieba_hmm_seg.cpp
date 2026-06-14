#include <jieba_common.h>

#include <span>

namespace Jieba
{

class DartsDict;

namespace
{

enum State
{
    B = 0,
    E = 1,
    M = 2,
    S = 3
};

constexpr size_t STATE_COUNT = 4;

/// Short alias used to keep the serialized hmm model file compact.
constexpr double Z = -3.14e+100;

/// Defines start_prob, trans_prob, emit_probs.
///
/// The HMM tables are indexed by raw Unicode codepoint (`emit_probs[state][rune]`).
/// `Rune` is `uint16_t` and `decodeUTF8Rune` returns either the raw codepoint or
/// the sentinel `0xFFFF` (for >BMP) / `0xFFFD` (for invalid UTF-8); both fit, so
/// every possible `Rune` value is a valid index into the table.
#include "jieba_hmm_model.dat"

std::vector<State> viterbi(std::span<const Rune> runes)
{
    std::vector<State> states;
    size_t len = runes.size();
    if (len == 0)
        return states;

    states.resize(len, B);
    if (len == 1)
    {
        states[0] = S;
        return states;
    }

    std::vector<std::array<double, STATE_COUNT>> weight(len);
    std::vector<std::array<State, STATE_COUNT>> path(len);

    for (size_t s = 0; s < STATE_COUNT; s++)
        weight[0][s] = start_prob[s] + emit_probs[s][runes[0]];

    for (size_t i = 1; i < len; i++)
    {
        for (size_t curr = 0; curr < STATE_COUNT; curr++)
        {
            weight[i][curr] = Z;
            double emit = emit_probs[curr][runes[i]];
            for (size_t prev = 0; prev < STATE_COUNT; prev++)
            {
                double score = weight[i - 1][prev] + trans_prob[prev][curr] + emit;
                if (score > weight[i][curr])
                {
                    weight[i][curr] = score;
                    path[i][curr] = static_cast<State>(prev);
                }
            }
        }
    }

    State state = weight[len - 1][E] >= weight[len - 1][S] ? E : S;
    for (size_t i = len; i-- > 0;)
    {
        states[i] = state;
        state = path[i][state];
    }

    return states;
}

/// Raw ASCII tests on the raw codepoint (no encoding remap).
inline bool isAsciiLetter(Rune r)
{
    return ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z');
}

inline bool isAsciiDigit(Rune r)
{
    return ('0' <= r && r <= '9');
}

inline bool isAsciiAlphaNumeric(Rune r)
{
    return isAsciiLetter(r) || isAsciiDigit(r);
}

/// Match a continuous alphanumeric run.
/// Mixed forms such as `5G`, `iPhone6s` and `H2O` are consumed as a single token.
/// A leading run of digits is allowed to contain `.` (so floats like `3.14` stay
/// in one token); once a letter has been seen we stop accepting `.` to avoid
/// swallowing sentence-ending dots.
size_t matchAlphaOrDigitSeq(const std::vector<Rune> & runes, size_t i)
{
    if (i >= runes.size() || !isAsciiAlphaNumeric(runes[i]))
        return i;

    bool seen_letter = isAsciiLetter(runes[i]);
    ++i;
    while (i < runes.size())
    {
        Rune r = runes[i];
        if (isAsciiAlphaNumeric(r))
        {
            seen_letter = seen_letter || isAsciiLetter(r);
            ++i;
            continue;
        }
        if (r == '.' && !seen_letter && i + 1 < runes.size() && isAsciiDigit(runes[i + 1]))
        {
            ++i;
            continue;
        }
        break;
    }
    return i;
}

}

struct HMMSegment
{
    static void cut(const DartsDict & /* dict */, const Runes & runes, size_t begin, size_t end, RuneRanges & ranges);
};

/// Unified segmentation: handle ASCII and non-ASCII ranges.
///   - ASCII alphanumeric: consume as a single English/number token (see `matchAlphaOrDigitSeq`).
///   - ASCII non-word characters: dropped (treated as separators).
///   - Non-ASCII: feed to the HMM (Viterbi) segmenter.
///
/// Non-word ASCII characters (punctuation, control characters, the `\0` padding
/// that `FixedString` adds to short values, etc.) are deliberately dropped here
/// rather than emitted as standalone tokens — they are not meaningful tokens for
/// a text index, and emitting them inflates the dictionary with noise.
void HMMSegment::cut(const DartsDict & /* dict */, const Runes & runes_data, size_t begin, size_t end, RuneRanges & ranges)
{
    if (begin >= end || runes_data.empty())
        return;

    const auto & runes = runes_data.getRunes();
    size_t i = begin;

    while (i < end)
    {
        Rune r = runes[i];

        /// ASCII branch (raw codepoint < 0x80).
        if (r < 0x80)
        {
            if (isAsciiAlphaNumeric(r))
            {
                size_t next = matchAlphaOrDigitSeq(runes, i);
                ranges.emplace_back(RuneRange{i, next - 1});
                i = next;
            }
            else
            {
                /// Drop ASCII non-word characters (punctuation, control bytes, padding).
                ++i;
            }
            continue;
        }

        /// Non-ASCII section (likely Chinese / CJK).
        size_t j = i;
        while (j < end && runes[j] >= 0x80)
            ++j;

        std::span<const Rune> span(&runes[i], j - i);
        std::vector<State> states = viterbi(span);

        size_t word_start = 0;
        for (size_t k = 0; k < states.size(); k++)
        {
            if (states[k] == E || states[k] == S)
            {
                ranges.emplace_back(RuneRange{i + word_start, i + k});
                word_start = k + 1;
            }
        }

        if (word_start < states.size())
            ranges.emplace_back(RuneRange{i + word_start, j - 1});

        i = j;
    }
}

}
