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

/// Defines start_prob, trans_prob, emit_probs
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

/// Check if a rune is an ASCII letter or digit
inline bool isAlpha(Rune r)
{
    return ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z');
}

inline bool isDigit(Rune r)
{
    return ('0' <= r && r <= '9');
}

/// Match a continuous sequence of [a-zA-Z0-9] or [0-9.]+
/// Return index right after the sequence
size_t matchAlphaOrDigitSeq(const std::vector<Rune> & runes, size_t i)
{
    /// Undo mapping to check
    Rune r = runes[i] - 0xF000;

    /// [a-zA-Z]+[a-zA-Z0-9]*
    if (isAlpha(r))
    {
        ++i;
        while (i < runes.size())
        {
            r = runes[i] - 0xF000;
            if (isAlpha(r) || isDigit(r))
                ++i;
            else
                break;
        }
        return i;
    }

    /// [0-9]+(.[0-9]+)*
    if (isDigit(r))
    {
        ++i;
        while (i < runes.size())
        {
            r = runes[i] - 0xF000;
            if (isDigit(r) || r == '.')
                ++i;
            else
                break;
        }
        return i;
    }

    /// not matched
    return i;
}

}

struct HMMSegment
{
    static void cut(const DartsDict & /* dict */, const Runes & runes, size_t begin, size_t end, RuneRanges & ranges);
};

/// Unified segmentation: handle ASCII and non-ASCII ranges
/// - ASCII: match English words / numbers
/// - non-ASCII: use HMM (Viterbi)
void HMMSegment::cut(const DartsDict & /* dict */, const Runes & runes_data, size_t begin, size_t end, RuneRanges & ranges)
{
    if (begin >= end || runes_data.empty())
        return;

    const auto & runes = runes_data.getRunes();
    size_t i = begin;

    while (i < end)
    {
        Rune r = runes[i];

        /// ASCII section. ASCII-range mapped to 0xF0xx
        if (0xF000 <= r && r <= 0xF0FF)
        {
            size_t next = matchAlphaOrDigitSeq(runes, i);
            if (next == i)
                ++next; // single ASCII symbol
            ranges.emplace_back(RuneRange{i, next - 1});
            i = next;
            continue;
        }

        /// Non-ASCII section (likely Chinese)
        size_t j = i;
        while (j < end)
        {
            Rune r = runes[j];

            /// Treat mapped ASCII (0xF000~0xF0FF) as ASCII, skip
            if (0xF000 <= r && r <= 0xF0FF)
                break;

            /// Otherwise non-ASCII
            ++j;
        }

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
