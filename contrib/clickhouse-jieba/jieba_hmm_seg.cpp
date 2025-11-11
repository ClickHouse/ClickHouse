#include "jieba_common.h"

#include <span>
namespace Jieba::HMMSegment
{

enum State
{
    B = 0,
    E = 1,
    M = 2,
    S = 3
};

static constexpr size_t STATE_COUNT = 4;

namespace
{

/// Defines start_prob, trans_prob, emit_probs
#include "jieba_hmm_model.dat"

double getEmitProb(size_t state, Rune rune)
{
    double prob = emit_probs[state][rune];
    if (prob == 0)
        return -8.0;
    return prob;
}

std::vector<State> viterbi(std::span<const RuneInfo> runes)
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

    std::vector<std::vector<double>> weight(len, std::vector<double>(STATE_COUNT, -1e100));
    std::vector<std::vector<State>> path(len, std::vector<State>(STATE_COUNT, B));

    for (size_t s = 0; s < STATE_COUNT; s++)
        weight[0][s] = start_prob[s] + getEmitProb(s, runes[0].rune);

    for (size_t i = 1; i < len; i++)
    {
        for (size_t curr = 0; curr < STATE_COUNT; curr++)
        {
            double emit = getEmitProb(curr, runes[i].rune);
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

    State best_state = weight[len - 1][E] > weight[len - 1][S] ? E : S;
    for (size_t i = len; i-- > 0;)
    {
        states[i] = best_state;
        if (i > 0)
            best_state = path[i][best_state];
    }

    return states;
}

}

void cut(const std::vector<RuneInfo> & runes, size_t begin, size_t end, std::vector<RuneRange> & ranges)
{
    if (begin >= end || runes.empty())
        return;

    std::vector<RuneInfo> segment_runes(runes.begin() + begin, runes.begin() + end);
    std::vector<State> states = viterbi(segment_runes);

    size_t word_start = 0;
    for (size_t i = 0; i < states.size(); i++)
    {
        if (states[i] == E || states[i] == S)
        {
            ranges.emplace_back(RuneRange{begin + word_start, begin + i + 1});
            word_start = i + 1;
        }
    }

    if (word_start < states.size())
        ranges.emplace_back(RuneRange{begin + word_start, begin + states.size()});
}

}
