#include <jieba_dict.h>

namespace Jieba
{

namespace
{

void calcDP(DAG & dag)
{
    size_t size = dag.size();
    for (size_t i = size; i-- > 0;)
    {
        for (const auto & it : dag[i].nexts)
        {
            int next = it.first;
            double val = it.second;

            if (next < size)
                val += dag[next].max_weight;

            if (next <= size && val > dag[i].max_weight)
            {
                dag[i].max_weight = val;
                dag[i].max_next = next;
            }
        }
    }
}

}

struct MPSegment
{
    static void cut(const DartsDict & dict, const Runes & runes, size_t begin, size_t end, RuneRanges & ranges);
};

void MPSegment::cut(const DartsDict & dict, const Runes & runes_data, size_t begin, size_t end, RuneRanges & ranges)
{
    if (begin >= end || runes_data.empty())
        return;

    const auto & runes = runes_data.getRunes();
    std::span<const Rune> span(&runes[begin], end - begin);
    auto dag = dict.buildDAG(span);
    calcDP(dag);
    for (size_t i = 0; i < dag.size();)
    {
        int next = dag[i].max_next;
        ranges.push_back({begin + i, begin + next - 1});
        i = next;
    }
}

}
