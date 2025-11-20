#pragma once

#include <span>
#include <vector>
#include <darts.h>
#include <jieba_common.h>

namespace Jieba
{

struct DAGNode
{
    std::vector<std::pair<size_t, double>> nexts;
    double max_weight = -3.14e+100;
    int max_next = -1;
};

using DAG = std::vector<DAGNode>;

struct DartsHeader
{
    double min_weight = 0;
    size_t num_elems = 0;
    size_t da_size = 0;
};

class DartsDict
{
public:
    DartsDict();

    double find(std::span<const Rune> key) const;
    DAG buildDAG(std::span<const Rune> runes) const;

private:
    ::Darts::DoubleArray da;
    const double * elems = nullptr;
    double min_weight = 0;
    size_t num_elems = 0;
};

}
