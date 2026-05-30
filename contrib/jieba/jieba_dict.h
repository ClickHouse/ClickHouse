#pragma once

#include <cstdint>
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

/// On-disk header layout. Sizes match the format produced by `generate_dict.py`
/// (8 bytes each), so we use fixed-width types here regardless of the platform's `size_t`.
struct DartsHeader
{
    double min_weight = 0;
    uint64_t num_elems = 0;
    uint64_t da_size = 0;
};

class DartsDict
{
public:
    DartsDict();

    /// `elems` and the internal `da` array point into `storage`, so copying or moving
    /// the object would leave those borrowed pointers dangling at the old buffer.
    /// The dictionary is only ever held as a single long-lived instance, so forbid both.
    DartsDict(const DartsDict &) = delete;
    DartsDict & operator=(const DartsDict &) = delete;
    DartsDict(DartsDict &&) = delete;
    DartsDict & operator=(DartsDict &&) = delete;

    double find(std::span<const Rune> key) const;
    DAG buildDAG(std::span<const Rune> runes) const;

private:
    /// Decompressed dictionary buffer. Held as uint64_t so that the underlying
    /// storage is guaranteed to be 8-byte aligned, which matches the alignment
    /// requirements of the embedded `double` weights array.
    std::vector<uint64_t> storage;

    ::Darts::DoubleArray da;
    const double * elems = nullptr;
    double min_weight = 0;
    uint64_t num_elems = 0;
};

}
