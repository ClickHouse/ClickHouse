#include <Storage/MergeTree/MarkRange.h>

namespace DB
{

struct MarkRangeSelective
{
    MarkRangeSelective() = default;
    MarkRangeSelective(MarkRange range, const std::vector<size_t>& selected) : MarkRange(range), selected(selected) {}
    MarkRangeSelective(MarkRange range, std::vector<size_t>&& selected) : MarkRange(range), selected(std::move(selected)) {}

    std::vector<size_t> selected;
}

}