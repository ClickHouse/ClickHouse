#include <Processors/QueryPlan/StepAnalyzeInfo.h>

namespace DB
{

std::string_view toString(MetricGroupKey key)
{
    switch (key)
    {
        case MetricGroupKey::IO: return "I/O";
        case MetricGroupKey::Left: return "Left";
        case MetricGroupKey::Right: return "Right";
        case MetricGroupKey::HashTable: return "Hash table";
        case MetricGroupKey::Buffer: return "Buffer";
        case MetricGroupKey::Spill: return "Spill";
        case MetricGroupKey::Build: return "Build";
        case MetricGroupKey::Probe: return "Probe";
    }
}

std::string_view toString(MetricKey key)
{
    switch (key)
    {
        case MetricKey::Unnamed: return "";

        case MetricKey::InputRows: return "input rows";
        case MetricKey::OutputRows: return "output rows";
        case MetricKey::InputBytes: return "input bytes";
        case MetricKey::OutputBytes: return "output bytes";

        case MetricKey::Rows: return "rows";
        case MetricKey::Matched: return "matched";
        case MetricKey::MatchRate: return "match rate";
        case MetricKey::Fanout: return "fanout";

        case MetricKey::UniqueKeys: return "unique keys";
        case MetricKey::Memory: return "memory";
        case MetricKey::Buckets: return "buckets";
        case MetricKey::Rehashes: return "rehashes";

        case MetricKey::LeftSpilled: return "left spilled";
        case MetricKey::RightSpilled: return "right spilled";
        case MetricKey::Spilled: return "spilled";
        case MetricKey::Compressed: return "compressed";

        case MetricKey::Size: return "size";
        case MetricKey::Blocks: return "blocks";
        case MetricKey::Storage: return "storage";

        case MetricKey::SortTime: return "sort time";
        case MetricKey::SortShare: return "sort share";

        case MetricKey::Min: return "min";
        case MetricKey::Median: return "median";
        case MetricKey::Max: return "max";
        case MetricKey::Sum: return "sum";
    }
}

MetricFormat formatOf(MetricKey key)
{
    switch (key)
    {
        case MetricKey::Unnamed:
        case MetricKey::Compressed:
        case MetricKey::Storage:
            return MetricFormat::Raw;

        case MetricKey::InputRows:
        case MetricKey::OutputRows:
        case MetricKey::Rows:
        case MetricKey::Matched:
        case MetricKey::UniqueKeys:
        case MetricKey::Buckets:
        case MetricKey::Rehashes:
        case MetricKey::Blocks:
            return MetricFormat::Quantity;

        case MetricKey::InputBytes:
        case MetricKey::OutputBytes:
        case MetricKey::Memory:
        case MetricKey::LeftSpilled:
        case MetricKey::RightSpilled:
        case MetricKey::Spilled:
        case MetricKey::Size:
            return MetricFormat::Bytes;

        case MetricKey::SortTime:
        case MetricKey::Min:
        case MetricKey::Median:
        case MetricKey::Max:
        case MetricKey::Sum:
            return MetricFormat::Time;

        case MetricKey::MatchRate:
        case MetricKey::SortShare:
            return MetricFormat::Percent;

        case MetricKey::Fanout:
            return MetricFormat::Ratio;
    }
}

}
