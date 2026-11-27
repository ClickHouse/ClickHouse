#pragma once

#include "config.h"

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Range.h>
#include <Processors/Chunk.h>

namespace DB
{

#if USE_AVRO

class DataFileStatistics
{
public:
    explicit DataFileStatistics(Poco::JSON::Array::Ptr schema_);

    void update(const Chunk & chunk);

    std::vector<std::pair<size_t, size_t>> getColumnSizes() const;
    std::vector<std::pair<size_t, size_t>> getNullCounts() const;
    std::vector<std::pair<size_t, Field>> getLowerBounds() const;
    std::vector<std::pair<size_t, Field>> getUpperBounds() const;

    const std::vector<Int64> & getFieldIds() const { return field_ids; }
private:
    static Range uniteRanges(const Range & left, const Range & right);

    std::vector<Int64> field_ids;
    std::vector<Int64> column_sizes;
    std::vector<Int64> null_counts;
    std::vector<Range> ranges;
};

#endif

}
