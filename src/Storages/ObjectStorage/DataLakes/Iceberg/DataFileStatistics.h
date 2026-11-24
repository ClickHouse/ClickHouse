#pragma once

#include <Processors/ISimpleTransform.h>
#include "config.h"

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Block_fwd.h>
#include <Core/Range.h>
#include <Processors/Chunk.h>

#include <IO/Progress.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Access/EnabledQuota.h>

namespace DB
{

#if USE_AVRO

class DataFileStatistics
{
public:
    explicit DataFileStatistics(Poco::JSON::Array::Ptr schema_);

    /// Every statistic is accumulated in written-block order, so `field_ids[i]` must be the id of
    /// block column `i` rather than of schema field `i`. An empty `written_field_ids` keeps the schema's ids.
    DataFileStatistics(Poco::JSON::Array::Ptr schema_, std::vector<Int64> written_field_ids);

    void update(const Chunk & chunk);
    void addColumnSizesOnDisk(const std::unordered_map<String, size_t> & sizes_by_column_name, const Block & sample_block);
    void merge(const DataFileStatistics & other);

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
    Ranges ranges;
};

using DataFileStatisticsPtr = std::shared_ptr<DataFileStatistics>;

class IcebergStatisticsTransform final : public ISimpleTransform
{
public:
    explicit IcebergStatisticsTransform(
        SharedHeader header,
        DataFileStatisticsPtr stats_)
        : ISimpleTransform(header, header, true)
        , stats(stats_)
        {}

    String getName() const override { return "IcebergStatisticsTransform"; }

    void transform(Chunk & chunk) override;
    DataFileStatisticsPtr getResultStats() const
    {
        return stats;
    }

protected:
    DataFileStatisticsPtr stats;

    Chunk cur_chunk;
};

using IcebergStatisticsTransformPtr = std::shared_ptr<IcebergStatisticsTransform>;

#endif

}
