#pragma once

#include <Storages/Statistics/Statistics.h>

namespace DB
{

/// The statistics of each path of a `JSON` column: typed paths, dynamic paths and paths stored in shared data. Each path
/// gets the declared statistics that support its type, and `basic`.
class StatisticsJSONPaths : public IStatistics
{
public:
    /// Each describes all rows of the column, a row without the path counting as NULL.
    using PathStatistics = std::map<String, ColumnStatisticsPtr>;

    StatisticsJSONPaths(
        const SingleStatisticsDescription & description,
        const DataTypePtr & data_type_,
        ColumnStatisticsDescription::StatisticsTypeDescMap path_types_);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    [[nodiscard]] bool isCompatibleWith(const IStatistics & other) const override;
    void copyStructureFrom(const IStatistics & other) override;

    [[nodiscard]] String getNameForLogs() const override;
    [[nodiscard]] size_t allocatedBytes() const override;

    /// The statistics of the paths built or merged in memory. Statistics read from disk keep them serialized, see
    /// `cloneWithPaths`.
    [[nodiscard]] const PathStatistics & getPathStatistics() const { return path_statistics; }
    /// A copy with the statistics of `paths` only, deserialized. It shares the statistics built in memory with this one.
    [[nodiscard]] std::shared_ptr<StatisticsJSONPaths> cloneWithPaths(const NameSet & paths) const;

private:
    [[nodiscard]] DataTypePtr getPathType(const String & path) const;
    [[nodiscard]] ColumnStatisticsPtr createPathStatistics(const DataTypePtr & path_type) const;
    [[nodiscard]] ColumnStatisticsPtr deserializePathStatistics(const String & path, const String & data) const;
    void addNullRowsToPaths();

    DataTypePtr data_type;
    ColumnStatisticsDescription::StatisticsTypeDescMap path_types;
    UInt64 rows = 0;
    PathStatistics path_statistics;
    /// The statistics of the paths read from disk, serialized: a path is deserialized when it is used.
    std::map<String, String> serialized_path_statistics;
};

/// The statistics of the paths of a column with the statistics `stats_desc`, or nullptr if it is not a `JSON` column
/// with declared statistics.
StatisticsPtr createJSONPathsStatistics(const ColumnStatisticsDescription & stats_desc);

bool jsonPathsStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr jsonPathsStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
