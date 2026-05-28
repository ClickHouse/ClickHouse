#pragma once

#include <Core/Range.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/StatisticsDescription.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{

constexpr std::string_view STATS_FILE_PREFIX = "statistics_";
constexpr std::string_view STATS_FILE_SUFFIX = ".stats";

/// Version of the per-column statistics file format stored inside statistics.packed (or legacy statistics_<col>.stats).
/// When adding a new version, bump the latest value and add a comment describing what changed.
enum class StatisticsFileVersion : UInt16
{
    V0 = 0,
    V1 = 1, /// modified the format of uniq, https://github.com/ClickHouse/ClickHouse/pull/90311
    V2 = 2, /// minmax statistics now serialize Field type and use Field instead of Float64
    V3 = 3, /// reserved — never use this value. PR #102356 briefly wrote V3 before being reverted.
            /// The deserializer rejects V3 to avoid attempting to read incompatible reverted-format files.
    V4 = 4, /// per-statistic size prefix added (`stat_size: UInt64` precedes each stat payload),
            /// so unknown statistics types can be skipped on deserialize.
};

class Field;
class Block;

struct StatisticsUtils
{
    /// Returns std::nullopt if input Field cannot be converted to a concrete value
    /// - `data_type` is the type of the column on which the statistics object was build on
    static std::optional<Float64> tryConvertToFloat64(const Field & value, const DataTypePtr & data_type);
};

class IStatistics;
using StatisticsPtr = std::shared_ptr<IStatistics>;

/// Interface for a single statistics object for a column within a part.
///
/// Statistics describe properties of the values in the column, e.g. how many unique values exist, what are
/// the N most frequent values, how frequent a value V is, etc.
class IStatistics
{
public:
    explicit IStatistics(const SingleStatisticsDescription & stat_);
    virtual ~IStatistics() = default;

    virtual void build(const ColumnPtr & column) = 0;
    virtual void merge(const StatisticsPtr & other_stats) = 0;

    virtual void serialize(WriteBuffer & buf) = 0;
    virtual void deserialize(ReadBuffer & buf, StatisticsFileVersion version) = 0;

    /// Estimate the cardinality of the column.
    /// Throws if the statistics object is not able to do a meaningful estimation.
    virtual UInt64 estimateCardinality() const;

    /// Per-value estimations.
    /// Returns std::nullopt when the statistics object cannot produce a meaningful estimate
    /// (e.g. the value cannot be converted to the column type).
    virtual Float64 estimateEqual(const Field & val) const; /// cardinality of val in the column
    virtual std::optional<Float64> estimateLess(const Field & val) const;  /// summarized cardinality of values < val in the column
    virtual Float64 estimateRange(const Range & range) const;
    virtual String getNameForLogs() const = 0;

protected:
    SingleStatisticsDescription stat;
};

class ColumnStatistics;
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;

struct Estimate
{
    std::set<StatisticsType> types;
    UInt64 rows_count = 0;
    std::optional<UInt64> estimated_cardinality;
    std::optional<Field> estimated_min;
    std::optional<Field> estimated_max;
    std::optional<UInt64> estimated_null_count;
};

using Estimates = std::unordered_map<String, Estimate>;

/// All statistics objects for a column in a part
class ColumnStatistics
{
public:
    using StatsMap = std::map<StatisticsType, StatisticsPtr>;
    explicit ColumnStatistics(const ColumnStatisticsDescription & stats_desc_);

    void serialize(WriteBuffer & buf) const;
    static std::shared_ptr<ColumnStatistics> deserialize(ReadBuffer & buf, const DataTypePtr & data_type);

    void build(const ColumnPtr & column);
    void merge(const ColumnStatisticsPtr & other);

    UInt64 getNumRows() const { return rows; }
    /// Total NULL rows for a Nullable column when `Basic` statistics are present; 0 otherwise.
    /// Callers should consult `hasNullCount` first.
    UInt64 getNullCount() const;
    /// Returns `rows - getNullCount()` when null-count tracking is available, else `rows`.
    UInt64 getNonNullRowCount() const;
    /// True iff null-count tracking is available for this column (e.g. via `Basic` on a Nullable column).
    bool hasNullCount() const;
    UInt64 estimateCardinality() const;
    UInt64 estimateDefaults() const;

    /// `null_count / rows` when `Basic` statistics are present; otherwise a default factor.
    Float64 estimateIsNull() const;
    /// `(rows - null_count) / rows` when `Basic` statistics are present; otherwise a default factor.
    Float64 estimateIsNotNull() const;

    std::optional<Float64> estimateLess(const Field & val) const;
    std::optional<Float64> estimateGreater(const Field & val) const;
    std::optional<Float64> estimateEqual(const Field & val) const;
    std::optional<Float64> estimateRange(const Range & range) const;

    Estimate getEstimate() const;
    String getNameForLogs() const;
    DataTypePtr getDataType() const { return stats_desc.data_type; }

    const StatsMap & getStats() const { return stats; }
    bool structureEquals(const ColumnStatistics & other) const;
    std::shared_ptr<ColumnStatistics> cloneEmpty() const;

private:
    friend class MergeTreeStatisticsFactory;
    ColumnStatisticsDescription stats_desc;
    StatsMap stats;
    UInt64 rows = 0; /// the number of rows in the column
};

class ColumnsStatistics : public std::map<String, ColumnStatisticsPtr>
{
public:
    static constexpr std::string_view FILENAME = "statistics.packed";

    ColumnsStatistics() = default;
    using Base = std::map<String, ColumnStatisticsPtr>;
    using Base::Base;

    explicit ColumnsStatistics(const ColumnsDescription & columns);
    ColumnsStatistics cloneEmpty() const;

    void build(const Block & block);
    void buildIfExists(const Block & block);
    void merge(const ColumnsStatistics & other);
    Estimates getEstimates() const;
};

struct ColumnDescription;
class ColumnsDescription;

class MergeTreeStatisticsFactory : private boost::noncopyable
{
public:
    static MergeTreeStatisticsFactory & instance();

    void validate(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const;
    ColumnStatisticsDescription cloneWithSupportedStatistics(const ColumnStatisticsDescription & stats, const DataTypePtr & data_type) const;

    using Validator = std::function<bool(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;
    using Creator = std::function<StatisticsPtr(const SingleStatisticsDescription & stats, const DataTypePtr & data_type)>;

    ColumnStatisticsPtr get(const ColumnDescription & column_desc) const;
    ColumnStatisticsPtr get(const ColumnStatisticsDescription & stats_desc) const;
    ColumnStatisticsDescription::StatisticsTypeDescMap get(const std::vector<StatisticsType> & stat_types, const DataTypePtr & data_type) const;
    /// Create a single statistics object by type. Returns `nullptr` if the type is unknown
    /// or unsupported for `data_type`. Used by the V4 deserializer to instantiate statistics
    /// types one at a time (and to silently skip types the current build doesn't know about).
    StatisticsPtr tryCreateSingle(StatisticsType type, const DataTypePtr & data_type) const;

    void registerValidator(StatisticsType type, Validator validator);
    void registerCreator(StatisticsType type, Creator creator);

protected:
    MergeTreeStatisticsFactory();

private:
    using Validators = std::unordered_map<StatisticsType, Validator>;
    using Creators = std::unordered_map<StatisticsType, Creator>;
    Validators validators;
    Creators creators;
};

void removeImplicitStatistics(ColumnsDescription & columns);
void addImplicitStatistics(ColumnsDescription & columns, const String & statistics_types_str);

}
