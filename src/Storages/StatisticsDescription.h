#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>

#include <base/types.h>

#include <map>

namespace DB
{

enum class StatisticsType : UInt8
{
    TDigest = 0,
    Uniq = 1,
    CountMinSketch = 2,
    MinMax = 3,
    Basic = 4,
    UniqV2 = 5,

    Max = 63,
};

struct SingleStatisticsDescription
{
    StatisticsType type;
    ASTPtr ast;

    String getTypeName() const;

    SingleStatisticsDescription() = delete;
    SingleStatisticsDescription(StatisticsType type_, ASTPtr ast_);

    SingleStatisticsDescription(const SingleStatisticsDescription & other) : type{} { *this = other; }
    SingleStatisticsDescription & operator=(const SingleStatisticsDescription & other);
    SingleStatisticsDescription(SingleStatisticsDescription && other) noexcept : type{} { *this = std::move(other); }
    SingleStatisticsDescription & operator=(SingleStatisticsDescription && other) noexcept;

    bool operator==(const SingleStatisticsDescription & other) const;

    /// The memory retained by `ast` (an upper bound), not including the object itself.
    size_t allocatedBytes() const;
};

class ColumnsDescription;

/// Statistics of a column. They are never declared explicitly: they are derived from the
/// `auto_statistics_types` MergeTree setting for every column whose type supports them.
struct ColumnStatisticsDescription
{
    bool operator==(const ColumnStatisticsDescription & other) const;

    bool empty() const;

    bool contains(const String & stat_type) const;

    void merge(const ColumnStatisticsDescription & other, const String & column_name, DataTypePtr column_type, bool if_not_exists);

    void clear();

    String getNameForLogs() const;

    using StatisticsTypeDescMap = std::map<StatisticsType, SingleStatisticsDescription>;
    StatisticsTypeDescMap types_to_desc;
    DataTypePtr data_type;
};

StatisticsType stringToStatisticsType(String type);
String statisticsTypeToString(StatisticsType type);

}
