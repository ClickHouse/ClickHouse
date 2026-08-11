#pragma once

#include <DataTypes/IDataType_fwd.h>
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
    UniqAssumedAllDistinct = 6, /// Serialized physical representation for Uniq with all non-NULL values assumed distinct.
    UniqV2AssumedAllDistinct = 7, /// Serialized physical representation for UniqV2 with all non-NULL values assumed distinct.

    Max = 63,
};

enum class StatisticsMaterialization : UInt8
{
    Default = 0,
    AssumedAllDistinct,
};

struct SingleStatisticsDescription
{
    StatisticsType type;
    ASTPtr ast;
    bool is_implicit = false;
    StatisticsMaterialization materialization = StatisticsMaterialization::Default;

    String getTypeName() const;

    SingleStatisticsDescription() = delete;
    SingleStatisticsDescription(
        StatisticsType type_,
        ASTPtr ast_,
        bool is_implicit_,
        StatisticsMaterialization materialization_ = StatisticsMaterialization::Default);

    SingleStatisticsDescription(const SingleStatisticsDescription & other)
        : type{}
    {
        *this = other;
    }
    SingleStatisticsDescription & operator=(const SingleStatisticsDescription & other);
    SingleStatisticsDescription(SingleStatisticsDescription && other) noexcept
        : type{}
    {
        *this = std::move(other);
    }
    SingleStatisticsDescription & operator=(SingleStatisticsDescription && other) noexcept;

    bool operator==(const SingleStatisticsDescription & other) const;
};

class ColumnsDescription;

struct ColumnStatisticsDescription
{
    bool operator==(const ColumnStatisticsDescription & other) const;

    bool empty() const;

    bool hasExplicitStatistics() const;

    bool contains(const String & stat_type) const;

    void merge(const ColumnStatisticsDescription & other, const String & column_name, DataTypePtr column_type, bool if_not_exists);

    void assign(const ColumnStatisticsDescription & other);

    void clear();

    ASTPtr getAST() const;

    String getNameForLogs() const;

    /// get a vector of <column name, statistics desc> pair
    static std::vector<std::pair<String, ColumnStatisticsDescription>>
    fromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);
    static ColumnStatisticsDescription fromStatisticsDescriptionAST(
        const ASTPtr & statistics_desc, const String & column_name, DataTypePtr data_type, bool is_implicit_ = false);

    using StatisticsTypeDescMap = std::map<StatisticsType, SingleStatisticsDescription>;
    StatisticsTypeDescMap types_to_desc;
    DataTypePtr data_type;
};

StatisticsType stringToStatisticsType(String type);
String statisticsTypeToString(StatisticsType type);
String statisticsMaterializationToString(StatisticsMaterialization materialization);
ASTPtr makeStatisticsTypeAST(StatisticsType type, StatisticsMaterialization materialization = StatisticsMaterialization::Default);
bool isAssumedAllDistinctSerializedStatisticsType(StatisticsType type);
StatisticsType getLogicalStatisticsType(StatisticsType type);
StatisticsType getAssumedAllDistinctSerializedStatisticsType(StatisticsType type);
bool isUniqLikeStatisticsType(StatisticsType type);

}
