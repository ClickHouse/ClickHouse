#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>

#include <base/types.h>

namespace DB
{

enum class StatisticsType : UInt8
{
    TDigest = 0,
    Uniq = 1,

    UnknownStatistics = 63,
};

struct SingleStatisticsDescription
{
    StatisticsType type;

    ASTPtr ast;

    String getTypeName() const;

    SingleStatisticsDescription() = delete;
    SingleStatisticsDescription(StatisticsType type_, ASTPtr ast_);

    bool operator==(const SingleStatisticsDescription & other) const;
};

class ColumnsDescription;

struct ColumnStatisticsDescription
{
    bool operator==(const ColumnStatisticsDescription & other) const;

    bool empty() const;

    bool contains(const String & stat_type) const;

    void merge(const ColumnStatisticsDescription & other, const String & column_name, DataTypePtr column_type, bool if_not_exists);

    void assign(const ColumnStatisticsDescription & other);

    void clear();

    ASTPtr getAST() const;

    static std::vector<ColumnStatisticsDescription> getStatisticsDescriptionsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);
    static ColumnStatisticsDescription getStatisticFromColumnDeclaration(const ASTColumnDeclaration & column);

    using StatisticsTypeDescMap = std::map<StatisticsType, SingleStatisticsDescription>;
    StatisticsTypeDescMap types_to_desc;
    String column_name;
    DataTypePtr data_type;
};

}
