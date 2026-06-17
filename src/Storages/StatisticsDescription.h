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
    CountMinSketch = 2,
    MinMax = 3,

    Max = 63,
};

struct SingleStatisticsDescription
{
    StatisticsType type;

    ASTPtr ast;

    String getTypeName() const;

    SingleStatisticsDescription() = delete;
    SingleStatisticsDescription(StatisticsType type_, ASTPtr ast_);

    SingleStatisticsDescription(const SingleStatisticsDescription & other) { *this = other; }
    SingleStatisticsDescription & operator=(const SingleStatisticsDescription & other);
    SingleStatisticsDescription(SingleStatisticsDescription && other) noexcept { *this = std::move(other); }
    SingleStatisticsDescription & operator=(SingleStatisticsDescription && other) noexcept;

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

    /// get a vector of <column name, statistics desc> pair
    static std::vector<std::pair<String, ColumnStatisticsDescription>> fromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);
    static ColumnStatisticsDescription fromColumnDeclaration(const ASTColumnDeclaration & column, DataTypePtr data_type);

    using StatisticsTypeDescMap = std::map<StatisticsType, SingleStatisticsDescription>;
    StatisticsTypeDescMap types_to_desc;
    DataTypePtr data_type;
};

}
