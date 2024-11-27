#pragma once

#include <stdexcept>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Common/CurrentThread.h>
#include "../BSON/Binary.h"
#include "../BSON/Document.h"
#include "../BSON/Element.h"
#include "Commands.h"

namespace DB
{
namespace MongoDB
{

struct SelectOptions
{
    enum AggregationOperators
    {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    };

    // TODO somehow rewrite this three functions |  idk how
    static std::string aggregateOperatorToString(AggregationOperators oper);

    static std::string aggregateOperatorToRawString(AggregationOperators oper);

    static AggregationOperators aggregateOperatorToRawString(const std::string & oper);
    static AggregationOperators aggregateOperatorFromRawString(const std::string & str);

    std::string expression;
    std::optional<std::string> aggregate_operator;
    std::optional<std::string> new_col_name;
    bool distinct = false;

    SelectOptions(std::string && expression_) : expression(std::move(expression_)) { }

    SelectOptions(BSON::Element::Ptr command);

    SelectOptions & setNewColumnName(std::string && new_col_name_)
    {
        new_col_name = std::move(new_col_name_);
        return *this;
    }

    SelectOptions & setNewColumnName(const std::string & new_col_name_)
    {
        new_col_name = new_col_name_;
        return *this;
    }

    // TODO maybe add function that takes std::string to avoid redundant casts
    SelectOptions & setAggregateOper(AggregationOperators oper)
    {
        aggregate_operator = aggregateOperatorToString(oper);
        return *this;
    }

    std::optional<std::string> getAggregateOper() const { return aggregate_operator; }

    SelectOptions & setDistinct(bool status)
    {
        distinct = status;
        return *this;
    }
};

class QueryBuilder
{
public:
    QueryBuilder(std::string collection_name_, std::vector<std::string> && columns_)
        : collection_name(collection_name_), proj_map(std::move(columns_))
    {
    }

    /// adders
    void addToSelect(const std::string & name, bool status);

    void addToSelect(SelectOptions && options);

    void addToOrderBy(const std::string & expr, Int32 order);

    void setLimit(Int32 limit_);

    void addWhere(const std::string & expr, bool having);

    void setOffset(Int32 offset);

    /// handlers
    void handleGroup(BSON::Document::Ptr group);
    void handleSort(BSON::Document::Ptr sort);
    void handleProject(BSON::Document::Ptr project);
    void handleWhereSingleColumn(const std::string & column_name, BSON::Element::Ptr payload, bool having);
    void handleWhere(BSON::Document::Ptr match, bool having = false);
    void handleCount(const std::string & new_col_name);


    /// builders
    std::string buildSelect() &&;
    std::string buildWhere() const;
    std::string buildGroupBy() const;
    std::string buildHaving() const;
    std::string buildOrderBy() const;
    std::string buildLimit() const;
    std::string buildOffset() const;

    std::string buildQuery() &&;

private:
    void handleGroupIds(std::vector<BSON::Element::Ptr> && ids, BSON::Document::Ptr operations);


    std::string collection_name;
    std::unordered_map<std::string, std::string> group_by_map;

    ProjectionMap proj_map;
    std::vector<std::string> select_elems;

    std::vector<std::string> order_by;

    std::vector<std::string> where;
    std::vector<std::string> having_vec;

    std::optional<Int32> limit;
    std::optional<Int32> offset;
    bool count_expr = false;
    bool has_group_operations = false;
};


inline void QueryBuilder::addToSelect(const std::string & name, bool status)
{
    proj_map.add(name, status);
}

inline void QueryBuilder::addToOrderBy(const std::string & expr, Int32 order)
{
    std::string expr_ = expr;
    removeDollarFromName(expr_);
    order_by.emplace_back(fmt::format("{} {}", expr_, order > 0 ? "ASC" : "DESC"));
}

inline void QueryBuilder::setLimit(Int32 limit_)
{
    limit = limit_;
}

inline void QueryBuilder::setOffset(Int32 offset_)
{
    offset = offset_;
}

inline void QueryBuilder::addWhere(const std::string & expr, bool having)
{
    if (having)
        having_vec.push_back(expr);
    else
        where.push_back(expr);
}

inline std::string QueryBuilder::buildLimit() const
{
    if (!limit.has_value())
        return "";
    return fmt::format("LIMIT {}", limit.value());
}

inline std::string QueryBuilder::buildOffset() const
{
    if (!offset.has_value())
        return "";
    return fmt::format("OFFSET {}", offset.value());
}


}
}
