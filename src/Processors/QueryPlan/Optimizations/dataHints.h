#pragma once

#include <limits>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>
#include <Core/Field.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

/// Hint about specific integer column
struct DataHint
{
    std::optional<Field> lower_boundary;
    std::optional<Field> upper_boundary;

    bool is_column_unsigned;

public:

    DataHint() : lower_boundary(std::nullopt), upper_boundary(std::nullopt), is_column_unsigned(false) {}

    DataHint(bool is_column_unsigned_) :
            lower_boundary(std::nullopt), upper_boundary(std::nullopt), is_column_unsigned(is_column_unsigned_)
    {
    }

    DataHint(std::optional<Field> lower, std::optional<Field> upper, bool is_column_unsigned_) :
            lower_boundary(lower), upper_boundary(upper), is_column_unsigned(is_column_unsigned_)
    {
    }

    void setLowerBoundary(const Field & value)
    {
        if ((value.getTypeName() == "UInt64" && is_column_unsigned) ||
            (value.getTypeName() == "Int64" && !is_column_unsigned))
        {
            lower_boundary = value;
            return;
        }
        if (value.getTypeName() == "Int64")
        {
            int64_t signed_value = value.get<int64_t>();
            if (signed_value < 0)
            {
                lower_boundary = static_cast<uint64_t>(0);
                return;
            }
            lower_boundary = value.get<uint64_t>();
        }
        else
        {
            uint64_t unsigned_value = value.get<uint64_t>();
            if (unsigned_value > std::numeric_limits<int64_t>::max())
            {
                lower_boundary = std::numeric_limits<int64_t>::max();
                return;
            }
            lower_boundary = value.get<int64_t>();
        }
    }

    void setStrictLowerBoundary(const Field & value)
    {
        if (value.getTypeName() == "UInt64")
        {
            if (value == std::numeric_limits<uint64_t>::max())
                setLowerBoundary(value.get<uint64_t>());
            else
                setLowerBoundary(value.get<uint64_t>() + 1);
        }
        else if (value.getTypeName() == "Int64")
        {
            if (value == std::numeric_limits<int64_t>::max())
                setLowerBoundary(value.get<int64_t>());
            else
                setLowerBoundary(value.get<int64_t>() + 1);
        }
    }

    void setUpperBoundary(const Field & value)
    {
        if ((value.getTypeName() == "UInt64" && is_column_unsigned) ||
            (value.getTypeName() == "Int64" && !is_column_unsigned))
        {
            upper_boundary = value;
            return;
        }
        if (value.getTypeName() == "Int64")
        {
            int64_t signed_value = value.get<int64_t>();
            if (signed_value < 0)
            {
                upper_boundary = static_cast<uint64_t>(0);
                return;
            }
            upper_boundary = value.get<uint64_t>();
        }
        else
        {
            uint64_t unsigned_value = value.get<uint64_t>();
            if (unsigned_value > std::numeric_limits<int64_t>::max())
            {
                upper_boundary = std::numeric_limits<int64_t>::max();
                return;
            }
            upper_boundary = value.get<int64_t>();
        }
    }

    void setStrictUpperBoundary(const Field & value)
    {
        if (value.getTypeName() == "UInt64")
        {
            if (value == std::numeric_limits<uint64_t>::min())
                setUpperBoundary(value.get<uint64_t>());
            else
                setUpperBoundary(value.get<uint64_t>() - 1);
        }
        else if (value.getTypeName() == "Int64")
        {
            if (value == std::numeric_limits<int64_t>::min())
                setUpperBoundary(value.get<int64_t>());
            else
                setUpperBoundary(value.get<int64_t>() - 1);
        }
    }

    void unionLowerBoundary(const std::optional<Field> & value)
    {
        if (!value || !lower_boundary)
            return;
        if (value.value() < lower_boundary.value())
            setLowerBoundary(value.value());
    }

    void unionUpperBoundary(const std::optional<Field> & value)
    {
        if (!value || !upper_boundary)
            return;
        if (upper_boundary.value() < value.value())
            setUpperBoundary(value.value());
    }

    void intersectLowerBoundary(const std::optional<Field> & value)
    {
        if (!value)
            return;
        if (!lower_boundary || lower_boundary.value() < value.value())
            setLowerBoundary(value.value());
    }

    void intersectUpperBoundary(const std::optional<Field> & value)
    {
        if (!value)
            return;
        if (!upper_boundary || value.value() < upper_boundary.value())
            setUpperBoundary(value.value());
    }

    bool isRangeLengthLessOrEqualThan(uint64_t length) const
    {
        if (!lower_boundary || !upper_boundary)
            return false;
        if (lower_boundary->getTypeName() == "UInt64")
        {
            uint64_t l = lower_boundary->get<uint64_t>();
            uint64_t r = upper_boundary->get<uint64_t>();
            if (l > r)
                return false;
            return r - l <= length;
        }
        else if (lower_boundary->getTypeName() == "Int64")
        {
            int64_t l = lower_boundary->get<int64_t>();
            int64_t r = upper_boundary->get<int64_t>();
            if (l > r)
                return false;
            return static_cast<uint64_t>(r) - static_cast<uint64_t>(l) <= length;
        }

        return false;
    }
};

using DataHints = std::unordered_map<std::string, DataHint>;

void updateDataHintsWithFilterActionsDAG(DataHints & hints, const ActionsDAG::Node & actions);

void updateDataHintsWithExpressionActionsDAG(DataHints & hints, const ActionsDAG & actions);

void intersectDataHints(DataHints & left_hints, const DataHints & right_hints);

void unionDataHints(DataHints & left_hints, const DataHints & right_hints);

void unionJoinDataHints(DataHints & left_hints, const DataHints & right_hints, const TableJoin & table_join);

void updateDataHintsWithOutputHeaderKeys(DataHints & hints, const Names & keys);

std::pair<Names, DataTypes> optimizeAggregatingStepWithDataHints(
        std::shared_ptr<AggregatingTransformParams> & transform_params,
        QueryPipelineBuilder & pipeline,
        const DataHints & hints,
        const ColumnsWithTypeAndName & input_header,
        const BuildQueryPipelineSettings & settings,
        const bool final);

void optimizeAggregatingStepWithDataHintsReturnInitialColumns(
    QueryPipelineBuilder & pipeline,
    const DataHints & hints,
    const Names & changed_keys,
    const DataTypes & changed_data_types,
    const BuildQueryPipelineSettings & settings);

}
