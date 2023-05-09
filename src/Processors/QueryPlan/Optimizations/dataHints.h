#pragma once

#include <limits>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>
#include <Core/Field.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Types.h>

namespace DB
{

namespace
{

template<typename T>
bool plusWithOverflowCheck(const Field & f1, const Field & f2, T & result)
{
    if (f1.getTypeName() == "UInt64")
    {
        if (f2.getTypeName() == "UInt64")
            return __builtin_add_overflow(f1.get<uint64_t>(), f2.get<uint64_t>(), &result);
        else
            return __builtin_add_overflow(f1.get<uint64_t>(), f2.get<int64_t>(), &result);
    }
    else
    {
        if (f2.getTypeName() == "UInt64")
            return __builtin_add_overflow(f1.get<int64_t>(), f2.get<uint64_t>(), &result);
        else
            return __builtin_add_overflow(f1.get<int64_t>(), f2.get<int64_t>(), &result);
    }
}

template<typename T>
bool minusWithOverflowCheck(const Field & f1, const Field & f2, T & result)
{
    if (f1.getTypeName() == "UInt64")
    {
        if (f2.getTypeName() == "UInt64")
            return __builtin_sub_overflow(f1.get<uint64_t>(), f2.get<uint64_t>(), &result);
        else
            return __builtin_sub_overflow(f1.get<uint64_t>(), f2.get<int64_t>(), &result);
    }
    else
    {
        if (f2.getTypeName() == "UInt64")
            return __builtin_sub_overflow(f1.get<int64_t>(), f2.get<uint64_t>(), &result);
        else
            return __builtin_sub_overflow(f1.get<int64_t>(), f2.get<int64_t>(), &result);
    }
}

template<typename T>
bool multiplyWithOverflowCheck(const Field & f1, const Field & f2, T & result)
{
    if (f1.getTypeName() == "UInt64")
    {
        if (f2.getTypeName() == "UInt64")
            return __builtin_mul_overflow(f1.get<uint64_t>(), f2.get<uint64_t>(), &result);
        else
            return __builtin_mul_overflow(f1.get<uint64_t>(), f2.get<int64_t>(), &result);
    }
    else
    {
        if (f2.getTypeName() == "UInt64")
            return __builtin_mul_overflow(f1.get<int64_t>(), f2.get<uint64_t>(), &result);
        else
            return __builtin_mul_overflow(f1.get<int64_t>(), f2.get<int64_t>(), &result);
    }
}

void fieldPlusConst(std::optional<Field> & field, const Field & value, bool is_column_unsigned)
{
    if (!field)
        return;

    if (is_column_unsigned)
    {
        uint64_t res;
        if (!plusWithOverflowCheck(field.value(), value, res))
            field = res;
        else
            field = std::nullopt;
    }
    else
    {
        int64_t res;
        if (!plusWithOverflowCheck(field.value(), value, res))
            field = res;
        else
            field = std::nullopt;
    }
}

void fieldMinusConst(std::optional<Field> & field, const Field & value, bool is_column_unsigned)
{
    if (!field)
        return;

    if (is_column_unsigned)
    {
        uint64_t res;
        if (!minusWithOverflowCheck(field.value(), value, res))
            field = res;
        else
            field = std::nullopt;
    }
    else
    {
        int64_t res;
        if (!minusWithOverflowCheck(field.value(), value, res))
            field = res;
        else
            field = std::nullopt;
    }
}

void fieldMultiplyConst(std::optional<Field> & field, const Field & value, bool is_column_unsigned)
{
    if (!field)
        return;

    if (is_column_unsigned)
    {
        uint64_t res;
        if (!multiplyWithOverflowCheck(field.value(), value, res))
            field = res;
        else
            field = std::nullopt;
    }
    else
    {
        int64_t res;
        if (!multiplyWithOverflowCheck(field.value(), value, res))
            field = res;
        else
            field = std::nullopt;
    }
}

}

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

    void plusConst(const Field & value)
    {
        fieldPlusConst(lower_boundary, value, is_column_unsigned);
        fieldPlusConst(upper_boundary, value, is_column_unsigned);
    }

    void minusConst(const Field & value)
    {
        fieldMinusConst(lower_boundary, value, is_column_unsigned);
        fieldMinusConst(upper_boundary, value, is_column_unsigned);
    }

    void multiplyConst(const Field & value)
    {
        if (value.getTypeName() == "Int64" && value.get<int64_t>() < 0)
            std::swap(lower_boundary, upper_boundary);

        fieldMultiplyConst(lower_boundary, value, is_column_unsigned);
        fieldMultiplyConst(upper_boundary, value, is_column_unsigned);
    }

    void unionLowerBoundary(const std::optional<Field> & value)
    {
        if (!lower_boundary)
            return;
        if (!value)
        {
            lower_boundary = std::nullopt;
            return;
        }
        if (value.value() < lower_boundary.value())
            setLowerBoundary(value.value());
    }

    void unionUpperBoundary(const std::optional<Field> & value)
    {
        if (!upper_boundary)
            return;
        if (!value)
        {
            upper_boundary = std::nullopt;
            return;
        }
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

    bool isEmpty() const { return !lower_boundary.has_value() && !upper_boundary.has_value(); }

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

}
