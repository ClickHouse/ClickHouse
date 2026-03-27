#pragma once

#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

/// For a ColumnConst, return its inner 1-row data column (no copy).
/// For a regular column, return it as-is.
inline ColumnPtr getUnderlyingColumnData(const ColumnPtr & col)
{
    if (const auto * col_const = typeid_cast<const ColumnConst *>(col.get()))
        return col_const->getDataColumnPtr();
    return col;
}

/// Helper for optimizing spherical geometry predicate functions (ST_Within, ST_Contains, etc.)
/// when one or both arguments are constant.
///
/// When a geometry argument is constant, this helper:
///  1. Extracts the inner 1-row data column from ColumnConst (avoiding the O(N) replicate
///     that convertToFullColumnIfConst() would do for large row counts).
///  2. Converts and corrects the constant geometry only once, instead of N times.
///
/// BinaryPredicate must be a callable: bool(const LeftGeom &, const RightGeom &)
template <
    typename Point,
    typename LeftConverter,
    typename RightConverter,
    bool left_is_point,
    bool right_is_point,
    typename BinaryPredicate>
void executeGeometryPredicate(
    const ColumnsWithTypeAndName & arguments,
    PaddedPODArray<UInt8> & res_data,
    size_t input_rows_count,
    bool left_is_const,
    bool right_is_const,
    BinaryPredicate && predicate)
{
    if (left_is_const && right_is_const)
    {
        /// Both arguments constant — compute once and fill.
        auto first = LeftConverter::convert(getUnderlyingColumnData(arguments[0].column));
        auto second = RightConverter::convert(getUnderlyingColumnData(arguments[1].column));

        if constexpr (!left_is_point)
            boost::geometry::correct(first[0]);
        if constexpr (!right_is_point)
            boost::geometry::correct(second[0]);

        UInt8 result = predicate(first[0], second[0]);
        res_data.resize_fill(input_rows_count, result);
    }
    else if (right_is_const)
    {
        /// Right (e.g. containing geometry) is constant — convert and correct once.
        auto second = RightConverter::convert(getUnderlyingColumnData(arguments[1].column));
        if constexpr (!right_is_point)
            boost::geometry::correct(second[0]);

        auto first = LeftConverter::convert(arguments[0].column);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if constexpr (!left_is_point)
                boost::geometry::correct(first[i]);
            res_data.emplace_back(predicate(first[i], second[0]));
        }
    }
    else if (left_is_const)
    {
        /// Left (e.g. inner geometry) is constant — convert and correct once.
        auto first = LeftConverter::convert(getUnderlyingColumnData(arguments[0].column));
        if constexpr (!left_is_point)
            boost::geometry::correct(first[0]);

        auto second = RightConverter::convert(arguments[1].column);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if constexpr (!right_is_point)
                boost::geometry::correct(second[i]);
            res_data.emplace_back(predicate(first[0], second[i]));
        }
    }
    else
    {
        /// Neither argument is constant — original path.
        auto first = LeftConverter::convert(arguments[0].column);
        auto second = RightConverter::convert(arguments[1].column);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if constexpr (!left_is_point)
                boost::geometry::correct(first[i]);
            if constexpr (!right_is_point)
                boost::geometry::correct(second[i]);
            res_data.emplace_back(predicate(first[i], second[i]));
        }
    }
}

}
