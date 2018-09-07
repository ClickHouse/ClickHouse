#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <ext/range.h>
#include <math.h>
#include <array>

#define DEGREES_IN_RADIANS (M_PI / 180.0)
#define EARTH_RADIUS_IN_METERS 6372797.560856


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

static inline Float64 degToRad(Float64 angle) { return angle * DEGREES_IN_RADIANS; }
static inline Float64 radToDeg(Float64 angle) { return angle / DEGREES_IN_RADIANS; }

/**
 *  The function calculates distance in meters between two points on Earth specified by longitude and latitude in degrees.
 *  The function uses great circle distance formula https://en.wikipedia.org/wiki/Great-circle_distance.
 *  Throws exception when one or several input values are not within reasonable bounds.
 *  Latitude must be in [-90, 90], longitude must be [-180, 180]
 *
 */
class FunctionGreatCircleDistance : public IFunction
{
public:

    static constexpr auto name = "greatCircleDistance";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGreatCircleDistance>(); }

private:

    enum class instr_type : uint8_t
    {
        get_float_64,
        get_const_float_64
    };

    using instr_t = std::pair<instr_type, const IColumn *>;
    using instrs_t = std::array<instr_t, 4>;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!WhichDataType(arg).isFloat64())
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeFloat64>();
    }

    instrs_t getInstructions(const Block & block, const ColumnNumbers & arguments, bool & out_const)
    {
        instrs_t result;
        out_const = true;

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto column = block.getByPosition(arguments[arg_idx]).column.get();

            if (const auto col = checkAndGetColumn<ColumnVector<Float64>>(column))
            {
                out_const = false;
                result[arg_idx] = instr_t{instr_type::get_float_64, col};
            }
            else if (const auto col = checkAndGetColumnConst<ColumnVector<Float64>>(column))
            {
                result[arg_idx] = instr_t{instr_type::get_const_float_64, col};
            }
            else
                throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return result;
    }

    /// https://en.wikipedia.org/wiki/Great-circle_distance
    Float64 greatCircleDistance(Float64 lon1Deg, Float64 lat1Deg, Float64 lon2Deg, Float64 lat2Deg)
    {
        if (lon1Deg < -180 || lon1Deg > 180 ||
            lon2Deg < -180 || lon2Deg > 180 ||
            lat1Deg < -90 || lat1Deg > 90 ||
            lat2Deg < -90 || lat2Deg > 90)
        {
            throw Exception("Arguments values out of bounds for function " + getName(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        Float64 lon1Rad = degToRad(lon1Deg);
        Float64 lat1Rad = degToRad(lat1Deg);
        Float64 lon2Rad = degToRad(lon2Deg);
        Float64 lat2Rad = degToRad(lat2Deg);
        Float64 u = sin((lat2Rad - lat1Rad) / 2);
        Float64 v = sin((lon2Rad - lon1Rad) / 2);
        return 2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(u * u + cos(lat1Rad) * cos(lat2Rad) * v * v));
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto size = input_rows_count;

        bool result_is_const{};
        auto instrs = getInstructions(block, arguments, result_is_const);

        if (result_is_const)
        {
            const auto & colLon1 = static_cast<const ColumnConst *>(block.getByPosition(arguments[0]).column.get())->getValue<Float64>();
            const auto & colLat1 = static_cast<const ColumnConst *>(block.getByPosition(arguments[1]).column.get())->getValue<Float64>();
            const auto & colLon2 = static_cast<const ColumnConst *>(block.getByPosition(arguments[2]).column.get())->getValue<Float64>();
            const auto & colLat2 = static_cast<const ColumnConst *>(block.getByPosition(arguments[3]).column.get())->getValue<Float64>();

            Float64 res = greatCircleDistance(colLon1, colLat1, colLon2, colLat2);
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(size, res);
        }
        else
        {
            auto dst = ColumnVector<Float64>::create();
            auto & dst_data = dst->getData();
            dst_data.resize(size);
            Float64 vals[instrs.size()];
            for (const auto row : ext::range(0, size))
            {
                for (const auto idx : ext::range(0, instrs.size()))
                {
                    if (instr_type::get_float_64 == instrs[idx].first)
                        vals[idx] = static_cast<const ColumnVector<Float64> *>(instrs[idx].second)->getData()[row];
                    else if (instr_type::get_const_float_64 == instrs[idx].first)
                        vals[idx] = static_cast<const ColumnConst *>(instrs[idx].second)->getValue<Float64>();
                    else
                        throw Exception{"Unknown instruction type in implementation of greatCircleDistance function", ErrorCodes::LOGICAL_ERROR};
                }
                dst_data[row] = greatCircleDistance(vals[0], vals[1], vals[2], vals[3]);
            }
            block.getByPosition(result).column = std::move(dst);
        }
    }
};


/**
 * The function checks if a point is in one of ellipses in set.
 * The number of arguments must be 2 + 4*N where N is the number of ellipses.
 * The arguments must be arranged as follows: (x, y, x_0, y_0, a_0, b_0, ..., x_i, y_i, a_i, b_i)
 * All ellipses parameters must be const values;
 *
 * The function first checks bounding box condition.
 * If a point is inside an ellipse's bounding box, the quadratic condition is evaluated.
 *
 * Here we assume that points in one block are close and are likely to fit in one ellipse,
 * so the last success ellipse index is remembered to check this ellipse first for next point.
 *
 */
class FunctionPointInEllipses : public IFunction
{
public:

    static constexpr auto name = "pointInEllipses";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionPointInEllipses>(); }

private:

    struct Ellipse
    {
        Float64 x;
        Float64 y;
        Float64 a;
        Float64 b;
    };

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 6 || arguments.size() % 4 != 2)
        {
            throw Exception(
                "Incorrect number of arguments of function " + getName() + ". Must be 2 for your point plus 4 * N for ellipses (x_i, y_i, a_i, b_i).");
        }

        /// For array on stack, see below.
        if (arguments.size() > 10000)
        {
            throw Exception(
                "Number of arguments of function " + getName() + " is too large.");
        }

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!WhichDataType(arg).isFloat64())
            {
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto size = input_rows_count;

        /// Prepare array of ellipses.
        size_t ellipses_count = (arguments.size() - 2) / 4;
        Ellipse ellipses[ellipses_count];

        for (const auto ellipse_idx : ext::range(0, ellipses_count))
        {
            Float64 ellipse_data[4];
            for (const auto idx : ext::range(0, 4))
            {
                int arg_idx = 2 + 4 * ellipse_idx + idx;
                const auto column = block.getByPosition(arguments[arg_idx]).column.get();
                if (const auto col = checkAndGetColumnConst<ColumnVector<Float64>>(column))
                {
                    ellipse_data[idx] = col->getValue<Float64>();
                }
                else
                {
                    throw Exception(
                        "Illegal type " + column->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be const Float64",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
            ellipses[ellipse_idx] = Ellipse{ellipse_data[0], ellipse_data[1], ellipse_data[2], ellipse_data[3]};
        }

        int const_cnt = 0;
        for (const auto idx : ext::range(0, 2))
        {
            const auto column = block.getByPosition(arguments[idx]).column.get();
            if (typeid_cast<const ColumnConst *> (column))
            {
                ++const_cnt;
            }
            else if (!typeid_cast<const ColumnVector<Float64> *> (column))
            {
                throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        const auto col_x = block.getByPosition(arguments[0]).column.get();
        const auto col_y = block.getByPosition(arguments[1]).column.get();
        if (const_cnt == 0)
        {
                const auto col_vec_x = static_cast<const ColumnVector<Float64> *> (col_x);
                const auto col_vec_y = static_cast<const ColumnVector<Float64> *> (col_y);

                auto dst = ColumnVector<UInt8>::create();
                auto & dst_data = dst->getData();
                dst_data.resize(size);

                size_t start_index = 0;
                for (const auto row : ext::range(0, size))
                {
                    dst_data[row] = isPointInEllipses(col_vec_x->getData()[row], col_vec_y->getData()[row], ellipses, ellipses_count, start_index);
                }

                block.getByPosition(result).column = std::move(dst);
            }
            else if (const_cnt == 2)
            {
                const auto col_const_x = static_cast<const ColumnConst *> (col_x);
                const auto col_const_y = static_cast<const ColumnConst *> (col_y);
                size_t start_index = 0;
                UInt8 res = isPointInEllipses(col_const_x->getValue<Float64>(), col_const_y->getValue<Float64>(), ellipses, ellipses_count, start_index);
                block.getByPosition(result).column = DataTypeUInt8().createColumnConst(size, UInt64(res));
            }
            else
            {
                throw Exception(
                    "Illegal types " + col_x->getName() + ", " + col_y->getName() + " of arguments 1, 2 of function " + getName() + ". Both must be either const or vector",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
    }

    static bool isPointInEllipses(Float64 x, Float64 y, const Ellipse * ellipses, size_t ellipses_count, size_t & start_index)
    {
        size_t index = 0 + start_index;
        for (size_t i = 0; i < ellipses_count; ++i)
        {
            Ellipse el = ellipses[index];
            double p1 = ((x - el.x) / el.a);
            double p2 = ((y - el.y) / el.b);
            if (x <= el.x + el.a && x >= el.x - el.a && y <= el.y + el.b && y >= el.y - el.b /// Bounding box check
                && p1 * p1 + p2 * p2 <= 1.0)    /// Precise check
            {
                start_index = index;
                return true;
            }
            ++index;
            if (index == ellipses_count)
            {
                index = 0;
            }
        }
        return false;
    }
};

}

#undef DEGREES_IN_RADIANS
