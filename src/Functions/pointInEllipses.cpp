#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <common/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/**
 * The function checks if a point is in one of ellipses in set.
 * The number of arguments must be 2 + 4*N where N is the number of ellipses.
 * The arguments must be arranged as follows: (x, y, x_0, y_0, a_0, b_0, ..., x_i, y_i, a_i, b_i)
 * All ellipses parameters must be const values;
 *
 * The function first checks bounding box condition.
 * If a point is inside an ellipse's bounding box, the quadratic condition is evaluated.
 *
 * Here we assume that points in one columns are close and are likely to fit in one ellipse,
 * so the last success ellipse index is remembered to check this ellipse first for next point.
 *
 */
class FunctionPointInEllipses : public IFunction
{
public:
    static constexpr auto name = "pointInEllipses";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPointInEllipses>(); }

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
                "Incorrect number of arguments of function " + getName() + ". Must be 2 for your point plus 4 * N for ellipses (x_i, y_i, a_i, b_i).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        /// For array on stack, see below.
        if (arguments.size() > 10000)
        {
            throw Exception(
                "Number of arguments of function " + getName() + " is too large.", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }

        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!WhichDataType(arg).isFloat64())
            {
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto size = input_rows_count;

        /// Prepare array of ellipses.
        size_t ellipses_count = (arguments.size() - 2) / 4;
        std::vector<Ellipse> ellipses(ellipses_count);

        for (const auto ellipse_idx : collections::range(0, ellipses_count))
        {
            Float64 ellipse_data[4];
            for (const auto idx : collections::range(0, 4))
            {
                int arg_idx = 2 + 4 * ellipse_idx + idx;
                const auto * column = arguments[arg_idx].column.get();
                if (const auto * col = checkAndGetColumnConst<ColumnVector<Float64>>(column))
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
        for (const auto idx : collections::range(0, 2))
        {
            const auto * column = arguments[idx].column.get();
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

        const auto * col_x = arguments[0].column.get();
        const auto * col_y = arguments[1].column.get();
        if (const_cnt == 0)
        {
                const auto * col_vec_x = assert_cast<const ColumnVector<Float64> *> (col_x);
                const auto * col_vec_y = assert_cast<const ColumnVector<Float64> *> (col_y);

                auto dst = ColumnVector<UInt8>::create();
                auto & dst_data = dst->getData();
                dst_data.resize(size);

                size_t start_index = 0;
                for (const auto row : collections::range(0, size))
                {
                    dst_data[row] = isPointInEllipses(col_vec_x->getData()[row], col_vec_y->getData()[row], ellipses.data(), ellipses_count, start_index);
                }

                return dst;
            }
            else if (const_cnt == 2)
            {
                const auto * col_const_x = assert_cast<const ColumnConst *> (col_x);
                const auto * col_const_y = assert_cast<const ColumnConst *> (col_y);
                size_t start_index = 0;
                UInt8 res = isPointInEllipses(col_const_x->getValue<Float64>(), col_const_y->getValue<Float64>(), ellipses.data(), ellipses_count, start_index);
                return DataTypeUInt8().createColumnConst(size, res);
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

void registerFunctionPointInEllipses(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPointInEllipses>();
}

}
