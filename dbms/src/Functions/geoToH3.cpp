#include <array>
#include <math.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>


extern "C" {
#include <h3Index.h>
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/// Implements the function geoToH3 which takes 3 arguments (latitude, longitude and h3 resolution)
/// and returns h3 index of this point
class FunctionGeoToH3 : public IFunction
{
public:
    static constexpr auto name = "geoToH3";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGeoToH3>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto arg = arguments[0].get();
        if (!WhichDataType(arg).isFloat64())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arg = arguments[1].get();
        if (!WhichDataType(arg).isFloat64())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(2) + " of function " + getName() + ". Must be Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arg = arguments[2].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(3) + " of function " + getName() + ". Must be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        int const_cnt = 0;
        const auto size = input_rows_count;

        for (const auto idx : ext::range(0, 2))
        {
            const auto column = block.getByPosition(arguments[idx]).column.get();
            if (typeid_cast<const ColumnConst *>(column))
            {
                ++const_cnt;
            }
            else if (!typeid_cast<const ColumnVector<Float64> *>(column))
            {
                throw Exception(
                    "Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        double resolution = 0;
        bool is_const_resulution = false;
        {
            const auto column = block.getByPosition(arguments[2]).column.get();
            if (typeid_cast<const ColumnConst *>(column))
            {
                is_const_resulution = true;
                const auto col_const_res = static_cast<const ColumnConst *>(column);
                resolution = col_const_res->getValue<UInt8>();
            }
            else if (!typeid_cast<const ColumnVector<UInt8> *>(column))
            {
                throw Exception(
                    "Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
            }
            else if (const_cnt == 2)
            {
                throw Exception(
                    "Illegal type " + column->getName() + " of arguments 3 of function " + getName()
                        + ". It must be const if arguments 1 and 2 are consts.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }


        const auto col_lat = block.getByPosition(arguments[0]).column.get();
        const auto col_lon = block.getByPosition(arguments[1]).column.get();
        const auto col_res = block.getByPosition(arguments[2]).column.get();
        if (const_cnt == 0)
        {
            const auto col_vec_lat = static_cast<const ColumnVector<Float64> *>(col_lat);
            const auto col_vec_lon = static_cast<const ColumnVector<Float64> *>(col_lon);
            const auto col_vec_res = static_cast<const ColumnVector<UInt8> *>(col_res);

            auto dst = ColumnVector<UInt64>::create();
            auto & dst_data = dst->getData();
            dst_data.resize(size);

            for (const auto row : ext::range(0, size))
            {
                const double lat = col_vec_lat->getData()[row];
                const double lon = col_vec_lon->getData()[row];
                if (!is_const_resulution)
                {
                    resolution = col_vec_res->getData()[row];
                }

                GeoCoord coord;
                setGeoDegs(&coord, lat, lon);

                H3Index hindex = H3_EXPORT(geoToH3)(&coord, resolution);

                dst_data[row] = hindex;
            }

            block.getByPosition(result).column = std::move(dst);
        }
        else if (const_cnt == 2)
        {
            const auto col_const_lat = static_cast<const ColumnConst *>(col_lat);
            const auto col_const_lon = static_cast<const ColumnConst *>(col_lon);

            const double lat = col_const_lat->getValue<Float64>();
            const double lon = col_const_lon->getValue<Float64>();

            GeoCoord coord;
            setGeoDegs(&coord, lat, lon);
            H3Index hindex = H3_EXPORT(geoToH3)(&coord, resolution);

            block.getByPosition(result).column = DataTypeUInt64().createColumnConst(size, hindex);
        }
        else
        {
            throw Exception(
                "Illegal types " + col_lat->getName() + ", " + col_lon->getName() + " of arguments 1, 2 of function " + getName()
                    + ". All must be either const or vector",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
};


void registerFunctionGeoToH3(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGeoToH3>(FunctionFactory::CaseInsensitive);
}

}
