#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <ext/range.h>
#include <math.h>
#include <array>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{
const double PI = 3.14159265358979323846;
const float TO_RADF = static_cast<float>(PI / 180.0);
const float TO_RADF2 = static_cast<float>(PI / 360.0);

const int GEODIST_TABLE_COS = 1024; // maxerr 0.00063%
const int GEODIST_TABLE_ASIN = 512;
const int GEODIST_TABLE_K = 1024;

float g_GeoCos[GEODIST_TABLE_COS + 1];        /// cos(x) table
float g_GeoAsin[GEODIST_TABLE_ASIN + 1];    /// asin(sqrt(x)) table
float g_GeoFlatK[GEODIST_TABLE_K + 1][2];    /// geodistAdaptive() flat ellipsoid method k1,k2 coeffs table

inline double sqr(double v)
{
    return v * v;
}

inline float fsqr(float v)
{
    return v * v;
}

void geodistInit()
{
    for (size_t i = 0; i <= GEODIST_TABLE_COS; ++i)
        g_GeoCos[i] = static_cast<float>(cos(2 * PI * i / GEODIST_TABLE_COS)); // [0, 2pi] -> [0, COSTABLE]

    for (size_t i = 0; i <= GEODIST_TABLE_ASIN; ++i)
        g_GeoAsin[i] = static_cast<float>(asin(
                sqrt(static_cast<double>(i) / GEODIST_TABLE_ASIN))); // [0, 1] -> [0, ASINTABLE]

    for (size_t i = 0; i <= GEODIST_TABLE_K; ++i)
    {
        double x = PI * i / GEODIST_TABLE_K - PI * 0.5; // [-pi/2, pi/2] -> [0, KTABLE]
        g_GeoFlatK[i][0] = static_cast<float>(sqr(111132.09 - 566.05 * cos(2 * x) + 1.20 * cos(4 * x)));
        g_GeoFlatK[i][1] = static_cast<float>(sqr(111415.13 * cos(x) - 94.55 * cos(3 * x) + 0.12 * cos(5 * x)));
    }
}

inline float geodistDegDiff(float f)
{
    f = static_cast<float>(fabs(f));
    while (f > 360)
        f -= 360;
    if (f > 180)
        f = 360 - f;
    return f;
}

inline float geodistFastCos(float x)
{
    float y = static_cast<float>(fabs(x) * GEODIST_TABLE_COS / PI / 2);
    int i = static_cast<int>(y);
    y -= i;
    i &= (GEODIST_TABLE_COS - 1);
    return g_GeoCos[i] + (g_GeoCos[i + 1] - g_GeoCos[i]) * y;
}

inline float geodistFastSin(float x)
{
    float y = static_cast<float>(fabs(x) * GEODIST_TABLE_COS / PI / 2);
    int i = static_cast<int>(y);
    y -= i;
    i = (i - GEODIST_TABLE_COS / 4) & (GEODIST_TABLE_COS - 1); // cos(x-pi/2)=sin(x), costable/4=pi/2
    return g_GeoCos[i] + (g_GeoCos[i + 1] - g_GeoCos[i]) * y;
}


/// fast implementation of asin(sqrt(x))
/// max error in floats 0.00369%, in doubles 0.00072%
inline float geodistFastAsinSqrt(float x)
{
    if (x < 0.122)
    {
        // distance under 4546km, Taylor error under 0.00072%
        float y = static_cast<float>(sqrt(x));
        return y + x * y * 0.166666666666666f + x * x * y * 0.075f + x * x * x * y * 0.044642857142857f;
    }
    if (x < 0.948)
    {
        // distance under 17083km, 512-entry LUT error under 0.00072%
        x *= GEODIST_TABLE_ASIN;
        int i = static_cast<int>(x);
        return g_GeoAsin[i] + (g_GeoAsin[i + 1] - g_GeoAsin[i]) * (x - i);
    }
    return static_cast<float>(asin(sqrt(x))); // distance over 17083km, just compute honestly
}
}
/**
 *  The function calculates distance in meters between two points on Earth specified by longitude and latitude in degrees.
 *  The function uses great circle distance formula https://en.wikipedia.org/wiki/Great-circle_distance .
 *  Throws exception when one or several input values are not within reasonable bounds.
 *  Latitude must be in [-90, 90], longitude must be [-180, 180].
 *  Original code of this implementation of this function is here https://github.com/sphinxsearch/sphinx/blob/409f2c2b5b2ff70b04e38f92b6b1a890326bad65/src/sphinxexpr.cpp#L3825.
 *  Andrey Aksenov, the author of original code, permitted to use this code in ClickHouse under the Apache 2.0 license.
 *  Presentation about this code from Highload++ Siberia 2019 is here https://github.com/ClickHouse/ClickHouse/files/3324740/1_._._GEODIST_._.pdf
 *  The main idea of this implementation is optimisations based on Taylor series, trigonometric identity and calculated constants once for cosine, arcsine(sqrt) and look up table.
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
            else if (const auto col_const = checkAndGetColumnConst<ColumnVector<Float64>>(column))
            {
                result[arg_idx] = instr_t{instr_type::get_const_float_64, col_const};
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
            throw Exception("Arguments values out of bounds for function " + getName(),
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        float dlat = geodistDegDiff(lat1Deg - lat2Deg);
        float dlon = geodistDegDiff(lon1Deg - lon2Deg);

        if (dlon < 13)
        {
            // points are close enough; use flat ellipsoid model
            // interpolate sqr(k1), sqr(k2) coefficients using latitudes midpoint
            float m = (lat1Deg + lat2Deg + 180) * GEODIST_TABLE_K / 360; // [-90, 90] degrees -> [0, KTABLE] indexes
            int i = static_cast<int>(m);
            i &= (GEODIST_TABLE_K - 1);
            float kk1 = g_GeoFlatK[i][0] + (g_GeoFlatK[i + 1][0] - g_GeoFlatK[i][0]) * (m - i);
            float kk2 = g_GeoFlatK[i][1] + (g_GeoFlatK[i + 1][1] - g_GeoFlatK[i][1]) * (m - i);
            return static_cast<float>(sqrt(kk1 * dlat * dlat + kk2 * dlon * dlon));
        }
        // points too far away; use haversine
        static const float D = 2 * 6371000;
        float a = fsqr(geodistFastSin(dlat * TO_RADF2)) +
                  geodistFastCos(lat1Deg * TO_RADF) * geodistFastCos(lat2Deg * TO_RADF) *
                  fsqr(geodistFastSin(dlon * TO_RADF2));
        return static_cast<float>(D * geodistFastAsinSqrt(a));
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto size = input_rows_count;

        bool result_is_const{};
        auto instrs = getInstructions(block, arguments, result_is_const);

        if (result_is_const)
        {
            const auto & colLon1 = assert_cast<const ColumnConst *>(block.getByPosition(arguments[0]).column.get())->getValue<Float64>();
            const auto & colLat1 = assert_cast<const ColumnConst *>(block.getByPosition(arguments[1]).column.get())->getValue<Float64>();
            const auto & colLon2 = assert_cast<const ColumnConst *>(block.getByPosition(arguments[2]).column.get())->getValue<Float64>();
            const auto & colLat2 = assert_cast<const ColumnConst *>(block.getByPosition(arguments[3]).column.get())->getValue<Float64>();

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
                        vals[idx] = assert_cast<const ColumnVector<Float64> *>(instrs[idx].second)->getData()[row];
                    else if (instr_type::get_const_float_64 == instrs[idx].first)
                        vals[idx] = assert_cast<const ColumnConst *>(instrs[idx].second)->getValue<Float64>();
                    else
                        throw Exception{"Unknown instruction type in implementation of greatCircleDistance function", ErrorCodes::LOGICAL_ERROR};
                }
                dst_data[row] = greatCircleDistance(vals[0], vals[1], vals[2], vals[3]);
            }
            block.getByPosition(result).column = std::move(dst);
        }
    }
};


void registerFunctionGreatCircleDistance(FunctionFactory & factory)
{
    geodistInit();
    factory.registerFunction<FunctionGreatCircleDistance>();
}

}

