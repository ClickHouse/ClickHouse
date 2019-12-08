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

/** https://en.wikipedia.org/wiki/Great-circle_distance
 *
 *  The function calculates distance in meters between two points on Earth specified by longitude and latitude in degrees.
 *  The function uses great circle distance formula https://en.wikipedia.org/wiki/Great-circle_distance .
 *  Throws exception when one or several input values are not within reasonable bounds.
 *  Latitude must be in [-90, 90], longitude must be [-180, 180].
 *  Original code of this implementation of this function is here https://github.com/sphinxsearch/sphinx/blob/409f2c2b5b2ff70b04e38f92b6b1a890326bad65/src/sphinxexpr.cpp#L3825.
 *  Andrey Aksenov, the author of original code, permitted to use this code in ClickHouse under the Apache 2.0 license.
 *  Presentation about this code from Highload++ Siberia 2019 is here https://github.com/ClickHouse/ClickHouse/files/3324740/1_._._GEODIST_._.pdf
 *  The main idea of this implementation is optimisations based on Taylor series, trigonometric identity and calculated constants once for cosine, arcsine(sqrt) and look up table.
 */

namespace
{

constexpr double PI = 3.14159265358979323846;
constexpr float TO_RADF = static_cast<float>(PI / 180.0);
constexpr float TO_RADF2 = static_cast<float>(PI / 360.0);

constexpr size_t GEODIST_TABLE_COS = 1024; // maxerr 0.00063%
constexpr size_t GEODIST_TABLE_ASIN = 512;
constexpr size_t GEODIST_TABLE_K = 1024;

float g_GeoCos[GEODIST_TABLE_COS + 1];        /// cos(x) table
float g_GeoAsin[GEODIST_TABLE_ASIN + 1];    /// asin(sqrt(x)) table
float g_GeoFlatK[GEODIST_TABLE_K + 1][2];    /// geodistAdaptive() flat ellipsoid method k1, k2 coeffs table

inline double sqr(double v)
{
    return v * v;
}

inline float sqrf(float v)
{
    return v * v;
}

void geodistInit()
{
    for (size_t i = 0; i <= GEODIST_TABLE_COS; ++i)
        g_GeoCos[i] = static_cast<float>(cos(2 * PI * i / GEODIST_TABLE_COS)); // [0, 2 * pi] -> [0, COSTABLE]

    for (size_t i = 0; i <= GEODIST_TABLE_ASIN; ++i)
        g_GeoAsin[i] = static_cast<float>(asin(
                sqrt(static_cast<double>(i) / GEODIST_TABLE_ASIN))); // [0, 1] -> [0, ASINTABLE]

    for (size_t i = 0; i <= GEODIST_TABLE_K; ++i)
    {
        double x = i * (PI / GEODIST_TABLE_K) - PI * 0.5; // [-pi / 2, pi / 2] -> [0, KTABLE]
        g_GeoFlatK[i][0] = static_cast<float>(sqr(111132.09 - 566.05 * cos(2 * x) + 1.20 * cos(4 * x)));
        g_GeoFlatK[i][1] = static_cast<float>(sqr(111415.13 * cos(x) - 94.55 * cos(3 * x) + 0.12 * cos(5 * x)));
    }
}

inline float geodistDegDiff(float f)
{
    f = fabsf(f);
    while (f > 360)
        f -= 360;
    if (f > 180)
        f = 360 - f;
    return f;
}

inline float geodistFastCos(float x)
{
    float y = fabsf(x) * (GEODIST_TABLE_COS / PI / 2);
    int i = static_cast<int>(y);
    y -= i;
    i &= (GEODIST_TABLE_COS - 1);
    return g_GeoCos[i] + (g_GeoCos[i + 1] - g_GeoCos[i]) * y;
}

inline float geodistFastSin(float x)
{
    float y = fabsf(x) * (GEODIST_TABLE_COS / PI / 2);
    int i = static_cast<int>(y);
    y -= i;
    i = (i - GEODIST_TABLE_COS / 4) & (GEODIST_TABLE_COS - 1); // cos(x - pi / 2) = sin(x), costable / 4 = pi / 2
    return g_GeoCos[i] + (g_GeoCos[i + 1] - g_GeoCos[i]) * y;
}

/// fast implementation of asin(sqrt(x))
/// max error in floats 0.00369%, in doubles 0.00072%
inline float geodistFastAsinSqrt(float x)
{
    if (x < 0.122f)
    {
        // distance under 4546km, Taylor error under 0.00072%
        float y = sqrtf(x);
        return y + x * y * 0.166666666666666f + x * x * y * 0.075f + x * x * x * y * 0.044642857142857f;
    }
    if (x < 0.948f)
    {
        // distance under 17083km, 512-entry LUT error under 0.00072%
        x *= GEODIST_TABLE_ASIN;
        int i = static_cast<int>(x);
        return g_GeoAsin[i] + (g_GeoAsin[i + 1] - g_GeoAsin[i]) * (x - i);
    }
    return asinf(sqrtf(x)); // distance over 17083km, just compute honestly
}

}


class FunctionGreatCircleDistance : public IFunction
{
public:
    static constexpr auto name = "greatCircleDistance";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGreatCircleDistance>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!WhichDataType(arg).isFloat())
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeFloat32>();
    }

    Float32 greatCircleDistance(Float32 lon1deg, Float32 lat1deg, Float32 lon2deg, Float32 lat2deg)
    {
        float lat_diff = geodistDegDiff(lat1deg - lat2deg);
        float lon_diff = geodistDegDiff(lon1deg - lon2deg);

        if (lon_diff < 13)
        {
            // points are close enough; use flat ellipsoid model
            // interpolate sqr(k1), sqr(k2) coefficients using latitudes midpoint
            float m = (lat1deg + lat2deg + 180) * GEODIST_TABLE_K / 360; // [-90, 90] degrees -> [0, KTABLE] indexes
            size_t i = static_cast<size_t>(m) & (GEODIST_TABLE_K - 1);
            float kk1 = g_GeoFlatK[i][0] + (g_GeoFlatK[i + 1][0] - g_GeoFlatK[i][0]) * (m - i);
            float kk2 = g_GeoFlatK[i][1] + (g_GeoFlatK[i + 1][1] - g_GeoFlatK[i][1]) * (m - i);
            return sqrtf(kk1 * lat_diff * lat_diff + kk2 * lon_diff * lon_diff);
        }
        else
        {
            // points too far away; use haversine
            static const float d = 2 * 6371000;
            float a = sqrf(geodistFastSin(lat_diff * TO_RADF2)) +
                geodistFastCos(lat1deg * TO_RADF) * geodistFastCos(lat2deg * TO_RADF) *
                sqrf(geodistFastSin(lon_diff * TO_RADF2));
            return d * geodistFastAsinSqrt(a);
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto dst = ColumnVector<Float32>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        const IColumn & col_lon1 = *block.getByPosition(arguments[0]).column;
        const IColumn & col_lat1 = *block.getByPosition(arguments[1]).column;
        const IColumn & col_lon2 = *block.getByPosition(arguments[2]).column;
        const IColumn & col_lat2 = *block.getByPosition(arguments[3]).column;

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            dst_data[row_num] = greatCircleDistance(
                col_lon1.getFloat32(row_num), col_lat1.getFloat32(row_num),
                col_lon2.getFloat32(row_num), col_lat2.getFloat32(row_num));

        block.getByPosition(result).column = std::move(dst);
    }
};


void registerFunctionGreatCircleDistance(FunctionFactory & factory)
{
    geodistInit();
    factory.registerFunction<FunctionGreatCircleDistance>();
}

}

