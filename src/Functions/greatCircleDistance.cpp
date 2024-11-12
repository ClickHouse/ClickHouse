#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PerformanceAdaptors.h>
#include <Interpreters/castColumn.h>
#include <Common/TargetSpecific.h>
#include <cmath>
#include <numbers>


namespace DB
{
namespace Setting
{
    extern const SettingsBool geo_distance_returns_float64_on_float64_arguments;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/** Calculates the distance between two geographical locations.
  * There are three variants:
  * greatCircleAngle: calculates the distance on a sphere in degrees: https://en.wikipedia.org/wiki/Great-circle_distance
  * greatCircleDistance: calculates the distance on a sphere in meters.
  * geoDistance: calculates the distance on WGS-84 ellipsoid in meters.
  *
  * The function calculates distance between two points on Earth specified by longitude and latitude in degrees.
  *
  * Latitude must be in [-90, 90], longitude must be [-180, 180].
  *
  * Original code of this implementation of this function is here:
  * https://github.com/sphinxsearch/sphinx/blob/409f2c2b5b2ff70b04e38f92b6b1a890326bad65/src/sphinxexpr.cpp#L3825.
  * Andrey Aksenov, the author of original code, permitted to use this code in ClickHouse under the Apache 2.0 license.
  * Presentation about this code from Highload++ Siberia 2019 is here https://github.com/ClickHouse/ClickHouse/files/3324740/1_._._GEODIST_._.pdf
  * The main idea of this implementation is optimizations based on Taylor series, trigonometric identity
  *  and calculated constants once for cosine, arcsine(sqrt) and look up table.
  */

namespace
{

enum class Method : uint8_t
{
    SPHERE_DEGREES,
    SPHERE_METERS,
    WGS84_METERS,
};

constexpr size_t ASIN_SQRT_LUT_SIZE = 512;
constexpr size_t COS_LUT_SIZE = 1024; // maxerr 0.00063%
constexpr size_t METRIC_LUT_SIZE = 1024;

/// Earth radius in meters using WGS84 authalic radius.
/// We use this value to be consistent with H3 library.
constexpr double EARTH_RADIUS = 6371007.180918475;
constexpr double EARTH_DIAMETER = 2.0 * EARTH_RADIUS;
constexpr double PI = std::numbers::pi_v<double>;

template <typename T>
T sqr(T v) { return v * v; }

template <typename T>
struct Impl
{
    T cos_lut[COS_LUT_SIZE + 1];       /// cos(x) table
    T asin_sqrt_lut[ASIN_SQRT_LUT_SIZE + 1]; /// asin(sqrt(x)) * earth_diameter table
    T sphere_metric_lut[METRIC_LUT_SIZE + 1]; /// sphere metric, unitless: the distance in degrees for one degree across longitude depending on latitude
    T sphere_metric_meters_lut[METRIC_LUT_SIZE + 1]; /// sphere metric: the distance in meters for one degree across longitude depending on latitude
    T wgs84_metric_meters_lut[2 * (METRIC_LUT_SIZE + 1)]; /// ellipsoid metric: the distance in meters across one degree latitude/longitude depending on latitude

    Impl()
    {
        for (size_t i = 0; i <= COS_LUT_SIZE; ++i)
            cos_lut[i] = T(std::cos(2 * PI * static_cast<double>(i) / COS_LUT_SIZE)); // [0, 2 * pi] -> [0, COS_LUT_SIZE]

        for (size_t i = 0; i <= ASIN_SQRT_LUT_SIZE; ++i)
            asin_sqrt_lut[i] = T(std::asin(std::sqrt(static_cast<double>(i) / ASIN_SQRT_LUT_SIZE))); // [0, 1] -> [0, ASIN_SQRT_LUT_SIZE]

        for (size_t i = 0; i <= METRIC_LUT_SIZE; ++i)
        {
            double latitude = i * (PI / METRIC_LUT_SIZE) - PI * 0.5; // [-pi / 2, pi / 2] -> [0, METRIC_LUT_SIZE]

            /// Squared metric coefficients (for the distance in meters) on a tangent plane, for latitude and longitude (in degrees),
            /// depending on the latitude (in radians).

            /// https://github.com/mapbox/cheap-ruler/blob/master/index.js#L67
            wgs84_metric_meters_lut[i * 2] = T(sqr(111132.09 - 566.05 * std::cos(2.0 * latitude) + 1.20 * std::cos(4.0 * latitude)));
            wgs84_metric_meters_lut[i * 2 + 1] = T(sqr(111415.13 * std::cos(latitude) - 94.55 * std::cos(3.0 * latitude) + 0.12 * std::cos(5.0 * latitude)));
            sphere_metric_meters_lut[i] = T(sqr((EARTH_DIAMETER * PI / 360) * std::cos(latitude)));

            sphere_metric_lut[i] = T(sqr(std::cos(latitude)));
        }
    }

    static NO_SANITIZE_UNDEFINED size_t toIndex(T x)
    {
        /// Implementation specific behaviour on overflow or infinite value.
        return static_cast<size_t>(x);
    }

    static T degDiff(T f)
    {
        f = std::abs(f);
        if (f > 180)
            f = 360 - f;
        return f;
    }

    T fastCos(T x)
    {
        T y = std::abs(x) * (T(COS_LUT_SIZE) / T(PI) / T(2.0));
        size_t i = toIndex(y);
        y -= i;
        i &= (COS_LUT_SIZE - 1);
        return cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y;
    }

    T fastSin(T x)
    {
        T y = std::abs(x) * (T(COS_LUT_SIZE) / T(PI) / T(2.0));
        size_t i = toIndex(y);
        y -= i;
        i = (i - COS_LUT_SIZE / 4) & (COS_LUT_SIZE - 1); // cos(x - pi / 2) = sin(x), costable / 4 = pi / 2
        return cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y;
    }

    /// fast implementation of asin(sqrt(x))
    /// max error in floats 0.00369%, in doubles 0.00072%
    T fastAsinSqrt(T x)
    {
        if (x < T(0.122))
        {
            // distance under 4546 km, Taylor error under 0.00072%
            T y = std::sqrt(x);
            return y + x * y * T(0.166666666666666) + x * x * y * T(0.075) + x * x * x * y * T(0.044642857142857);
        }
        if (x < T(0.948))
        {
            // distance under 17083 km, 512-entry LUT error under 0.00072%
            x *= ASIN_SQRT_LUT_SIZE;
            size_t i = toIndex(x);
            return asin_sqrt_lut[i] + (asin_sqrt_lut[i + 1] - asin_sqrt_lut[i]) * (x - i);
        }
        return std::asin(std::sqrt(x)); /// distance is over 17083 km, just compute exact
    }
};

template <typename T> Impl<T> impl;

DECLARE_MULTITARGET_CODE(

namespace
{

template <Method method, typename T>
T distance(T lon1deg, T lat1deg, T lon2deg, T lat2deg)
{
    T lat_diff = impl<T>.degDiff(lat1deg - lat2deg);
    T lon_diff = impl<T>.degDiff(lon1deg - lon2deg);

    if (lon_diff < 13)
    {
        // points are close enough; use flat ellipsoid model
        // interpolate metric coefficients using latitudes midpoint

        /// Why comparing only difference in longitude?
        /// If longitudes are different enough, there is a big difference between great circle line and a line with constant latitude.
        ///  (Remember how a plane flies from Amsterdam to New York)
        /// But if longitude is close but latitude is different enough, there is no difference between meridian and great circle line.

        T latitude_midpoint = (lat1deg + lat2deg + 180) * METRIC_LUT_SIZE / 360; // [-90, 90] degrees -> [0, METRIC_LUT_SIZE] indexes
        size_t latitude_midpoint_index = impl<T>.toIndex(latitude_midpoint) & (METRIC_LUT_SIZE - 1);

        /// This is linear interpolation between two table items at index "latitude_midpoint_index" and "latitude_midpoint_index + 1".

        T k_lat{};
        T k_lon{};

        if constexpr (method == Method::SPHERE_DEGREES)
        {
            k_lat = 1;

            k_lon = impl<T>.sphere_metric_lut[latitude_midpoint_index]
                + (impl<T>.sphere_metric_lut[latitude_midpoint_index + 1] - impl<T>.sphere_metric_lut[latitude_midpoint_index]) * (latitude_midpoint - latitude_midpoint_index);
        }
        else if constexpr (method == Method::SPHERE_METERS)
        {
            k_lat = sqr(T(EARTH_DIAMETER) * T(PI) / T(360.0));

            k_lon = impl<T>.sphere_metric_meters_lut[latitude_midpoint_index]
                + (impl<T>.sphere_metric_meters_lut[latitude_midpoint_index + 1] - impl<T>.sphere_metric_meters_lut[latitude_midpoint_index]) * (latitude_midpoint - latitude_midpoint_index);
        }
        else if constexpr (method == Method::WGS84_METERS)
        {
            k_lat = impl<T>.wgs84_metric_meters_lut[latitude_midpoint_index * 2]
                + (impl<T>.wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2] - impl<T>.wgs84_metric_meters_lut[latitude_midpoint_index * 2]) * (latitude_midpoint - latitude_midpoint_index);

            k_lon = impl<T>.wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1]
                + (impl<T>.wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2 + 1] - impl<T>.wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1]) * (latitude_midpoint - latitude_midpoint_index);
        }

        /// Metric on a tangent plane: it differs from Euclidean metric only by scale of coordinates.
        return std::sqrt(k_lat * lat_diff * lat_diff + k_lon * lon_diff * lon_diff);
    }
    else
    {
        /// Points are too far away: use Haversine.

        static constexpr T RAD_IN_DEG = T(PI / 180.0);
        static constexpr T RAD_IN_DEG_HALF = T(PI / 360.0);

        T a = sqr(impl<T>.fastSin(lat_diff * RAD_IN_DEG_HALF))
            + impl<T>.fastCos(lat1deg * RAD_IN_DEG) * impl<T>.fastCos(lat2deg * RAD_IN_DEG) * sqr(impl<T>.fastSin(lon_diff * RAD_IN_DEG_HALF));

        if constexpr (method == Method::SPHERE_DEGREES)
            return (T(360.0) / T(PI)) * impl<T>.fastAsinSqrt(a);
        else
            return T(EARTH_DIAMETER) * impl<T>.fastAsinSqrt(a);
    }
}

}

template <Method method>
class FunctionGeoDistance : public IFunction
{
public:
    explicit FunctionGeoDistance(ContextPtr context)
    {
        always_float32 = !context->getSettingsRef()[Setting::geo_distance_returns_float64_on_float64_arguments];
    }

private:
    bool always_float32;

    String getName() const override
    {
        if constexpr (method == Method::SPHERE_DEGREES)
            return "greatCircleAngle";
        if constexpr (method == Method::SPHERE_METERS)
            return "greatCircleDistance";
        else
            return "geoDistance";
    }

    size_t getNumberOfArguments() const override { return 4; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        bool has_float64 = false;

        for (size_t arg_idx = 0; arg_idx < 4; ++arg_idx)
        {
            WhichDataType which(arguments[arg_idx]);

            if (!isNumber(which))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument {} of function {}. "
                    "Must be numeric", arguments[arg_idx]->getName(), std::to_string(arg_idx + 1), getName());

            if (which.isFloat64())
                has_float64 = true;
        }

        if (has_float64 && !always_float32)
            return std::make_shared<DataTypeFloat64>();
        else
            return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool returns_float64 = WhichDataType(result_type).isFloat64();

        auto dst = result_type->createColumn();

        auto arguments_copy = arguments;
        for (auto & argument : arguments_copy)
        {
            argument.column = argument.column->convertToFullColumnIfConst();
            argument.column = castColumn(argument, result_type);
            argument.type = result_type;
        }

        if (returns_float64)
            run<Float64>(arguments_copy, dst, input_rows_count);
        else
            run<Float32>(arguments_copy, dst, input_rows_count);

        return dst;
    }

    template <typename T>
    void run(const ColumnsWithTypeAndName & arguments, MutableColumnPtr & dst, size_t input_rows_count) const
    {
        const auto * col_lon1 = convertArgumentColumn<T>(arguments, 0);
        const auto * col_lat1 = convertArgumentColumn<T>(arguments, 1);
        const auto * col_lon2 = convertArgumentColumn<T>(arguments, 2);
        const auto * col_lat2 = convertArgumentColumn<T>(arguments, 3);

        auto & dst_data = assert_cast<ColumnVector<T> &>(*dst).getData();
        dst_data.resize(input_rows_count);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            dst_data[row_num] = distance<method>(
                col_lon1->getData()[row_num], col_lat1->getData()[row_num],
                col_lon2->getData()[row_num], col_lat2->getData()[row_num]);
        }
    }

    template <typename T>
    const ColumnVector<T> * convertArgumentColumn(const ColumnsWithTypeAndName & arguments, size_t argument_index) const
    {
        const auto * column_typed = checkAndGetColumn<ColumnVector<T>>(arguments[argument_index].column.get());
        if (!column_typed)
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of argument {} of function {}. Must be {}.",
                    arguments[argument_index].type->getName(),
                    argument_index + 1,
                    getName(),
                    TypeName<T>);

        return column_typed;
    }
};

) // DECLARE_MULTITARGET_CODE

template <Method method>
class FunctionGeoDistance : public TargetSpecific::Default::FunctionGeoDistance<method>
{
public:
    explicit FunctionGeoDistance(ContextPtr context)
        : TargetSpecific::Default::FunctionGeoDistance<method>(context), selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionGeoDistance<method>>(context);

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX,
            TargetSpecific::AVX::FunctionGeoDistance<method>>(context);
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionGeoDistance<method>>(context);
        selector.registerImplementation<TargetArch::AVX512F,
            TargetSpecific::AVX512F::FunctionGeoDistance<method>>(context);
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionGeoDistance<method>>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

}

REGISTER_FUNCTION(GeoDistance)
{
    factory.registerFunction("greatCircleAngle", [](ContextPtr context) { return std::make_shared<FunctionGeoDistance<Method::SPHERE_DEGREES>>(std::move(context)); });
    factory.registerFunction("greatCircleDistance", [](ContextPtr context) { return std::make_shared<FunctionGeoDistance<Method::SPHERE_METERS>>(std::move(context)); });
    factory.registerFunction("geoDistance", [](ContextPtr context) { return std::make_shared<FunctionGeoDistance<Method::WGS84_METERS>>(std::move(context)); });
}

}
