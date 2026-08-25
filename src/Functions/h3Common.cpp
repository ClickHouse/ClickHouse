#include <Functions/h3Common.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#if USE_H3

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int INCORRECT_DATA;
}
namespace Setting
{
    extern const SettingsBool functions_h3_default_if_invalid;
}

LatLng h3LatLngFromDegrees(Float64 longitude_degrees, Float64 latitude_degrees, std::string_view function_name)
{
    /// Written as a negated range check so that a coordinate that is not a number is rejected as well.
    if (!(longitude_degrees >= -180.0 && longitude_degrees <= 180.0)
        || !(latitude_degrees >= -90.0 && latitude_degrees <= 90.0))
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "The geometry argument of function {} has a vertex out of bounds: longitude {}, latitude {}. "
            "Longitude must be in [-180, 180] and latitude in [-90, 90] degrees",
            function_name,
            longitude_degrees,
            latitude_degrees);

    /// The same expression the geometry converters use, rather than `degsToRads`, which rounds differently.
    LatLng result;
    result.lng = longitude_degrees * M_PI / 180.0;
    result.lat = latitude_degrees * M_PI / 180.0;
    return result;
}

H3Validator::H3Validator(const ContextPtr & context)
    : throw_on_error(!context->getSettingsRef()[Setting::functions_h3_default_if_invalid])
{}

bool H3Validator::validateCell(UInt64 h) const
{
    if (!isValidCell(h))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid H3 cell index: {}", h);
        else
            return false;
    }
    return true;
}

bool H3Validator::validateEdge(UInt64 h) const
{
    if (!isValidDirectedEdge(h))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid H3 directed edge index: {}", h);
        else
            return false;
    }
    return true;
}

}

#endif
