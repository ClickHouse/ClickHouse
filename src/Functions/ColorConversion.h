#pragma once

#include <base/types.h>
#include <array>


namespace DB
{

namespace ColorConversion
{
    /// Conversion constants for conversion between OKLCH and sRGB
    ///
    /// Source: Björn Ottosson, “OkLab – A perceptual colour space”, https://bottosson.github.io/posts/oklab/

    constexpr Float64 epsilon = 1e-6;           /// epsilon used when Chroma is approximately 0; below this value we set Chroma = Hue = 0.
    constexpr Float64 rad2deg = 57.2957795131;  /// 180 / pi (radians -> degrees)
    constexpr Float64 deg2rad = 0.01745329251;  /// pi / 180 (degrees -> radians)
    constexpr Float64 gamma_fallback = 1e-6;    /// substituted when the caller passes gamma = 0 so that 1 / gamma in gamma transfer function stays finite
    constexpr Float64 default_gamma = 2.2;      /// gamma defaults at 2.2 if not provided
    constexpr size_t channels = 3;              /// number of colour channels (always 3).

    using Color = std::array<Float64, channels>;
    using Mat3x3 = std::array<Float64, 9>;

    /// These 3 x 3 matrices are copied verbatim from the reference code from the source.
    ///
    ///  The forward pipeline implemented in srgbToOklchBase() and its inverse oklchToSrgbBase().
    ///     sRGB - linear-RGB - LMS (cube) - OkLab - OKLCH
    ///
    /// Each matrix converts from the space in its name to the next stage in the pipeline:

    /// linear-RGB -> LMS
    constexpr Mat3x3 linear_to_lms_base = {0.4122214708, 0.5363325363, 0.0514459929,
                                           0.2119034982, 0.6806995451, 0.1073969566,
                                           0.0883024619, 0.2817188376, 0.6299787005};
    /// LMS -> OKLab
    constexpr Mat3x3 lms_to_oklab_base = {0.2104542553,  0.7936177850, -0.0040720468,
                                          1.9779984951, -2.4285922050,  0.45059370996,
                                          0.0259040371,  0.7827717662, -0.8086757660};
    /// OKLab -> LMS
    constexpr Mat3x3 oklab_to_lms_base = {1,  0.3963377774,  0.2158037573,
                                          1, -0.1055613458, -0.0638541728,
                                          1, -0.0894841775, -1.2914855480};
    /// LMS -> linear-RGB
    constexpr Mat3x3 lms_to_linear_base = {4.0767416621, -3.3077115913,  0.2309699292,
                                           -1.2684380046,  2.6097574011, -0.3413193965,
                                           -0.0041960863, -0.7034186147,  1.7076147010};
}

}
