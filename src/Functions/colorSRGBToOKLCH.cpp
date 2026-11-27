#include <Functions/ColorConversion.h>

namespace DB
{

/** Function that converts color from sRGB color space to perceptual OKLCH color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
namespace
{
class FunctionColorSRGBToOKLCH : public ColorConversionFromSRGBBase<FunctionColorSRGBToOKLCH>
{
public:
    static constexpr auto name = "colorSRGBToOKLCH";

    explicit FunctionColorSRGBToOKLCH(ContextPtr context_)
        : ColorConversionFromSRGBBase(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorSRGBToOKLCH>(context_); }

    String getName() const override { return name; }

    /// sRGB -> OKLCH conversion. Follows the step-by-step pipeline described in Ottosson's article. See ColorConversion.h
    ColorConversion::Color convertFromSrgb(const ColorConversion::Color & rgb, Float64 gamma) const
    {
        /// Step 1-3: sRGB to OKLab (handled by ColorConversion namespace helper)
        auto oklab = ColorConversion::srgbToOklab(rgb, gamma);

        /// Step 4: OKLab to OKLCH (Cartesian to cylindrical)
        auto oklch = oklab;

        Float64 a = oklab[1];
        Float64 b = oklab[2];

        oklch[1] = std::sqrt(a * a + b * b);
        if (oklch[1] >= ColorConversion::epsilon)
        {
            Float64 hue_degrees = std::atan2(b, a) * ColorConversion::rad2deg;
            oklch[2]  = std::fmod(hue_degrees + 360.0, 360.0);
        }
        else
        {
            oklch[1] = 0;
            oklch[2] = 0;
        }

        return oklch;
    }
};
}

REGISTER_FUNCTION(ColorSRGBToOKLCH)
{
    FunctionDocumentation::Description description = R"(
        Converts a colour encoded in the **sRGB** colour space to the perceptually uniform **OKLCH** colour space.

        If any input channel is outside `[0...255]` or the gamma value is non-positive, the behaviour is implementation-defined.

        :::note
        **OKLCH** is a cylindrical version of the OKLab colour space.
        It's three coordinates are `L` (the lightness in the range `[0...1]`), `C` (chroma `>= 0`) and `H` (the hue in degrees from `[0...360]`).
        OKLab/OKLCH is designed to be perceptually uniform while remaining cheap to compute.
        :::

        The conversion consists of three stages:
        1) sRGB to Linear sRGB
        2) Linear sRGB to OKLab
        3) OKLab to OKLCH.

        For references of colors in the OKLCH space, and how they correspond to sRGB colors, please see [https://OKLCH.com/](https://OKLCH.com/).
    )";
    FunctionDocumentation::Syntax syntax = "colorSRGBToOKLCH(tuple[, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "Tuple of three values R, G, B in the range `[0...255]`.", {"Tuple(UInt8, UInt8, UInt8)"}},
        {"gamma", "Optional. Exponent that is used to linearize sRGB by applying `(x / 255)^gamma` to each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (L, C, H) representing the OKLCH color space values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Convert sRGB to OKLCH",
        R"(
SELECT colorSRGBToOKLCH((128, 64, 32), 2.2) AS lch;
                    )",
        R"(
┌─lch───────────────────────────────────────────────────────┐
│ (0.4436238384931984,0.1044269954567863,45.90734548193018) │
└───────────────────────────────────────────────────────────┘
                    )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorSRGBToOKLCH>(documentation);
}

}
