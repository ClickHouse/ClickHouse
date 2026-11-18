#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/ColorConversion.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ITupleFunction.h>

namespace DB
{

/** Function that converts color from OKLCH perceptual color space to sRGB color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
namespace
{
class FunctionColorOKLCHToSRGB : public ColorConversionToSRGBBase<FunctionColorOKLCHToSRGB>
{
public:
    static constexpr auto name = "colorOKLCHToSRGB";

    explicit FunctionColorOKLCHToSRGB(ContextPtr context_)
        : ColorConversionToSRGBBase(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorOKLCHToSRGB>(context_); }

    String getName() const override { return name; }

    /// OKLCH -> sRGB conversion. Follows the step-by-step pipeline described in Ottosson's article. See ColorConversion.h
    ColorConversion::Color convertToSrgb(const ColorConversion::Color & oklch, Float64 gamma) const
    {
        /// OKLCH to OKLab (cylindrical to Cartesian)
        Float64 chroma = oklch[1];
        Float64 hue_rad = oklch[2] * ColorConversion::deg2rad;

        auto oklab = oklch;
        oklab[1] = chroma * std::cos(hue_rad);
        oklab[2] = chroma * std::sin(hue_rad);

        return ColorConversion::oklabToSrgb(oklab, gamma);
    }
};
}

REGISTER_FUNCTION(FunctionColorOKLCHToSRGB)
{
    FunctionDocumentation::Description description = R"(
        Converts a colour from the **OKLCH** perceptual colour space to the familiar **sRGB** colour space.

        If `L` is outside the range `[0...1]`, `C` is negative, or `H` is outside the range `[0...360]`, the result is implementation-defined.

        :::note
        **OKLCH** is a cylindrical version of the OKLab colour space.
        It's three coordinates are `L` (the lightness in the range `[0...1]`), `C` (chroma `>= 0`) and `H` (hue in degrees  from `[0...360]`).
        OKLab/OKLCH is designed to be perceptually uniform while remaining cheap to compute.
        :::

        The conversion is the inverse of [`colorSRGBToOKLCH`](#colorSRGBToOKLCH):

        1) OKLCH to OKLab.
        2) OKLab to Linear sRGB
        3) Linear sRGB to sRGB

        The second argument gamma is used at the last stage.

        For references of colors in OKLCH space, and how they correspond to sRGB colors please see [https://oklch.com/](https://oklch.com/).
    )";
    FunctionDocumentation::Syntax syntax = "colorOKLCHToSRGB(tuple [, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "A tuple of three numeric values `L`, `C`, `H`, where `L` is in the range `[0...1]`, `C >= 0` and `H` is in the range `[0...360]`.", {"Tuple(Float64, Float64, Float64)"}},
        {"gamma", "Optional. The exponent that is used to transform linear sRGB back to sRGB by applying `(x ^ (1 / gamma)) * 255` for each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (R, G, B) representing sRGB color values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Convert OKLCH to sRGB",
            R"(
SELECT colorOKLCHToSRGB((0.6, 0.12, 40)) AS rgb;
                        )",
            R"(
┌─rgb───────────────────────────────────────────────────────┐
│ (186.02058688365264,100.68677189684993,71.67819977081575) │
└───────────────────────────────────────────────────────────┘
                        )"
            },
         {
            "Convert OKLCH to sRGB (UInt8)",
            R"(
WITH colorOKLCHToSRGB((0.6, 0.12, 40)) AS t
SELECT tuple(toUInt8(t.1), toUInt8(t.2), toUInt8(t.3)) AS RGB;
                        )",
            R"(
┌─RGB──────────┐
│ (186,100,71) │
└──────────────┘
                        )"
            }
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorOKLCHToSRGB>(documentation);
}
}
