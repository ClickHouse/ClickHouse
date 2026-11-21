#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/ColorConversion.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ITupleFunction.h>

namespace DB
{

/** Function that converts color from OKLAB perceptual color space to sRGB color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
namespace
{
class FunctionColorOKLABToSRGB : public ColorConversionToSRGBBase<FunctionColorOKLABToSRGB>
{
public:
    static constexpr auto name = "colorOKLABToSRGB";

    explicit FunctionColorOKLABToSRGB(ContextPtr context_)
        : ColorConversionToSRGBBase(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorOKLABToSRGB>(context_); }

    String getName() const override { return name; }

    /// OKLAB -> sRGB conversion. Follows the step-by-step pipeline described in Ottosson's article. See ColorConversion.h
    ColorConversion::Color convertToSrgb(const ColorConversion::Color & oklab, Float64 gamma) const
    {
        return ColorConversion::oklabToSrgb(oklab, gamma);
    }
};
}

REGISTER_FUNCTION(FunctionColorOKLABToSRGB)
{
    FunctionDocumentation::Description description = R"(
        Converts a color from the OKLab perceptual color space to the sRGB color space.

        The input color is specified in the OKLab color space. If the input values are outside
        the typical OKLab ranges, the result is implementation-defined.

        OKLab uses three components:
        - L: perceptual lightness (typically in the range [0..1])
        - a: green-red opponent axis
        - b: blue-yellow opponent axis

        The a and b components are theoretically unbounded, but in practice are between -0.4 and 0.4.
        OKLab is designed to be perceptually uniform
        while remaining inexpensive to compute.

        The conversion is intended to be the inverse of colorSRGBToOKLAB and consists of
        the following stages:
        1) Conversion from OKLab to linear sRGB.
        2) Conversion from linear sRGB to gamma-encoded sRGB.

        The optional gamma argument specifies the exponent used when converting from linear
        sRGB to gamma-encoded RGB values. If not specified, a default gamma value is used
        for consistency with colorSRGBToOKLAB.

        For more information about the OKLab color space and its relationship to sRGB, see https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/Values/color_value/oklab
        .
    )";
    FunctionDocumentation::Syntax syntax = "colorOKLABToSRGB(tuple [, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "A tuple of three numeric values `L`, `a`, `b`, where `L` is in the range `[0...1]`.", {"Tuple(Float64, Float64, Float64)"}},
        {"gamma", "Optional. The exponent that is used to transform linear sRGB back to sRGB by applying `(x ^ (1 / gamma)) * 255` for each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (R, G, B) representing sRGB color values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Convert OKLAB to sRGB (Float)",
            R"(
SELECT colorOKLABToSRGB((0.4466, 0.0991, 0.44)) AS rgb;
                        )",
            R"(
┌─rgb──────────────────────┐
│ (198.07056923258935,0,0) │
└──────────────────────────┘
                        )"
            },
         {
            "Convert OKLAB to sRGB (UInt8)",
            R"(
WITH colorOKLABToSRGB((0.7, 0.1, 0.54)) AS t
SELECT tuple(toUInt8(t.1), toUInt8(t.2), toUInt8(t.3)) AS RGB;
                        )",
            R"(
┌─RGB──────────┐
│ (255,0,0)    │
└──────────────┘
                        )"
            }
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorOKLABToSRGB>(documentation);
}
}
