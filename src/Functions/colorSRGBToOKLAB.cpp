#include <Functions/ColorConversion.h>

namespace DB
{

/** Function that converts color from sRGB color space to perceptual OKLAB color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
namespace
{
class FunctionColorSRGBToOKLAB : public ColorConversionFromSRGBBase<FunctionColorSRGBToOKLAB>
{
public:
    static constexpr auto name = "colorSRGBToOKLAB";

    explicit FunctionColorSRGBToOKLAB(ContextPtr context_)
        : ColorConversionFromSRGBBase(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorSRGBToOKLAB>(context_); }

    String getName() const override { return name; }

    /// sRGB -> OKLAB conversion. Follows the step-by-step pipeline described in Ottosson's article. See ColorConversion.h
    ColorConversion::Color convertFromSrgb(const ColorConversion::Color & rgb, Float64 gamma) const
    {
        /// sRGB is already in Cartesian form, so we can directly convert to OKLab
        /// Steps 1-3: sRGB to OKLab (handled by ColorConversion namespace helper)
        return ColorConversion::srgbToOklab(rgb, gamma);
    }
};
}

REGISTER_FUNCTION(ColorSRGBToOKLAB)
{
    FunctionDocumentation::Description description = R"(
        Converts a colour encoded in the **sRGB** colour space to the perceptually uniform **OKLAB** colour space.

        If any input channel is outside `[0...255]` or the gamma value is non-positive, the behaviour is implementation-defined.

        :::note
        **OKLAB** is a perceptually uniform color space.
        Its three coordinates are `L` (the lightness in the range `[0...1]`), `a (Green-Red axis)` and `b (Blue-Yellow axis)`.
        OKLab is designed to be perceptually uniform while remaining cheap to compute.
        :::

        The conversion consists of two stages:
        1) sRGB to Linear sRGB
        2) Linear sRGB to OKLab
    )";
    FunctionDocumentation::Syntax syntax = "colorSRGBToOKLAB(tuple[, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "Tuple of three values R, G, B in the range `[0...255]`.", {"Tuple(UInt8, UInt8, UInt8)"}},
        {"gamma", "Optional. Exponent that is used to linearize sRGB by applying `(x / 255)^gamma` to each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (L, a, b) representing the OKLAB color space values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Convert sRGB to OKLAB",
        R"(
                        SELECT colorSRGBToOKLAB((128, 64, 32), 2.2) AS lab
                    )",
        R"(
                        ┌─lab───────────────────────────────────────────────────────── ┐
                        │ (0.4436238384931984,0.07266246769242975,0.07500108778529994) │
                        └───────────────────────────────────────────────────────────── ┘
                    )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorSRGBToOKLAB>(documentation);
}

}
