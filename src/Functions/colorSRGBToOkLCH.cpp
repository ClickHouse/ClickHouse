#include <cmath>
#include <tuple>
#include "Color_Utils.h"

using namespace std;

tuple<float, float, float> colorSRGBToOkLCH(tuple<float, float, float> rgb, float gamma = 2.2f)
{
    float r = srgbToLinear(get<0>(rgb));
    float g = srgbToLinear(get<1>(rgb));
    float b = srgbToLinear(get<2>(rgb));

    // sRGB Linear → XYZ
    float X = 0.4122214708f * r + 0.5363325363f * g + 0.0514459929f * b;
    float Y = 0.2119034982f * r + 0.6806995451f * g + 0.1073969566f * b;
    float Z = 0.0883024619f * r + 0.2817188376f * g + 0.6299787005f * b;

    // XYZ → LMS → cube root
    float l_ = cbrtf(M1[0][0] * X + M1[0][1] * Y + M1[0][2] * Z);
    float m_ = cbrtf(M1[1][0] * X + M1[1][1] * Y + M1[1][2] * Z);
    float s_ = cbrtf(M1[2][0] * X + M1[2][1] * Y + M1[2][2] * Z);

    // LMS → OKLab
    float L = 0.2104542553f * l_ + 0.7936177850f * m_ - 0.0040720468f * s_;
    float a = 1.9779984951f * l_ - 2.4285922050f * m_ + 0.4505937099f * s_;
    float b2 = 0.0259040371f * l_ + 0.7827717662f * m_ - 0.8086757660f * s_;

    // OKLab → OKLCH
    float C = sqrt(a * a + b2 * b2);
    float H = atan2(b2, a) * (180.0f / M_PI);
    if (H < 0.0f)
        H += 360.0f;

    return make_tuple(L * 100.0f, C * 100.0f, H);
}
