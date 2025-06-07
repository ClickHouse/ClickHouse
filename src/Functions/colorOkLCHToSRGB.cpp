#include <cmath>
#include <tuple>
#include "Color_Utils.h"

using namespace std;

tuple<float, float, float> colorOkLCHToSRGB(tuple<float, float, float> oklch, float gamma = 2.2f)
{
    float L = get<0>(oklch) / 100.0f;
    float C = get<1>(oklch) / 100.0f;
    float H = get<2>(oklch) * M_PI / 180.0f;

    float a = cos(H) * C;
    float b2 = sin(H) * C;

    // OKLab → LMS
    float l_ = L + 0.3963377774f * a + 0.2158037573f * b2;
    float m_ = L - 0.1055613458f * a - 0.0638541728f * b2;
    float s_ = L - 0.0894841775f * a - 1.2914855480f * b2;

    l_ = l_ * l_ * l_;
    m_ = m_ * m_ * m_;
    s_ = s_ * s_ * s_;

    // LMS → XYZ
    float X = M2[0][0] * l_ + M2[0][1] * m_ + M2[0][2] * s_;
    float Y = M2[1][0] * l_ + M2[1][1] * m_ + M2[1][2] * s_;
    float Z = M2[2][0] * l_ + M2[2][1] * m_ + M2[2][2] * s_;

    // XYZ → linear sRGB
    float r_lin = 3.2409699419f * X - 1.5373831776f * Y - 0.4986107603f * Z;
    float g_lin = -0.9692436363f * X + 1.8759675015f * Y + 0.0415550574f * Z;
    float b_lin = 0.0556300797f * X - 0.2039769589f * Y + 1.0569715142f * Z;

    // linear sRGB → sRGB
    float r = linearToSrgb(r_lin);
    float g = linearToSrgb(g_lin);
    float b = linearToSrgb(b_lin);

    return make_tuple(r, g, b);
}
