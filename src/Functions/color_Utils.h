#pragma once
#include <cmath>
#include <tuple>

using namespace std;

// sRGB gamma correction
inline float srgbToLinear(float c)
{
    c /= 255.0f;
    return (c <= 0.04045f) ? c / 12.92f : pow((c + 0.055f) / 1.055f, 2.4f);
}

inline float linearToSrgb(float c)
{
    c = (c <= 0.0031308f) ? 12.92f * c : 1.055f * pow(c, 1.0f / 2.4f) - 0.055f;
    return c * 255.0f;
}

// XYZ ↔ OKLab constants
const float M1[3][3]
    = {{0.8189330101f, 0.3618667424f, -0.1288597137f},
       {0.0329845436f, 0.9293118715f, 0.0361456387f},
       {0.0482003018f, 0.2643662691f, 0.6338517070f}};

const float M2[3][3]
    = {{1.2270138511f, -0.5577999807f, 0.2812561490f},
       {-0.0405801784f, 1.1122568696f, -0.0716766787f},
       {-0.0763812845f, -0.4214819784f, 1.5861632204f}};
