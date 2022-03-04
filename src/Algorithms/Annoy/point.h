#pragma once

#include <cmath>
#include <vector>

using Point = std::vector<double>;

double ScalarMul(const Point& first, const Point& second);

Point operator-(const Point& first, const Point& second);