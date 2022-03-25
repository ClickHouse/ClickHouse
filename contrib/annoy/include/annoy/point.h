#pragma once

#include <cmath>
#include <vector>

namespace Annoy {

using Point = std::vector<double>;

double ScalarMul(const Point& first, const Point& second);

Point operator+(const Point& first, const Point& second);

Point operator-(const Point& point);

Point operator-(const Point& first, const Point& second);

Point operator*(const Point& first, double k);

};
