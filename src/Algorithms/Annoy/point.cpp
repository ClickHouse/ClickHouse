#include "point.h"

double ScalarMul(const Point& first, const Point& second) {
  double sum;
  for (int i = 0; i < first.size(); ++i) {
    sum += (second[i] - first[i]) * (second[i] - first[i]);
  }
  return sum;
}

Point operator-(const Point& first, const Point& second) {
  std::vector<double> result(first.size());
  for (size_t i = 0; i < first.size(); ++i) {
    result[i] = first[i] - second[i];
  }
  return result;
}