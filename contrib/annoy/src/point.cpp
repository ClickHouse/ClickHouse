#include "annoy/point.h"

namespace Annoy {

double ScalarMul(const Point& first, const Point& second) {
  double sum = 0.;
  for (int i = 0; i < first.size(); ++i) {
    sum += first[i] * second[i];
  }
  return sum;
}

Point operator+(const Point& first, const Point& second) {
  std::vector<double> result(first.size());
  for (size_t i = 0; i < first.size(); ++i) {
    result[i] = first[i] + second[i];
  }
  return result;
}

Point operator-(const Point& point) {
  return point * (-1.);
}

Point operator-(const Point& first, const Point& second) {
  return first + (-second);
}

Point operator*(const Point& point, double k) {
  Point result = point;
  for (int i = 0; i < point.size(); ++i) {
    result[i] *= k;
  }
  return result;
}

};
