#pragma once

#include <memory>
#include <vector>

#include "node.h"

class Annoy {
  using Point = std::vector<double>;
 public:
  explicit Annoy(int dim);
  explicit Annoy(const std::vector<Point>& points);
  std::vector<Point> FindKNN(const Point& x, size_t k) const;
 private:
  std::shared_ptr<const std::vector<Point>> points_;
  std::shared_ptr<Node> tree_;
  // std::vector<std::shared_ptr<Node>> trees_;
  int dim_;
};