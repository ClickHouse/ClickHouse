#pragma once

#include <cassert>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <vector>

// Подбираемая константа - максимальное количество точек в листе дерева
const int MAX_LEAF_NODE_SIZE = 4;

struct Node {
  using Point = std::vector<double>;
  std::shared_ptr<Node> left;
  std::shared_ptr<Node> right;
  size_t dim = 0;
  std::shared_ptr<const std::vector<Point>> points;
  bool is_list = false;
  std::vector<size_t> indexes;
  size_t div_line_point;
  Point div_line_norm;

  Node() = default;

  Node(int dim, std::shared_ptr<const std::vector<Point>> points);

  void Split();
};
