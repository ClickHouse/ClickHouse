#pragma once

#include <cassert>
#include <memory>
#include <variant>
#include <vector>

namespace Annoy {

struct Node {
  using Point = std::vector<double>;

  struct InnerData {
    std::shared_ptr<Node> left;
    std::shared_ptr<Node> right;
    Point div_line_point;
    Point div_line_norm;
  };
  struct LeafData {
    std::vector<size_t> indexes;
  };

  std::variant<InnerData, LeafData> data;

  size_t dim = 0;
  std::shared_ptr<const std::vector<Point>> points;

  bool IsList() const;

  Node() = default;

  Node(std::shared_ptr<const std::vector<Point>> points);

  void TrySplit();

 private:
  void GenerateLine(InnerData& inner_node_data, const std::vector<size_t>& indexes);
};

};
