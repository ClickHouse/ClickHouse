#include "annoy/node.h"
#include "annoy/point.h"
#include "settings.cpp"

namespace Annoy {

const double EPS = 1e-5;

Node::Node(std::shared_ptr<const std::vector<Point>> points) : points(std::move(points)), data(LeafData()) {}


void Node::TrySplit() {
  if (std::get<LeafData>(data).indexes.size() <= MAX_LEAF_NODE_SIZE) {
    return;
  }
  auto indexes = std::move(std::get<LeafData>(data).indexes);
  data.emplace<InnerData>();
  auto& inner_node_data = std::get<InnerData>(data);

  GenerateLine(inner_node_data, indexes);

  inner_node_data.left = std::make_shared<Node>(points);
  inner_node_data.right = std::make_shared<Node>(points);
  auto& left_child_indexes = std::get<LeafData>(inner_node_data.left->data).indexes;
  auto& right_child_indexes = std::get<LeafData>(inner_node_data.right->data).indexes;
  for (size_t i : indexes) {
    if (ScalarMul((*points)[i] - inner_node_data.div_line_point, inner_node_data.div_line_norm) < 0) {
      left_child_indexes.push_back(i);
    } else {
      right_child_indexes.push_back(i);
    }
  }

  inner_node_data.left->TrySplit();
  inner_node_data.right->TrySplit();
}

bool Node::IsList() const {
  return std::holds_alternative<LeafData>(data);
}

void Node::GenerateLine(InnerData& inner_node_data, const std::vector<size_t>& indexes) {
  size_t i1 = std::rand() % (indexes.size() - 1);
  size_t i2 = std::rand() % (indexes.size() - i1 - 1);
  i2 += i1 + 1;
  inner_node_data.div_line_point = ((*points)[indexes[i1]] + (*points)[indexes[i2]]) * 0.5;
  inner_node_data.div_line_norm = (*points)[indexes[i2]] - (*points)[indexes[i1]];
}

};
