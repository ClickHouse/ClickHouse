#include "node.h"
#include "point.h"

Node::Node(int dim, std::shared_ptr<const std::vector<Point>> points) : dim(dim), points(points) {}

void Node::Split() {
  assert(dim > 0);
  if (indexes.size() <= MAX_LEAF_NODE_SIZE) {
    is_list = true;
    return;
  }

  size_t i1 = std::rand() % indexes.size();
  size_t i2 = std::rand() % (indexes.size() - 1);
  i2 += (i2 >= i1);
  div_line_point = i1;

  div_line_norm = (*points)[indexes[i2]] - (*points)[indexes[i1]];
  double scalar_mul = std::fabs(ScalarMul(div_line_norm, div_line_norm)) / 2;

  left = std::make_shared<Node>(dim, points);
  right = std::make_shared<Node>(dim, points);
  left->dim = dim;
  right->dim = dim;
  for (size_t i : indexes) {
    if (std::fabs(ScalarMul((*points)[i] - (*points)[indexes[i1]], div_line_norm)) < scalar_mul) {
      left->indexes.push_back(i);
    } else {
      right->indexes.push_back(i);
    }
  }

  left->Split();
  right->Split();
}
