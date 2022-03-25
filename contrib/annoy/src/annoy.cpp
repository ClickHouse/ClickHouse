#include <ctime>
#include <limits>
#include <map>
#include <set>

#include "annoy/annoy.h"
#include "annoy/point.h"

#include "settings.cpp"

namespace Annoy {

Annoy::Annoy(const std::vector<Point>& points) : points_(std::make_shared<const std::vector<Point>>(points)), trees_(NUM_OF_TREES) {
  std::srand(std::time(nullptr));
  std::vector<size_t> indexes(points_->size());
  for (int i = 0; i < points_->size(); ++i) {
    indexes[i] = i;
  }
  for (auto& tree : trees_) {
    tree = std::make_shared<Node>(points_);
    tree->data.emplace<Node::LeafData>(Node::LeafData{indexes});
    tree->TrySplit();
  }
}

std::vector<Point> Annoy::FindKNN(const Point& x, size_t k) const {
  std::multimap<double, std::shared_ptr<Node>> heap;
  std::map<size_t, double> candidates_set;
  for (const auto& tree : trees_) {
    heap.insert({std::numeric_limits<double>::max(), tree});
  }
  while (candidates_set.size() < k && !heap.empty()) {
    auto [dist, node] = *heap.rbegin();
    heap.erase(std::prev(heap.end()));
    if (node->IsList()) {
      for (size_t i : std::get<Node::LeafData>(node->data).indexes) {
        candidates_set[i] = ScalarMul(x - (*points_)[i], x - (*points_)[i]);
      }
    } else {
      auto& leaf_data = std::get<Node::InnerData>(node->data);
      double scalar_mul = ScalarMul(x - leaf_data.div_line_point, leaf_data.div_line_norm);
      heap.insert({std::min(-scalar_mul, dist), leaf_data.left});
      heap.insert({std::min(scalar_mul, dist), leaf_data.right});
    }
  }
  std::vector<std::pair<size_t, double>> candidates;
  candidates.reserve(candidates_set.size());
  for (const auto& x : candidates_set) {
    candidates.emplace_back(x);
  }
  std::vector<Point> result(std::min(k, candidates.size()));
  std::partial_sort(candidates.begin(), candidates.begin() + result.size(), candidates.end());
  for (int i = 0; i < result.size(); ++i) {
    result[i] = (*points_)[candidates[i].first];
  }
  return result;
}

};
