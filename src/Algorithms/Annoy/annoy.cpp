#include <algorithm>
#include <limits>
#include <map>

#include "annoy.h"
#include "point.h"

Annoy::Annoy(int dim) : dim_(dim) {}

Annoy::Annoy(const std::vector<Point>& points) : points_(std::make_shared<const std::vector<Point>>(points)) {
  assert(!points.empty());
  std::srand(std::time(nullptr));
  dim_ = (*points_)[0].size();
  tree_ = std::make_shared<Node>(dim_, points_);
  tree_->Split();
}

std::vector<Point> Annoy::FindKNN(const Point& x, size_t k) const {
  std::multimap<double, std::shared_ptr<Node>> heap;
  std::vector<std::pair<size_t, double>> candidates;
  heap.insert({std::numeric_limits<double>::max(), tree_});
  while (candidates.size() < k && heap.size() > 0) {

    auto [dist, node] = *heap.begin();
    heap.erase(heap.begin());
    if (node->is_list) {
      for (size_t i : node->indexes) {
        candidates.push_back({i, ScalarMul(x - (*points_)[i], x - (*points_)[i])});
      }
    } else {
      double scalar_mul = std::fabs(ScalarMul(x - (*points_)[node->div_line_point], node->div_line_norm));
      scalar_mul = std::min(scalar_mul, dist);
      heap.insert({scalar_mul, node->left});
      heap.insert({scalar_mul, node->right});
    }
  }
  std::partial_sort(candidates.begin(), candidates.begin() + k, candidates.end());
  std::vector<Point> result(k);
  for (int i = 0; i < k; ++i) {
    result[i] = (*points_)[candidates[i].first];
  }
  return result;
}