# Train the partitioner from reservoir-sampled indices instead of copying every
# selected high-dimensional vector into a second dense dataset. The underlying
# GMM implementation already reads index subsets in bounded batches; these
# patches only expose that path through the KMeans tree and partitioner factory.

# Add a subset-aware KMeans tree training entry. Keep the ordinary virtual
# Train interface unchanged for every existing caller.
set(_kmeans_tree_h_src "${SCANN_SOURCE_DIR}/scann/trees/kmeans_tree/kmeans_tree.h")
set(_kmeans_tree_h_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/trees/kmeans_tree/kmeans_tree.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/trees/kmeans_tree")
configure_file("${_kmeans_tree_h_src}" "${_kmeans_tree_h_dst}" COPYONLY)
file(READ "${_kmeans_tree_h_dst}" _kmeans_tree_h_content)
scann_checked_replace(
[==[  Status Train(const Dataset& training_data,
               const DistanceMeasure& training_distance, int32_t k_per_level,
               KMeansTreeTrainingOptions* training_options) override;]==]
[==[  Status Train(const Dataset& training_data,
               const DistanceMeasure& training_distance, int32_t k_per_level,
               KMeansTreeTrainingOptions* training_options) override;

  Status TrainWithSubset(
      const Dataset& training_data,
      std::vector<DatapointIndex> training_subset,
      const DistanceMeasure& training_distance, int32_t k_per_level,
      KMeansTreeTrainingOptions* training_options);]==]
    _kmeans_tree_h_content "${_kmeans_tree_h_content}")
file(WRITE "${_kmeans_tree_h_dst}" "${_kmeans_tree_h_content}")

# Keep the subset training implementation in a normal source file so the
# upstream KMeans tree implementation file remains untouched.
list(APPEND SCANN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/scann_partitioning_training.cpp")

# Let the pre-sampled factory receive indices while retaining its original
# three-argument behavior for every existing caller.
set(_kmeans_utils_h_src "${SCANN_SOURCE_DIR}/scann/partitioning/kmeans_tree_partitioner_utils.h")
set(_kmeans_utils_h_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/partitioning/kmeans_tree_partitioner_utils.h")
configure_file("${_kmeans_utils_h_src}" "${_kmeans_utils_h_dst}" COPYONLY)
file(READ "${_kmeans_utils_h_dst}" _kmeans_utils_h_content)
scann_checked_replace(
[==[#define SCANN_PARTITIONING_KMEANS_TREE_PARTITIONER_UTILS_H_

#include "scann/distance_measures/distance_measure_factory.h"]==]
[==[#define SCANN_PARTITIONING_KMEANS_TREE_PARTITIONER_UTILS_H_

#include <type_traits>
#include <utility>
#include <vector>

#include "scann/distance_measures/distance_measure_factory.h"]==]
    _kmeans_utils_h_content "${_kmeans_utils_h_content}")
scann_checked_replace(
[==[KMeansTreePartitionerFactoryPreSampledAndProjected(
    const TypedDataset<T>* dataset, const PartitioningConfig& config,
    shared_ptr<ThreadPool> training_parallelization_pool) {]==]
[==[KMeansTreePartitionerFactoryPreSampledAndProjected(
    const TypedDataset<T>* dataset, const PartitioningConfig& config,
    shared_ptr<ThreadPool> training_parallelization_pool,
    std::vector<DatapointIndex> training_subset = {}) {]==]
    _kmeans_utils_h_content "${_kmeans_utils_h_content}")
scann_checked_replace(
[==[  auto result = make_unique<KMeansTreePartitioner<T>>(
      database_tokenization_dist, query_tokenization_dist);
  KMeansTreeTrainingOptions opts(config);
  opts.training_parallelization_pool = training_parallelization_pool;
  SCANN_RETURN_IF_ERROR(result->CreatePartitioning(
      *dataset, *training_dist, config.num_children(), &opts));]==]
[==[  KMeansTreeTrainingOptions opts(config);
  opts.training_parallelization_pool = training_parallelization_pool;

  unique_ptr<KMeansTreePartitioner<T>> result;
  if constexpr (std::is_same_v<T, float>) {
    if (!training_subset.empty()) {
      auto tree = make_shared<KMeansTree>();
      SCANN_RETURN_IF_ERROR(tree->TrainWithSubset(
          *dataset, std::move(training_subset), *training_dist,
          config.num_children(), &opts));
      result = make_unique<KMeansTreePartitioner<T>>(
          database_tokenization_dist, query_tokenization_dist,
          std::move(tree));
    } else {
      result = make_unique<KMeansTreePartitioner<T>>(
          database_tokenization_dist, query_tokenization_dist);
      SCANN_RETURN_IF_ERROR(result->CreatePartitioning(
          *dataset, *training_dist, config.num_children(), &opts));
    }
  } else {
    SCANN_RET_CHECK(training_subset.empty());
    result = make_unique<KMeansTreePartitioner<T>>(
        database_tokenization_dist, query_tokenization_dist);
    SCANN_RETURN_IF_ERROR(result->CreatePartitioning(
        *dataset, *training_dist, config.num_children(), &opts));
  }]==]
    _kmeans_utils_h_content "${_kmeans_utils_h_content}")
file(WRITE "${_kmeans_utils_h_dst}" "${_kmeans_utils_h_content}")

# Enable the subset path only for the ClickHouse dense Float32/no-projection
# build. Sparse, projected, and other typed datasets retain upstream behavior.
set(_partitioner_factory_cc_src "${SCANN_SOURCE_DIR}/scann/partitioning/partitioner_factory_base.cc")
set(_partitioner_factory_cc_dst "${CMAKE_CURRENT_BINARY_DIR}/partitioner_factory_base.cc")
configure_file("${_partitioner_factory_cc_src}" "${_partitioner_factory_cc_dst}" COPYONLY)
file(READ "${_partitioner_factory_cc_dst}" _partitioner_factory_cc_content)
scann_checked_replace(
[==[#include <memory>
#include <utility>]==]
[==[#include <memory>
#include <type_traits>
#include <utility>]==]
    _partitioner_factory_cc_content "${_partitioner_factory_cc_content}")
scann_checked_replace(
[==[  const size_t sample_size = ComputeSampleSize(config, dataset);
  if (sample_size < dataset->size()) {]==]
[==[  const size_t sample_size = ComputeSampleSize(config, dataset);
  if constexpr (std::is_same_v<T, float>) {
    if (sample_size > 0 && sample_size < dataset->size() &&
        dataset->IsDense()) {
      MTRandom rng(kDeterministicSeed + 1);
      std::vector<DatapointIndex> sample;
      {
        auto reservoir_sample =
            ReservoirSampleIdxs(rng, dataset->size(), sample_size);
        sample.assign(reservoir_sample.begin(), reservoir_sample.end());
      }
      LOG(INFO) << "Size of sampled dataset for training partition: "
                << sample.size();
      return KMeansTreePartitionerFactoryPreSampledAndProjected(
          dataset, config, pool, std::move(sample));
    }
  }

  if (sample_size < dataset->size()) {]==]
    _partitioner_factory_cc_content "${_partitioner_factory_cc_content}")
file(WRITE "${_partitioner_factory_cc_dst}" "${_partitioner_factory_cc_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_partitioner_factory_cc_src}")
list(APPEND SCANN_SOURCES "${_partitioner_factory_cc_dst}")
