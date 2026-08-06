# Reduce Tree-AH artifact persistence and AH training memory without changing
# the serialized index representation or the ordinary AH training path.

# Allow ClickHouse's persistence adapter to read Tree-AH artifacts without
# adding a persistence-specific public API to the upstream ScaNN class.
set(_tahr_h_src "${SCANN_SOURCE_DIR}/scann/tree_x_hybrid/tree_ah_hybrid_residual.h")
set(_tahr_h_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/tree_x_hybrid/tree_ah_hybrid_residual.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/tree_x_hybrid")
configure_file("${_tahr_h_src}" "${_tahr_h_dst}" COPYONLY)
file(READ "${_tahr_h_dst}" _tahr_h_content)
scann_checked_replace(
[==[ private:
  class UnlockedTreeAHHybridResidualPreprocessingResults]==]
[==[ private:
  friend class TreeAHHybridResidualPersistenceAdapter;

  class UnlockedTreeAHHybridResidualPreprocessingResults]==]
    _tahr_h_content "${_tahr_h_content}")
file(WRITE "${_tahr_h_dst}" "${_tahr_h_content}")
list(APPEND SCANN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/scann_persistence_adapter.cpp")
# Tree-AH residual training no longer needs its row-major residual dataset once
# all block-major training datasets have been materialized. Keep those block
# datasets as Float32 and convert only the block currently passed to double-based
# k-means. The release callback opts only the Tree-AH residual path into this
# behavior; ordinary AH training continues to use the upstream all-double path.

# Extend the internal AH training declaration with an optional release callback.
set(_ahi_h_src "${SCANN_SOURCE_DIR}/scann/hashes/internal/asymmetric_hashing_impl.h")
set(_ahi_h_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/hashes/internal/asymmetric_hashing_impl.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/hashes/internal")
configure_file("${_ahi_h_src}" "${_ahi_h_dst}" COPYONLY)
file(READ "${_ahi_h_dst}" _ahi_h_content)
scann_checked_replace(
[==[#include <cmath>
#include <cstdint>]==]
[==[#include <cmath>
#include <cstdint>
#include <functional>]==]
    _ahi_h_content "${_ahi_h_content}")
scann_checked_replace(
[==[namespace research_scann {
namespace asymmetric_hashing_internal {]==]
[==[namespace research_scann {
namespace asymmetric_hashing_internal {

using ReleaseDatasetCallback = std::function<void()>;]==]
    _ahi_h_content "${_ahi_h_content}")
scann_checked_replace(
[==[  static StatusOr<std::vector<DenseDataset<double>>> TrainAsymmetricHashing(
      const TypedDataset<T>& dataset, const TrainingOptionsT& opts,
      shared_ptr<ThreadPool> pool);]==]
[==[  static StatusOr<std::vector<DenseDataset<double>>> TrainAsymmetricHashing(
      const TypedDataset<T>& dataset, const TrainingOptionsT& opts,
      shared_ptr<ThreadPool> pool, ReleaseDatasetCallback release_dataset);]==]
    _ahi_h_content "${_ahi_h_content}")
scann_checked_replace(
[==[StatusOr<std::vector<DenseDataset<double>>> TrainAsymmetricHashing(
    const TypedDataset<T>& dataset,
    const asymmetric_hashing2::TrainingOptionsTyped<T>& opts,
    shared_ptr<ThreadPool> pool) {
  return AhImpl<T>::TrainAsymmetricHashing(dataset, opts, std::move(pool));
}]==]
[==[StatusOr<std::vector<DenseDataset<double>>> TrainAsymmetricHashing(
    const TypedDataset<T>& dataset,
    const asymmetric_hashing2::TrainingOptionsTyped<T>& opts,
    shared_ptr<ThreadPool> pool,
    ReleaseDatasetCallback release_dataset) {
  return AhImpl<T>::TrainAsymmetricHashing(
      dataset, opts, std::move(pool), std::move(release_dataset));
}]==]
    _ahi_h_content "${_ahi_h_content}")
file(WRITE "${_ahi_h_dst}" "${_ahi_h_content}")

# Forward the callback through TrainSingleMachine.
set(_aht_h_src "${SCANN_SOURCE_DIR}/scann/hashes/asymmetric_hashing2/training.h")
set(_aht_h_dst "${CMAKE_CURRENT_BINARY_DIR}/scann/hashes/asymmetric_hashing2/training.h")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/scann/hashes/asymmetric_hashing2")
configure_file("${_aht_h_src}" "${_aht_h_dst}" COPYONLY)
file(READ "${_aht_h_dst}" _aht_h_content)
scann_checked_replace(
[==[StatusOr<unique_ptr<Model<T>>> TrainSingleMachine(
    const TypedDataset<T>& dataset, const TrainingOptions<T>& params,
    shared_ptr<ThreadPool> pool = nullptr) {]==]
[==[StatusOr<unique_ptr<Model<T>>> TrainSingleMachine(
    const TypedDataset<T>& dataset, const TrainingOptions<T>& params,
    shared_ptr<ThreadPool> pool = nullptr,
    asymmetric_hashing_internal::ReleaseDatasetCallback release_dataset = {}) {]==]
    _aht_h_content "${_aht_h_content}")
scann_checked_replace(
[==[        ::research_scann::asymmetric_hashing_internal::TrainAsymmetricHashing(
            dataset_no_bias, params, pool));]==]
[==[        ::research_scann::asymmetric_hashing_internal::TrainAsymmetricHashing(
            dataset_no_bias, params, pool, {}));]==]
    _aht_h_content "${_aht_h_content}")
scann_checked_replace(
[==[        ::research_scann::asymmetric_hashing_internal::TrainAsymmetricHashing(
            dataset, params, pool));]==]
[==[        ::research_scann::asymmetric_hashing_internal::TrainAsymmetricHashing(
            dataset, params, pool, std::move(release_dataset)));]==]
    _aht_h_content "${_aht_h_content}")
file(WRITE "${_aht_h_dst}" "${_aht_h_content}")

# Stage block residuals in their native floating-point type and convert one
# block at a time for GmmUtils. The callback is invoked only after staging has
# finished, so releasing the row-major dataset cannot invalidate projection.
set(_ahi_cc_src "${SCANN_SOURCE_DIR}/scann/hashes/internal/asymmetric_hashing_impl.cc")
set(_ahi_cc_dst "${CMAKE_CURRENT_BINARY_DIR}/asymmetric_hashing_impl.cc")
configure_file("${_ahi_cc_src}" "${_ahi_cc_dst}" COPYONLY)
file(READ "${_ahi_cc_dst}" _ahi_cc_content)
scann_checked_replace(
[==[#include <cstdint>
#include <numeric>]==]
[==[#include <cstdint>
#include <numeric>
#include <type_traits>]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[template <typename T>
StatusOr<vector<DenseDataset<double>>> AhImpl<T>::TrainAsymmetricHashing(
    const TypedDataset<T>& dataset, const TrainingOptionsT& opts,
    shared_ptr<ThreadPool> pool) {]==]
[==[template <typename T, typename BlockT>
StatusOr<vector<DenseDataset<double>>> TrainAsymmetricHashingImpl(
    const TypedDataset<T>& dataset,
    const asymmetric_hashing2::TrainingOptionsTyped<T>& opts,
    shared_ptr<ThreadPool> pool, ReleaseDatasetCallback release_dataset) {]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[  ChunkedDatapoint<double> chunked_vec;]==]
[==[  ChunkedDatapoint<BlockT> chunked_vec;]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[  vector<DenseDataset<double>> chunked_dataset(num_blocks);]==]
[==[  vector<DenseDataset<BlockT>> chunked_dataset(num_blocks);]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[      DenseDataset<double>& ds = chunked_dataset[i];]==]
[==[      DenseDataset<BlockT>& ds = chunked_dataset[i];]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[  const auto& quantization_distance = opts.quantization_distance();]==]
[==[  if (release_dataset) release_dataset();

  const auto& quantization_distance = opts.quantization_distance();]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[  for (size_t i : Seq(num_blocks)) {
    DenseDataset<double> centers;
    vector<vector<DatapointIndex>> subpartitions;
    SCANN_RETURN_IF_ERROR(gmm.ComputeKmeansClustering(
        chunked_dataset[i], opts.config().num_clusters_per_block(), &centers,
        {.final_partitions = &subpartitions, .weights = weights}));]==]
[==[  for (size_t i : Seq(num_blocks)) {
    DenseDataset<double> training_dataset;
    if constexpr (std::is_same_v<BlockT, double>) {
      training_dataset = std::move(chunked_dataset[i]);
    } else {
      chunked_dataset[i].ConvertType(&training_dataset);
      chunked_dataset[i].clear();
      chunked_dataset[i].ShrinkToFit();
    }

    DenseDataset<double> centers;
    vector<vector<DatapointIndex>> subpartitions;
    SCANN_RETURN_IF_ERROR(gmm.ComputeKmeansClustering(
        training_dataset, opts.config().num_clusters_per_block(), &centers,
        {.final_partitions = &subpartitions, .weights = weights}));]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[          ComputeNormBiasCorrection(chunked_dataset[i], centers[center_idx],
                                    subpartitions[center_idx]));]==]
[==[          ComputeNormBiasCorrection(training_dataset, centers[center_idx],
                                    subpartitions[center_idx]));]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[    chunked_dataset[i].clear();
    chunked_dataset[i].ShrinkToFit();

    vector<uint32_t> centers_permutation(centers.size());]==]
[==[    vector<uint32_t> centers_permutation(centers.size());]==]
    _ahi_cc_content "${_ahi_cc_content}")
scann_checked_replace(
[==[  return std::move(all_centers);
}

template <typename T>
Status AhImpl<T>::IndexDatapoint]==]
[==[  return std::move(all_centers);
}

template <typename T>
StatusOr<vector<DenseDataset<double>>> AhImpl<T>::TrainAsymmetricHashing(
    const TypedDataset<T>& dataset, const TrainingOptionsT& opts,
    shared_ptr<ThreadPool> pool, ReleaseDatasetCallback release_dataset) {
  if (release_dataset) {
    return TrainAsymmetricHashingImpl<T, FloatT>(
        dataset, opts, std::move(pool), std::move(release_dataset));
  }
  return TrainAsymmetricHashingImpl<T, double>(dataset, opts, std::move(pool),
                                                {});
}

template <typename T>
Status AhImpl<T>::IndexDatapoint]==]
    _ahi_cc_content "${_ahi_cc_content}")
file(WRITE "${_ahi_cc_dst}" "${_ahi_cc_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_ahi_cc_src}")
list(APPEND SCANN_SOURCES "${_ahi_cc_dst}")

# The Tree-AH residual dataset is the only caller that can release its
# training input after block staging. Full-dataset hashing later recomputes
# residuals from the original vectors and does not use this sampled dataset.
set(_txhf_src "${SCANN_SOURCE_DIR}/scann/base/internal/tree_x_hybrid_factory.cc")
set(_txhf_dst "${CMAKE_CURRENT_BINARY_DIR}/tree_x_hybrid_factory.cc")
configure_file("${_txhf_src}" "${_txhf_dst}" COPYONLY)
file(READ "${_txhf_dst}" _txhf_content)
scann_checked_replace(
[==[        ah_model, asymmetric_hashing2::TrainSingleMachine(
                      residuals, training_opts, opts->parallelization_pool));]==]
[==[        ah_model, asymmetric_hashing2::TrainSingleMachine(
                      residuals, training_opts, opts->parallelization_pool,
                      [&residuals] {
                        residuals.clear();
                        residuals.ShrinkToFit();
                      }));]==]
    _txhf_content "${_txhf_content}")
file(WRITE "${_txhf_dst}" "${_txhf_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_txhf_src}")
list(APPEND SCANN_SOURCES "${_txhf_dst}")
