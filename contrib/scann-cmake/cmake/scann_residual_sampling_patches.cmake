# ---------------------------------------------------------------------------
# Train the AH codebook on a sampled subset of residuals instead of the full
# dataset.  Upstream ScaNN's ComputeResidualsImpl computes residuals for every
# datapoint (e.g. ~2.9 GB for 1M x 768), which dominates index build time and
# peak memory.  The residuals are used only as the codebook training set
# (asymmetric_hashing2::TrainSingleMachine); the full dataset is still quantized
# point-by-point in BuildLeafSearchers (which re-residualizes each point via
# ResidualizeToFloat), so subsampling only the training set has negligible
# recall impact.  The sample contains 50% of the dataset, clamped to
# [100,000, 2,000,000].  The AH codebook is a set of 2-D, 16-center k-means whose
# training-set requirement is essentially independent of the dataset size, so 50%
# is ample; the floor keeps small datasets well-trained and the cap keeps build
# time bounded for very large datasets.
#
# Implemented as build-directory patches rather than edits to the scann
# submodule, mirroring the flags.cc / avx512.h patches above. The checked replacement
# uses bracket arguments so the multi-line C++ needs no escaping, and the whole
# ComputeResidualsImpl body is replaced as a single anchor so the in-body
# dataset.size() uses are rewritten while the function's other dataset.size()
# uses are left untouched.

# Rewrite ComputeResidualsImpl in tree_ah_hybrid_residual.cc to sample the
# AH codebook training data.
set(_tahr_src "${SCANN_SOURCE_DIR}/scann/tree_x_hybrid/tree_ah_hybrid_residual.cc")
set(_tahr_dst "${CMAKE_CURRENT_BINARY_DIR}/tree_ah_hybrid_residual.cc")
configure_file("${_tahr_src}" "${_tahr_dst}" COPYONLY)
file(READ "${_tahr_dst}" _tahr_content)
scann_checked_replace(
[==[#include <numeric>
#include <unordered_set>]==]
[==[#include <numeric>
#include <random>
#include <unordered_set>]==]
    _tahr_content "${_tahr_content}")
scann_checked_replace(
[==[  vector<uint32_t> tokens_by_datapoint(dataset.size());
  for (uint32_t token : Seq(datapoints_by_token.size())) {
    for (DatapointIndex dp_idx : datapoints_by_token[token]) {
      tokens_by_datapoint[dp_idx] = token;
    }
  }

  vector<float> residuals_storage;
  auto loop_body = [&](size_t dp_idx, bool is_first_dp)
                       SCANN_INLINE_LAMBDA -> Status {
    const uint32_t token = tokens_by_datapoint[dp_idx];
    SCANN_ASSIGN_OR_RETURN(auto residual, get_residual(dataset[dp_idx], token));

    if (is_first_dp) {
      residuals_storage =
          vector<float>(dataset.size() * residual.dimensionality());
    } else {
      DCHECK_EQ(residuals_storage.size(),
                dataset.size() * residual.dimensionality());
    }
    std::copy(residual.values().begin(), residual.values().end(),
              residuals_storage.begin() + dp_idx * residual.dimensionality());
    return OkStatus();
  };

  if (dataset.empty()) return DenseDataset<float>();
  SCANN_RETURN_IF_ERROR(loop_body(0, true));
  SCANN_RETURN_IF_ERROR(ParallelForWithStatus<1>(
      Seq(1, dataset.size()), parallelization_pool,
      [&](size_t dp_idx) { return loop_body(dp_idx, false); }));
  return DenseDataset<float>(std::move(residuals_storage), dataset.size());
}]==]
[==[  if (dataset.empty()) return DenseDataset<float>();

  vector<uint32_t> tokens_by_datapoint(dataset.size());
  for (uint32_t token : Seq(datapoints_by_token.size())) {
    for (DatapointIndex dp_idx : datapoints_by_token[token]) {
      tokens_by_datapoint[dp_idx] = token;
    }
  }

  // The residuals are used only as the training set for the AH codebook (see
  // TrainSingleMachine in base/internal/tree_x_hybrid_factory.cc), so training
  // on a representative subset rather than the full dataset cuts build time and
  // peak memory roughly in half with negligible recall impact. Quantization of
  // the full dataset happens separately in BuildLeafSearchers and is unaffected.
  // The training-set requirement of the 2-D, 16-center AH codebooks is
  // essentially independent of the full dataset size. Use a 50% sample with a
  // floor for small datasets and a cap to bound large-dataset memory usage.
  const size_t dataset_size = dataset.size();
  const size_t sample_size = std::min(
      std::clamp(dataset_size / 2, size_t{100000}, size_t{2000000}),
      dataset_size);

  // Select sample_size datapoint indices via reservoir sampling. The seed is
  // fixed so the sample (and therefore the codebook) is deterministic.
  vector<DatapointIndex> sample_indices(sample_size);
  {
    std::mt19937 g(2023);
    std::uniform_int_distribution<uint64_t> dis;
    for (size_t i = 0; i < dataset.size(); ++i) {
      if (i < sample_size) {
        sample_indices[i] = i;
      } else {
        uint64_t rdx = dis(
            g, std::uniform_int_distribution<uint64_t>::param_type(0, i));
        if (rdx < sample_size) sample_indices[rdx] = i;
      }
    }
  }

  vector<float> residuals_storage;
  auto loop_body = [&](size_t sample_pos, bool is_first_dp)
                       SCANN_INLINE_LAMBDA -> Status {
    const DatapointIndex dp_idx = sample_indices[sample_pos];
    const uint32_t token = tokens_by_datapoint[dp_idx];
    SCANN_ASSIGN_OR_RETURN(auto residual, get_residual(dataset[dp_idx], token));

    if (is_first_dp) {
      residuals_storage =
          vector<float>(sample_size * residual.dimensionality());
    } else {
      DCHECK_EQ(residuals_storage.size(),
                sample_size * residual.dimensionality());
    }
    std::copy(residual.values().begin(), residual.values().end(),
              residuals_storage.begin() +
                  sample_pos * residual.dimensionality());
    return OkStatus();
  };

  SCANN_RETURN_IF_ERROR(loop_body(0, true));
  SCANN_RETURN_IF_ERROR(ParallelForWithStatus<1>(
      Seq(1, sample_size), parallelization_pool,
      [&](size_t sample_pos) { return loop_body(sample_pos, false); }));
  return DenseDataset<float>(std::move(residuals_storage), sample_size);
}]==]
    _tahr_content "${_tahr_content}")
file(WRITE "${_tahr_dst}" "${_tahr_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_tahr_src}")
list(APPEND SCANN_SOURCES "${_tahr_dst}")
