# Reduce Tree-AH artifact persistence and training memory without changing the
# serialized index representation or the ordinary AH training path.

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

# Keep ClickHouse's direct block-major residual training in a separate adapter.
# The upstream asymmetric-hashing training implementation remains unchanged.
list(APPEND SCANN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/scann_tree_ah_training.cpp")

set(_txhf_src "${SCANN_SOURCE_DIR}/scann/base/internal/tree_x_hybrid_factory.cc")
set(_txhf_dst "${CMAKE_CURRENT_BINARY_DIR}/tree_x_hybrid_factory.cc")
configure_file("${_txhf_src}" "${_txhf_dst}" COPYONLY)
file(READ "${_txhf_dst}" _txhf_content)
scann_checked_replace(
[==[#include "scann/base/internal/tree_x_hybrid_factory.h"]==]
[==[#include "scann/base/internal/tree_x_hybrid_factory.h"

#include <scann_tree_ah_training.h>]==]
    _txhf_content "${_txhf_content}")
scann_checked_replace(
[==[    SCANN_ASSIGN_OR_RETURN(
        DenseDataset<float> residuals,
        TreeAHHybridResidual::ComputeResiduals(
            *dense, kmeans_tree_partitioner.get(), datapoints_by_token,
            opts->parallelization_pool.get()));
    asymmetric_hashing2::TrainingOptions<float> training_opts(
        config.hash().asymmetric_hash(), quantization_distance, residuals,
        opts->parallelization_pool.get());
    SCANN_RETURN_IF_ERROR(training_opts.Validate());
    SCANN_ASSIGN_OR_RETURN(
        ah_model, asymmetric_hashing2::TrainSingleMachine(
                      residuals, training_opts, opts->parallelization_pool));]==]
[==[    SCANN_ASSIGN_OR_RETURN(
        ah_model, TrainTreeAHResidualModel(
                      *dense, *kmeans_tree_partitioner, datapoints_by_token,
                      config.hash().asymmetric_hash(), quantization_distance,
                      opts->parallelization_pool));]==]
    _txhf_content "${_txhf_content}")
file(WRITE "${_txhf_dst}" "${_txhf_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_txhf_src}")
list(APPEND SCANN_SOURCES "${_txhf_dst}")
