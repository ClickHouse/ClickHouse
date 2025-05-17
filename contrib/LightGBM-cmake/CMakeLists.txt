include(CheckCXXSourceCompiles)

set(LIGHTGBM_DIR "${ClickHouse_SOURCE_DIR}/contrib/LightGBM")
set(LIGHTGBM_SOURCE_DIR "${LIGHTGBM_DIR}/src")
set(LIGHTGBM_INCLUDE_DIR "${LIGHTGBM_DIR}/include")

set(LIGHTGBM_SOURCES
    ${LIGHTGBM_SOURCE_DIR}/boosting/boosting.cpp
    ${LIGHTGBM_SOURCE_DIR}/boosting/gbdt_model_text.cpp
    ${LIGHTGBM_SOURCE_DIR}/boosting/gbdt_prediction.cpp
    ${LIGHTGBM_SOURCE_DIR}/boosting/gbdt.cpp
    ${LIGHTGBM_SOURCE_DIR}/boosting/prediction_early_stop.cpp
    ${LIGHTGBM_SOURCE_DIR}/boosting/sample_strategy.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/bin.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/config_auto.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/config.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/dataset_loader.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/dataset.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/file_io.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/json11.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/metadata.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/parser.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/train_share_states.cpp
    ${LIGHTGBM_SOURCE_DIR}/io/tree.cpp
    ${LIGHTGBM_SOURCE_DIR}/metric/dcg_calculator.cpp
    ${LIGHTGBM_SOURCE_DIR}/metric/metric.cpp
    ${LIGHTGBM_SOURCE_DIR}/network/linker_topo.cpp
    ${LIGHTGBM_SOURCE_DIR}/network/linkers_mpi.cpp
    ${LIGHTGBM_SOURCE_DIR}/network/linkers_socket.cpp
    ${LIGHTGBM_SOURCE_DIR}/network/network.cpp
    ${LIGHTGBM_SOURCE_DIR}/objective/objective_function.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/data_parallel_tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/feature_histogram.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/feature_parallel_tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/gpu_tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/gradient_discretizer.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/linear_tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/serial_tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/treelearner/voting_parallel_tree_learner.cpp
    ${LIGHTGBM_SOURCE_DIR}/utils/openmp_wrapper.cpp
    ${LIGHTGBM_SOURCE_DIR}/c_api.cpp
)

set(LIGHTGBM_EXTERNAL_INCLUDE_DIRS
    ${LIGHTGBM_DIR}/external_libs/eigen
    ${LIGHTGBM_DIR}/external_libs/fast_double_parser/include
    ${ClickHouse_SOURCE_DIR}/contrib/fmtlib/include
)

set(LIGHTGBM_COMPILE_DEFENITIONS
    -DEIGEN_MPL2_ONLY
    -DEIGEN_DONT_PARALLELIZE
    -DUSE_SOCKET
)

check_cxx_source_compiles("
#include <xmmintrin.h>
int main() {
  int a = 0;
  _mm_prefetch(&a, _MM_HINT_NTA);
  return 0;
}
" LIGHTGBM_MM_PREFETCH)

if(${LIGHTGBM_MM_PREFETCH})
    list(APPEND LIGHTGBM_COMPILE_DEFENITIONS -DMM_PREFETCH)
endif()

check_cxx_source_compiles("
#include <mm_malloc.h>
int main() {
  char *a = (char*)_mm_malloc(8, 16);
  _mm_free(a);
  return 0;
}
" LIGHTGBM_MM_MALLOC)

if(${LIGHTGBM_MM_MALLOC})
    list(APPEND LIGHTGBM_COMPILE_DEFENITIONS -DMM_MALLOC)
endif()

# LightGBM doesn't support fmtlib newest versions with 'consteval' formatting
set(CMAKE_CXX_STANDARD 14)

add_library(_lightgbm ${LIGHTGBM_SOURCES})
target_compile_definitions(_lightgbm PRIVATE ${LIGHTGBM_COMPILE_DEFENITIONS})
target_include_directories(_lightgbm PUBLIC ${LIGHTGBM_INCLUDE_DIR} ${LIGHTGBM_EXTERNAL_INCLUDE_DIRS})

add_library(ch_contrib::lightgbm ALIAS _lightgbm)
