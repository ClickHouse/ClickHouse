# TODO: optimizations, instinsics. See libs' cmake/

set(XGBOOST_DIR "${ClickHouse_SOURCE_DIR}/contrib/xgboost")
set(XGBOOST_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/xgboost")
set(XGBOOST_SOURCE_DIR "${XGBOOST_DIR}/src")
set(XGBOOST_INCLUDE_DIR "${XGBOOST_DIR}/include")

set(DMLC_CORE_LIB "${XGBOOST_DIR}/dmlc-core")
set(DMLC_CORE_BINARY_DIR "${XGBOOST_BINARY_DIR}/dmlc-core")
set(DMLC_CORE_SOURCE_DIR "${DMLC_CORE_LIB}/src")
set(DMLC_CORE_INCLUDE_DIR "${DMLC_CORE_LIB}/include")

# dmlc-core

include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckSymbolExists)
include(CheckLibraryExists)
include(CheckCXXCompilerFlag)
include(TestBigEndian)

set(DMLC_CORE_SOURCES
    ${DMLC_CORE_SOURCE_DIR}/config.cc
    ${DMLC_CORE_SOURCE_DIR}/data.cc
    ${DMLC_CORE_SOURCE_DIR}/io.cc
    ${DMLC_CORE_SOURCE_DIR}/recordio.cc
    ${DMLC_CORE_SOURCE_DIR}/io/line_split.cc
    ${DMLC_CORE_SOURCE_DIR}/io/recordio_split.cc
    ${DMLC_CORE_SOURCE_DIR}/io/indexed_recordio_split.cc
    ${DMLC_CORE_SOURCE_DIR}/io/input_split_base.cc
    ${DMLC_CORE_SOURCE_DIR}/io/filesys.cc
    ${DMLC_CORE_SOURCE_DIR}/io/local_filesys.cc
)

set(DMLC_CORE_HEADERS
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/concurrentqueue.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/any.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/threadediter.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/blockingconcurrentqueue.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/build_config_default.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/optional.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/common.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/recordio.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/io.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/serializer.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/base.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/memory_io.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/concurrency.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/input_split_shuffle.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/lua.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/filesystem.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/array_view.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/strtonum.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/type_traits.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/parameter.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/omp.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/data.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/json.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/thread_group.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/thread_local.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/timer.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/config.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/logging.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/memory.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/endian.h
    ${DMLC_CORE_INCLUDE_DIR}/dmlc/registry.h
)

add_library(_dmlc_core ${DMLC_CORE_SOURCES} ${DMLC_CORE_HEADERS})
target_include_directories(_dmlc_core PUBLIC ${DMLC_CORE_INCLUDE_DIR} ${DMLC_CORE_BINARY_DIR})
target_compile_features(_dmlc_core PRIVATE cxx_std_14)

set(DMLC_CORE_COMPILE_DEFENITIONS
    -DDMLC_USE_CXX11=1
    -DDMLC_USE_CXX14=1
    -D_XOPEN_SOURCE=700
    -D_POSIX_SOURCE
    -D_POSIX_C_SOURCE=200809L
    -D_DARWIN_C_SOURCE
    -D__USE_XOPEN2K8
    -DDMLC_CORE_USE_CMAKE
    -DDMLC_EXECINFO_H_PRESENT=0
)

check_symbol_exists(fopen64 stdio.h DMLC_FOPEN_64_PRESENT)
if (DMLC_FOPEN_64_PRESENT)
    list(APPEND DMLC_CORE_COMPILE_DEFENITIONS -DDMLC_FOPEN_64_PRESENT)
endif()

check_symbol_exists(nanosleep time.h DMLC_NANOSLEEP_PRESENT)
if (DMLC_NANOSLEEP_PRESENT)
    list(APPEND DMLC_CORE_COMPILE_DEFENITIONS -DDMLC_NANOSLEEP_PRESENT)
endif()

check_include_file_cxx(cxxabi.h DMLC_CXXABI_H_PRESENT)
if (DMLC_CXXABI_H_PRESENT)
    list(APPEND DMLC_CORE_COMPILE_DEFENITIONS -DDMLC_CXXABI_H_PRESENT)
endif()

# Not a compile defenition but a build_config.h.in constant
test_big_endian(DMLC_CORE_BIG_ENDIAN)
if(DMLC_CORE_BIG_ENDIAN)
    set(DMLC_CMAKE_LITTLE_ENDIAN 0)
else()
    set(DMLC_CMAKE_LITTLE_ENDIAN 1)
endif()

target_compile_definitions(_dmlc_core PRIVATE ${DMLC_CORE_COMPILE_DEFENITIONS})

# Generate config with constants into binary directory
configure_file(
    ${DMLC_CORE_LIB}/cmake/build_config.h.in
    ${DMLC_CORE_BINARY_DIR}/dmlc/build_config.h
)

set(DMLC_CORE_COMPILE_OPTIONS
    -Wall
    -Wno-unknown-pragmas
    -fPIC
)

check_cxx_compiler_flag("-msse2" DMLC_CORE_SUPPORT_MSSE2)
if(DMLC_CORE_SUPPORT_MSSE2)
    list(APPEND DMLC_CORE_COMPILE_OPTIONS -msse2)
endif()

target_compile_options(_dmlc_core PRIVATE ${DMLC_CORE_COMPILE_OPTIONS})

check_library_exists(rt clock_gettime "time.h" HAVE_CLOCK_GETTIME_IN_LIBRT)
if(HAVE_CLOCK_GETTIME_IN_LIBRT)
    target_link_libraries(_dmlc_core PRIVATE rt)
endif()

# XGBoost

set(XGBOOST_SOURCES
    ${XGBOOST_SOURCE_DIR}/metric/elementwise_metric.cc
    ${XGBOOST_SOURCE_DIR}/metric/survival_metric.cc
    ${XGBOOST_SOURCE_DIR}/metric/auc.cc
    ${XGBOOST_SOURCE_DIR}/metric/metric.cc
    ${XGBOOST_SOURCE_DIR}/metric/multiclass_metric.cc
    ${XGBOOST_SOURCE_DIR}/metric/rank_metric.cc
    ${XGBOOST_SOURCE_DIR}/collective/allgather.cc
    ${XGBOOST_SOURCE_DIR}/collective/tracker.cc
    ${XGBOOST_SOURCE_DIR}/collective/comm_group.cc
    ${XGBOOST_SOURCE_DIR}/collective/nccl_stub.cc
    ${XGBOOST_SOURCE_DIR}/collective/comm.cc
    ${XGBOOST_SOURCE_DIR}/collective/coll.cc
    ${XGBOOST_SOURCE_DIR}/collective/broadcast.cc
    ${XGBOOST_SOURCE_DIR}/collective/allreduce.cc
    ${XGBOOST_SOURCE_DIR}/collective/socket.cc
    ${XGBOOST_SOURCE_DIR}/collective/loop.cc
    ${XGBOOST_SOURCE_DIR}/collective/result.cc
    ${XGBOOST_SOURCE_DIR}/collective/in_memory_handler.cc
    ${XGBOOST_SOURCE_DIR}/global_config.cc
    ${XGBOOST_SOURCE_DIR}/tree/constraints.cc
    ${XGBOOST_SOURCE_DIR}/tree/tree_updater.cc
    ${XGBOOST_SOURCE_DIR}/tree/hist/hist_param.cc
    ${XGBOOST_SOURCE_DIR}/tree/hist/histogram.cc
    ${XGBOOST_SOURCE_DIR}/tree/updater_quantile_hist.cc
    ${XGBOOST_SOURCE_DIR}/tree/updater_refresh.cc
    ${XGBOOST_SOURCE_DIR}/tree/tree_model.cc
    ${XGBOOST_SOURCE_DIR}/tree/multi_target_tree_model.cc
    ${XGBOOST_SOURCE_DIR}/tree/updater_prune.cc
    ${XGBOOST_SOURCE_DIR}/tree/updater_colmaker.cc
    ${XGBOOST_SOURCE_DIR}/tree/param.cc
    ${XGBOOST_SOURCE_DIR}/tree/updater_approx.cc
    ${XGBOOST_SOURCE_DIR}/tree/updater_sync.cc
    ${XGBOOST_SOURCE_DIR}/tree/fit_stump.cc
    ${XGBOOST_SOURCE_DIR}/learner.cc
    ${XGBOOST_SOURCE_DIR}/logging.cc
    ${XGBOOST_SOURCE_DIR}/gbm/gbtree_model.cc
    ${XGBOOST_SOURCE_DIR}/gbm/gbm.cc
    ${XGBOOST_SOURCE_DIR}/gbm/gblinear.cc
    ${XGBOOST_SOURCE_DIR}/gbm/gbtree.cc
    ${XGBOOST_SOURCE_DIR}/gbm/gblinear_model.cc
    ${XGBOOST_SOURCE_DIR}/c_api/c_api_error.cc
    ${XGBOOST_SOURCE_DIR}/c_api/c_api.cc
    ${XGBOOST_SOURCE_DIR}/c_api/coll_c_api.cc
    ${XGBOOST_SOURCE_DIR}/linear/updater_coordinate.cc
    ${XGBOOST_SOURCE_DIR}/linear/linear_updater.cc
    ${XGBOOST_SOURCE_DIR}/linear/updater_shotgun.cc
    ${XGBOOST_SOURCE_DIR}/predictor/predictor.cc
    ${XGBOOST_SOURCE_DIR}/predictor/cpu_treeshap.cc
    ${XGBOOST_SOURCE_DIR}/predictor/cpu_predictor.cc
    ${XGBOOST_SOURCE_DIR}/objective/hinge.cc
    ${XGBOOST_SOURCE_DIR}/objective/init_estimation.cc
    ${XGBOOST_SOURCE_DIR}/objective/aft_obj.cc
    ${XGBOOST_SOURCE_DIR}/objective/quantile_obj.cc
    ${XGBOOST_SOURCE_DIR}/objective/lambdarank_obj.cc
    ${XGBOOST_SOURCE_DIR}/objective/objective.cc
    ${XGBOOST_SOURCE_DIR}/objective/adaptive.cc
    ${XGBOOST_SOURCE_DIR}/objective/multiclass_obj.cc
    ${XGBOOST_SOURCE_DIR}/objective/regression_obj.cc
    ${XGBOOST_SOURCE_DIR}/data/proxy_dmatrix.cc
    ${XGBOOST_SOURCE_DIR}/data/batch_utils.cc
    ${XGBOOST_SOURCE_DIR}/data/adapter.cc
    ${XGBOOST_SOURCE_DIR}/data/quantile_dmatrix.cc
    ${XGBOOST_SOURCE_DIR}/data/gradient_index.cc
    ${XGBOOST_SOURCE_DIR}/data/gradient_index_page_source.cc
    ${XGBOOST_SOURCE_DIR}/data/iterative_dmatrix.cc
    ${XGBOOST_SOURCE_DIR}/data/cat_container.cc
    ${XGBOOST_SOURCE_DIR}/data/file_iterator.cc
    ${XGBOOST_SOURCE_DIR}/data/gradient_index_format.cc
    ${XGBOOST_SOURCE_DIR}/data/array_interface.cc
    ${XGBOOST_SOURCE_DIR}/data/simple_dmatrix.cc
    ${XGBOOST_SOURCE_DIR}/data/sparse_page_raw_format.cc
    ${XGBOOST_SOURCE_DIR}/data/validation.cc
    ${XGBOOST_SOURCE_DIR}/data/sparse_page_source.cc
    ${XGBOOST_SOURCE_DIR}/data/ellpack_page.cc
    ${XGBOOST_SOURCE_DIR}/data/extmem_quantile_dmatrix.cc
    ${XGBOOST_SOURCE_DIR}/data/data.cc
    ${XGBOOST_SOURCE_DIR}/data/sparse_page_dmatrix.cc
    ${XGBOOST_SOURCE_DIR}/common/common.cc
    ${XGBOOST_SOURCE_DIR}/common/cuda_dr_utils.cc
    ${XGBOOST_SOURCE_DIR}/common/charconv.cc
    ${XGBOOST_SOURCE_DIR}/common/error_msg.cc
    ${XGBOOST_SOURCE_DIR}/common/quantile.cc
    ${XGBOOST_SOURCE_DIR}/common/numeric.cc
    ${XGBOOST_SOURCE_DIR}/common/hist_util.cc
    ${XGBOOST_SOURCE_DIR}/common/ranking_utils.cc
    ${XGBOOST_SOURCE_DIR}/common/json.cc
    ${XGBOOST_SOURCE_DIR}/common/survival_util.cc
    ${XGBOOST_SOURCE_DIR}/common/host_device_vector.cc
    ${XGBOOST_SOURCE_DIR}/common/version.cc
    ${XGBOOST_SOURCE_DIR}/common/threading_utils.cc
    ${XGBOOST_SOURCE_DIR}/common/column_matrix.cc
    ${XGBOOST_SOURCE_DIR}/common/io.cc
    ${XGBOOST_SOURCE_DIR}/common/random.cc
    ${XGBOOST_SOURCE_DIR}/common/cuda_rt_utils.cc
    ${XGBOOST_SOURCE_DIR}/common/pseudo_huber.cc
    ${XGBOOST_SOURCE_DIR}/common/timer.cc
    ${XGBOOST_SOURCE_DIR}/common/quantile_loss_utils.cc
    ${XGBOOST_SOURCE_DIR}/common/stats.cc
    ${XGBOOST_SOURCE_DIR}/context.cc
)

add_library(_xgboost ${XGBOOST_SOURCES})
target_include_directories(_xgboost PUBLIC ${XGBOOST_INCLUDE_DIR} ${DMLC_CORE_INCLUDE_DIR})
target_compile_options(_xgboost PRIVATE -fPIC -std=c++17)
target_link_libraries(_xgboost PRIVATE
    _dmlc_core
    Threads::Threads ${CMAKE_THREAD_LIBS_INIT}
)

add_library(ch_contrib::xgboost ALIAS _xgboost)
