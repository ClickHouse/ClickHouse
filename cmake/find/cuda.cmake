option (USE_CUDA "Enable highly experimental CUDA support" OFF)

if (USE_CUDA)
    set (CUB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/contrib/cub)
    include(FindCUDA)
    include_directories(${CUDA_INCLUDE_DIRS})
    set (CUDA_PROPAGATE_HOST_FLAGS ON)
    set (CUDA_NVCC_FLAGS ${CUDA_NVCC_FLAGS};-std=c++14)
endif ()
