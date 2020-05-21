option (ENABLE_CUDA "Enable highly experimental CUDA support" OFF)

if (ENABLE_CUDA)
    set (CUB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/contrib/cub)
    include(FindCUDA)
    if(CUDA_FOUND)
        set(USE_CUDA 1)
    endif()

    include_directories(${CUDA_INCLUDE_DIRS})
    set (CUDA_PROPAGATE_HOST_FLAGS ON)
    set (CUDA_NVCC_FLAGS ${CUDA_NVCC_FLAGS};-std=c++14)
endif ()

message(STATUS "Using cuda=${USE_CUDA}: ${CUDA_INCLUDE_DIRS} : ${CUDA_LIBRARIES} : ${CUDA_VERSION_STRING}")
