list(APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/modules")

set(DISKANN_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/diskann")
set(DISKANN_SOURCE_DIR "${DISKANN_PROJECT_DIR}/src")

set(DISKANN_SOURCES 
    ${DISKANN_SOURCE_DIR}/ann_exception.cpp 
    ${DISKANN_SOURCE_DIR}/aux_utils.cpp 
    ${DISKANN_SOURCE_DIR}/index.cpp    
    ${DISKANN_SOURCE_DIR}/linux_aligned_file_reader.cpp 
    ${DISKANN_SOURCE_DIR}/math_utils.cpp 
    ${DISKANN_SOURCE_DIR}/memory_mapper.cpp
    ${DISKANN_SOURCE_DIR}/partition_and_pq.cpp  
    ${DISKANN_SOURCE_DIR}/pq_flash_index.cpp 
    ${DISKANN_SOURCE_DIR}/logger.cpp 
    ${DISKANN_SOURCE_DIR}/utils.cpp
)

add_library(_diskann ${DISKANN_SOURCES})

target_include_directories (_diskann SYSTEM PUBLIC
    "${DISKANN_PROJECT_DIR}/include"
)

target_compile_options(_diskann PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-mavx2 -mfma -mf16c -mpopcnt>)

# MKL dependency
find_package(MKL)
if(MKL_FOUND)
  message("Found MKL! ${MKL_ROOT}/include ${mkl_core_file}")
  target_include_directories(_diskann PUBLIC "${MKL_ROOT}/include")

  set(LIBMKL_THREAD_PATH
      "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/mkl/libmkl_intel_thread.a"
  )

  set(LIBMKL_ILP64_PATH
    "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/mkl/libmkl_intel_ilp64.a"
  )

  set(LIBMKL_CORE_PATH
    "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/mkl/libmkl_core.a"
  )

  set (CMAKE_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} ${LIBMKL_THREAD_PATH}" )
  set (CMAKE_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} ${LIBMKL_ILP64_PATH}" )
  set (CMAKE_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} ${LIBMKL_CORE_PATH}" )

else()
  message(FATAL_ERROR "MKL not found!")
endif()

# OpenMP dependancy
find_package(OpenMP REQUIRED)
target_link_libraries(_diskann PRIVATE OpenMP::OpenMP_C)
target_link_libraries(_diskann PRIVATE OpenMP::OpenMP_CXX)

# Boost dependancy
target_link_libraries(_diskann PRIVATE boost::headers_only)

# gperftools dependancy
target_link_libraries(_diskann PRIVATE ch_contrib::gperftools)

# aiolib dependancy
target_include_directories(_diskann SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/aio/include")

add_library(ch_contrib::diskann ALIAS _diskann)
