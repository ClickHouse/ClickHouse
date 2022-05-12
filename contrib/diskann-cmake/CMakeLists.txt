list(APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/modules")

set(DISKANN_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/diskann")
set(DISKANN_SOURCE_DIR "${DISKANN_PROJECT_DIR}/src")

message("${CMAKE_MODULE_PATH}")

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
    "${ClickHouse_SOURCE_DIR}"
)

target_compile_options(_diskann PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-mavx2 -mfma -mf16c -mpopcnt>)

# MKL dependency
find_package(MKL)
if(MKL_FOUND)
  message("Found MKL! ${MKL_ROOT}")
  target_include_directories(_diskann PUBLIC "${MKL_ROOT}/include")
  target_link_libraries(_diskann PUBLIC ${MKL_LIBRARIES})
else()
  message(FATAL_ERROR "MKL not found!")
endif()

# OpenMP dependancy
target_link_libraries(_diskann PRIVATE ch_contrib::openmp)

# Boost dependancy
target_link_libraries(_diskann PRIVATE boost::headers_only)

# gperftools dependancy
target_link_libraries(_diskann PRIVATE ch_contrib::gperftools)

# aiolib dependancy
target_include_directories(_diskann SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/aio/include")

add_library(ch_contrib::diskann ALIAS _diskann)
