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

  find_package(BLAS REQUIRED)
  target_link_libraries(_diskann PRIVATE ${BLAS_LIBRARIES})

  find_package(LAPACK REQUIRED)
  target_link_libraries(_diskann PRIVATE ${LAPACK_LIBRARIES})

  # set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${MKL_LINKER_FLAGS}")
  # link_directories("${MKL_ROOT}/lib/intel64")
  # target_link_libraries(_diskann PRIVATE ${MKL_LIBRARIES})

#   add_library(_mkl_core_file SHARED IMPORTED)
#   set_property(TARGET _mkl_core_file PROPERTY IMPORTED_LOCATION ${mkl_core_file})

#   add_library(_mkl_intel_ilp64_file SHARED IMPORTED)
#   set_property(TARGET _mkl_intel_ilp64_file PROPERTY IMPORTED_LOCATION ${mkl_intel_ilp64_file})

#   add_library(_mkl_intel_thread_file SHARED IMPORTED)
#   set_property(TARGET _mkl_intel_thread_file PROPERTY IMPORTED_LOCATION ${mkl_intel_thread_file})

#   target_link_libraries(_diskann PRIVATE _mkl_core_file _mkl_intel_ilp64_file _mkl_intel_thread_file)
else()
  message("MKL not found!")

  find_package(BLAS REQUIRED)
  target_link_libraries(_diskann PRIVATE ${BLAS_LIBRARIES})

  find_package(LAPACK REQUIRED)
  target_link_libraries(_diskann PRIVATE ${LAPACK_LIBRARIES})
endif()

find_package(OpenMP REQUIRED)
target_link_libraries(_diskann PRIVATE OpenMP::OpenMP_C)
target_link_libraries(_diskann PRIVATE OpenMP::OpenMP_CXX)

target_link_libraries(_diskann PRIVATE boost::headers_only)

target_link_libraries(_diskann PRIVATE ch_contrib::gperftools)

target_include_directories(_diskann SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/aio/include")

# add_library(aiolib SHARED IMPORTED)
# set_target_properties(aiolib
#     PROPERTIES IMPORTED_LOCATION ${ClickHouse_SOURCE_DIR}/contrib/diskann-cmake/libaio.so
# )
# target_link_libraries(_diskann PRIVATE aiolib)

# find_package(AIO)
# if(HAVE_AIO)
#     target_include_directories(_diskann PUBLIC ${LIBAIO_INCLUDE_DIR})
#     target_link_libraries(_diskann PRIVATE ${LIBAIO_LIBRARIES})
# else()
#     message(FATAL_ERROR "AIO Not found!")
# endif()

#arget_link_libraries(_diskann PRIVATE mkl_intel_ilp64 mkl_intel_thread mkl_core iomp5 pthread m dl aio)

add_library(ch_contrib::diskann ALIAS _diskann)
