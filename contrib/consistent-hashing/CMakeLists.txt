add_library(consistent-hashing consistent_hashing.cpp popcount.cpp)
target_include_directories(consistent-hashing PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
