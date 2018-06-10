cmake_minimum_required(VERSION 2.8)
project(libconsistent-hashing CXX)

add_library(libconsistent-hashing yandex/consistent_hashing.cpp yandex/popcount.cpp mailru/sumbur.cpp)
target_include_directories(libconsistent-hashing PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})