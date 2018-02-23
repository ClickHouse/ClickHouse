cmake_minimum_required(VERSION 2.8)
project(yandex-consistent-hashing CXX)

add_library(yandex-consistent-hashing yandex/consistent_hashing.cpp yandex/popcount.cpp)
target_include_directories(yandex-consistent-hashing PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})