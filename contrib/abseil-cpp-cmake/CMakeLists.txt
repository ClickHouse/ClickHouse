set(ABSL_USE_EXTERNAL_GOOGLETEST ON)
set(ABSL_RUN_TESTS OFF)

# There is no abseil in distro repos anyway
option (USE_INTERNAL_ABSEIL_CPP "Use internal abseil library" ON)

add_subdirectory (../abseil-cpp ${CMAKE_CURRENT_BINARY_DIR}/abseil-cpp)
