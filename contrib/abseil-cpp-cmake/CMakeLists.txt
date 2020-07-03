set(ABSL_USE_EXTERNAL_GOOGLETEST ON)
set(ABSL_RUN_TESTS OFF)

# There is no abseil in distro repos anyway
option (USE_INTERNAL_ABSEIL_CPP "Use internal abseil library" ON)

# XXX:
# abseil-cpp/absl/strings/internal/str_format/bind.h:94:11: error: 'enable_if' is a clang extension [-Werror,-Wgcc-compat]
# ... and others ...
add_definitions (-Wno-gcc-compat)

add_subdirectory (../abseil-cpp ${CMAKE_CURRENT_BINARY_DIR}/abseil-cpp)
