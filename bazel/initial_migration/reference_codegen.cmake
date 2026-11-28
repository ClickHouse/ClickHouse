# Extract the target architecture's inline TableGen definitions even when
# configuring on another architecture. Bazel builds execution tools through
# its own platform transition; CMake's imported host executables hide their
# sources from the File API. This configuration is for graph extraction only.
if (PROJECT_NAME STREQUAL "ClickHouse")
    set(CMAKE_CROSSCOMPILING FALSE)
endif()
