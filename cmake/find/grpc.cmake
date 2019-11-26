if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/grpc/CMakeLists.txt")
    set (MISSING_GRPC_LIBRARY 1)
endif()

if (NOT MISSING_GRPC_LIBRARY)
    set (USE_GRPC 1)
endif()