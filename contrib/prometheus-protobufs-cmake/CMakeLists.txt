option(ENABLE_PROMETHEUS_PROTOBUFS "Enable Prometheus Protobufs" ${ENABLE_PROTOBUF})

if(NOT ENABLE_PROMETHEUS_PROTOBUFS)
  message(STATUS "Not using prometheus-protobufs")
  return()
endif()

set(Protobuf_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")
set(Prometheus_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/prometheus-protobufs")
set(GogoProto_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/prometheus-protobufs-gogo")

# Protobuf_IMPORT_DIRS specify where the protobuf compiler will look for .proto files.
set(Old_Protobuf_IMPORT_DIRS ${Protobuf_IMPORT_DIRS})
list(APPEND Protobuf_IMPORT_DIRS "${Protobuf_INCLUDE_DIR}" "${Prometheus_INCLUDE_DIR}" "${GogoProto_INCLUDE_DIR}")

PROTOBUF_GENERATE_CPP(prometheus_protobufs_sources prometheus_protobufs_headers
    "prompb/remote.proto"
    "prompb/types.proto"
    "gogoproto/gogo.proto"
)

set(Protobuf_IMPORT_DIRS ${Old_Protobuf_IMPORT_DIRS})

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(_prometheus_protobufs ${prometheus_protobufs_sources} ${prometheus_protobufs_headers})
target_include_directories(_prometheus_protobufs SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries (_prometheus_protobufs PUBLIC ch_contrib::protobuf)

add_library (ch_contrib::prometheus_protobufs ALIAS _prometheus_protobufs)
