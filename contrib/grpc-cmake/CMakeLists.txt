set(ENABLE_GRPC_DEFAULT ${ENABLE_LIBRARIES})
option(ENABLE_GRPC "Use gRPC" ${ENABLE_GRPC_DEFAULT})

if(NOT ENABLE_GRPC)
  message(STATUS "Not using gRPC")
  return()
endif()

set(_gRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
set(_gRPC_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/grpc")

if(TARGET OpenSSL::SSL)
  set(gRPC_USE_UNSECURE_LIBRARIES FALSE)
else()
  set(gRPC_USE_UNSECURE_LIBRARIES TRUE)
endif()

include(grpc.cmake)
include(protobuf_generate_grpc.cmake)

set(gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)
set(gRPC_PYTHON_PLUGIN $<TARGET_FILE:grpc_python_plugin>)

set(gRPC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/grpc/include")
if(gRPC_USE_UNSECURE_LIBRARIES)
  set(gRPC_LIBRARIES grpc_unsecure grpc++_unsecure)
else()
  set(gRPC_LIBRARIES grpc grpc++)
endif()
add_library(_ch_contrib_grpc INTERFACE)
target_link_libraries(_ch_contrib_grpc INTERFACE ${gRPC_LIBRARIES})
target_include_directories(_ch_contrib_grpc SYSTEM INTERFACE ${gRPC_INCLUDE_DIRS})
add_library(ch_contrib::grpc ALIAS _ch_contrib_grpc)
