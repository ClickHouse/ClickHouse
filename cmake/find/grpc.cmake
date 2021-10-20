# disable grpc due to conflicts of abseil (required by grpc) dynamic annotations with libtsan.a
if (SANITIZE STREQUAL "thread" AND COMPILER_GCC)
  set(ENABLE_GRPC_DEFAULT OFF)
else()
  set(ENABLE_GRPC_DEFAULT ${ENABLE_LIBRARIES})
endif()

option(ENABLE_GRPC "Use gRPC" ${ENABLE_GRPC_DEFAULT})

if(NOT ENABLE_GRPC)
  if(USE_INTERNAL_GRPC_LIBRARY)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal gRPC library with ENABLE_GRPC=OFF")
  endif()
  return()
endif()

if(NOT USE_PROTOBUF)
  message(WARNING "Cannot use gRPC library without protobuf")
endif()

# Normally we use the internal gRPC framework.
# You can set USE_INTERNAL_GRPC_LIBRARY to OFF to force using the external gRPC framework, which should be installed in the system in this case.
# The external gRPC framework can be installed in the system by running
# sudo apt-get install libgrpc++-dev protobuf-compiler-grpc
option(USE_INTERNAL_GRPC_LIBRARY "Set to FALSE to use system gRPC library instead of bundled. (Experimental. Set to OFF on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/grpc/CMakeLists.txt")
  if(USE_INTERNAL_GRPC_LIBRARY)
    message(WARNING "submodule contrib/grpc is missing. to fix try run: \n git submodule update --init")
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal grpc")
    set(USE_INTERNAL_GRPC_LIBRARY 0)
  endif()
  set(MISSING_INTERNAL_GRPC_LIBRARY 1)
endif()

if(USE_SSL)
  set(gRPC_USE_UNSECURE_LIBRARIES FALSE)
else()
  set(gRPC_USE_UNSECURE_LIBRARIES TRUE)
endif()

if(NOT USE_INTERNAL_GRPC_LIBRARY)
  find_package(gRPC)
  if(NOT gRPC_INCLUDE_DIRS OR NOT gRPC_LIBRARIES)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't find system gRPC library")
    set(EXTERNAL_GRPC_LIBRARY_FOUND 0)
  elseif(NOT gRPC_CPP_PLUGIN)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't find system grpc_cpp_plugin")
    set(EXTERNAL_GRPC_LIBRARY_FOUND 0)
  else()
    set(EXTERNAL_GRPC_LIBRARY_FOUND 1)
    set(USE_GRPC 1)
  endif()
endif()

if(NOT EXTERNAL_GRPC_LIBRARY_FOUND AND NOT MISSING_INTERNAL_GRPC_LIBRARY)
  set(gRPC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/grpc/include")
  if(gRPC_USE_UNSECURE_LIBRARIES)
    set(gRPC_LIBRARIES grpc_unsecure grpc++_unsecure)
  else()
    set(gRPC_LIBRARIES grpc grpc++)
  endif()
  set(gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)
  set(gRPC_PYTHON_PLUGIN $<TARGET_FILE:grpc_python_plugin>)

  include("${ClickHouse_SOURCE_DIR}/contrib/grpc-cmake/protobuf_generate_grpc.cmake")

  set(USE_INTERNAL_GRPC_LIBRARY 1)
  set(USE_GRPC 1)
endif()

message(STATUS "Using gRPC=${USE_GRPC}: ${gRPC_INCLUDE_DIRS} : ${gRPC_LIBRARIES} : ${gRPC_CPP_PLUGIN}")
