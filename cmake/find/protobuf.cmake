option(ENABLE_PROTOBUF "Enable protobuf" ${ENABLE_LIBRARIES})

if(NOT ENABLE_PROTOBUF)
  if(USE_INTERNAL_PROTOBUF_LIBRARY)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal protobuf with ENABLE_PROTOBUF=OFF")
  endif()
  return()
endif()

# Normally we use the internal protobuf library.
# You can set USE_INTERNAL_PROTOBUF_LIBRARY to OFF to force using the external protobuf library, which should be installed in the system in this case.
# The external protobuf library can be installed in the system by running
# sudo apt-get install libprotobuf-dev protobuf-compiler libprotoc-dev
option(USE_INTERNAL_PROTOBUF_LIBRARY "Set to FALSE to use system protobuf instead of bundled. (Experimental. Set to OFF on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/protobuf/cmake/CMakeLists.txt")
  if(USE_INTERNAL_PROTOBUF_LIBRARY)
    message(WARNING "submodule contrib/protobuf is missing. to fix try run: \n git submodule update --init")
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal protobuf")
    set(USE_INTERNAL_PROTOBUF_LIBRARY 0)
  endif()
  set(MISSING_INTERNAL_PROTOBUF_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_PROTOBUF_LIBRARY)
  find_package(Protobuf)
  if(NOT Protobuf_INCLUDE_DIR OR NOT Protobuf_LIBRARY)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't find system protobuf library")
    set(EXTERNAL_PROTOBUF_LIBRARY_FOUND 0)
  elseif(NOT Protobuf_PROTOC_EXECUTABLE)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't find system protobuf compiler")
    set(EXTERNAL_PROTOBUF_LIBRARY_FOUND 0)
  else()
    set(EXTERNAL_PROTOBUF_LIBRARY_FOUND 1)
    set(USE_PROTOBUF 1)
  endif()
endif()

if(NOT EXTERNAL_PROTOBUF_LIBRARY_FOUND AND NOT MISSING_INTERNAL_PROTOBUF_LIBRARY)
  set(Protobuf_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/protobuf/src")
  set(Protobuf_LIBRARY libprotobuf)
  set(Protobuf_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>")
  set(Protobuf_PROTOC_LIBRARY libprotoc)

  include("${ClickHouse_SOURCE_DIR}/contrib/protobuf-cmake/protobuf_generate.cmake")

  set(USE_INTERNAL_PROTOBUF_LIBRARY 1)
  set(USE_PROTOBUF 1)
endif()

if(OS_FREEBSD AND SANITIZE STREQUAL "address")
  # ../contrib/protobuf/src/google/protobuf/arena_impl.h:45:10: fatal error: 'sanitizer/asan_interface.h' file not found
  # #include <sanitizer/asan_interface.h>
  if(LLVM_INCLUDE_DIRS)
    set(Protobuf_INCLUDE_DIR "${Protobuf_INCLUDE_DIR}" ${LLVM_INCLUDE_DIRS})
  else()
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use protobuf on FreeBSD with address sanitizer without LLVM")
    set(USE_PROTOBUF 0)
  endif()
endif()

message(STATUS "Using protobuf=${USE_PROTOBUF}: ${Protobuf_INCLUDE_DIR} : ${Protobuf_LIBRARY} : ${Protobuf_PROTOC_EXECUTABLE} : ${Protobuf_PROTOC_LIBRARY}")
