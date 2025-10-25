option(ENABLE_SUBSTRAIT "Enable Substrait" ${ENABLE_LIBRARIES})

if(NOT ENABLE_SUBSTRAIT)
  message(STATUS "Not using Substrait")
  return()
endif()

if(NOT TARGET ch_contrib::protoc)
  message(WARNING "Substrait requires protobuf, but protobuf is not available")
  return()
endif()

set(SUBSTRAIT_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/substrait")
set(SUBSTRAIT_PROTO_DIR "${SUBSTRAIT_SOURCE_DIR}/proto")

# Generate C++ files from proto files
set(SUBSTRAIT_PROTO_FILES
    ${SUBSTRAIT_PROTO_DIR}/substrait/algebra.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/capabilities.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/extended_expression.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/extensions/extensions.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/function.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/parameterized_types.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/plan.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/type.proto
    ${SUBSTRAIT_PROTO_DIR}/substrait/type_expressions.proto
)

# Set the import directory for protobuf generation
# This is critical - it tells protoc where to find the proto files
# and how to construct the import paths
# We need both the substrait proto directory AND the google protobuf src directory
set(Protobuf_IMPORT_DIRS 
    ${SUBSTRAIT_PROTO_DIR}
    ${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src
)
set(PROTOBUF_GENERATE_CPP_APPEND_PATH FALSE)

# Generate the C++ source and header files from the proto files
protobuf_generate_cpp(SUBSTRAIT_PROTO_SRCS SUBSTRAIT_PROTO_HDRS ${SUBSTRAIT_PROTO_FILES})

# Ignore warnings while compiling protobuf-generated files
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated files
set(CMAKE_CXX_CLANG_TIDY "")

# Create the substrait library
add_library(substrait ${SUBSTRAIT_PROTO_SRCS} ${SUBSTRAIT_PROTO_HDRS})

# Add include directories
# The SYSTEM keyword suppresses warnings from these headers
target_include_directories(substrait SYSTEM PUBLIC 
    ${CMAKE_CURRENT_BINARY_DIR}  # For generated .pb.h files
    ${SUBSTRAIT_PROTO_DIR}       # For proto files (if needed)
)

# Link with protobuf
target_link_libraries(substrait PUBLIC ch_contrib::protoc)

# Create an alias for consistency with other contrib libraries
add_library(ch_contrib::substrait ALIAS substrait)

message(STATUS "Substrait protobuf files will be generated at: ${CMAKE_CURRENT_BINARY_DIR}")
