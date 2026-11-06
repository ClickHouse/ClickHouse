set (ENABLE_ARROW_FLIGHT_DEFAULT ${ENABLE_LIBRARIES})
option (ENABLE_ARROW_FLIGHT "Enable ArrowFlight" ${ENABLE_ARROW_FLIGHT_DEFAULT})

if (NOT ENABLE_ARROW_FLIGHT)
    message(STATUS "Not using ArrowFlight")
    return()
endif()

if(NOT ENABLE_GRPC)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use ArrowFlight without gRPC")
endif()

set(GRPC_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/grpc/include)

set(ARROW_FLIGHT_SRC_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow/flight)
set(ARROW_FLIGHT_PROTO_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/format)
set(ARROW_FLIGHT_GENERATED_SRC_DIR ${ARROW_GENERATED_SRC_DIR}/arrow/flight)

add_custom_command(
    OUTPUT
        "${ARROW_FLIGHT_GENERATED_SRC_DIR}/Flight.grpc.pb.cc"
        "${ARROW_FLIGHT_GENERATED_SRC_DIR}/Flight.grpc.pb.h"
        "${ARROW_FLIGHT_GENERATED_SRC_DIR}/Flight.pb.cc"
        "${ARROW_FLIGHT_GENERATED_SRC_DIR}/Flight.pb.h"
    COMMAND ${PROTOBUF_EXECUTABLE}
    -I ${ARROW_FLIGHT_PROTO_DIR}
    -I "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src"
    --cpp_out="${ARROW_FLIGHT_GENERATED_SRC_DIR}"
    --grpc_out="${ARROW_FLIGHT_GENERATED_SRC_DIR}"
    --plugin=protoc-gen-grpc="${GRPC_EXECUTABLE}"
    "${ARROW_FLIGHT_PROTO_DIR}/Flight.proto"
)

# NOTE: we do not compile the ${ARROW_FLIGHT_GENERATED_SRCS} directly, instead
# compiling then via protocol_internal.cc which contains some gRPC template
# overrides to enable Flight-specific optimizations. See comments in
# protobuf-internal.cc
set(ARROW_FLIGHT_SRCS
        ${ARROW_FLIGHT_GENERATED_SRC_DIR}/Flight.pb.cc
        ${ARROW_FLIGHT_SRC_DIR}/client.cc
        ${ARROW_FLIGHT_SRC_DIR}/client_cookie_middleware.cc
        ${ARROW_FLIGHT_SRC_DIR}/client_tracing_middleware.cc
        ${ARROW_FLIGHT_SRC_DIR}/cookie_internal.cc
        ${ARROW_FLIGHT_SRC_DIR}/middleware.cc
        ${ARROW_FLIGHT_SRC_DIR}/serialization_internal.cc
        ${ARROW_FLIGHT_SRC_DIR}/server.cc
        ${ARROW_FLIGHT_SRC_DIR}/server_auth.cc
        ${ARROW_FLIGHT_SRC_DIR}/server_tracing_middleware.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport_server.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport/grpc/grpc_client.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport/grpc/grpc_server.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport/grpc/protocol_grpc_internal.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport/grpc/serialization_internal.cc
        ${ARROW_FLIGHT_SRC_DIR}/transport/grpc/util_internal.cc
        ${ARROW_FLIGHT_SRC_DIR}/types.cc
)

add_library(_arrow_flight ${ARROW_FLIGHT_SRCS})
add_library(ch_contrib::arrow_flight ALIAS _arrow_flight)

add_dependencies(_arrow_flight _protoc grpc_cpp_plugin)
target_link_libraries(_arrow_flight PUBLIC _arrow)
target_link_libraries(_arrow_flight PRIVATE _protobuf grpc++)
target_include_directories(_arrow_flight PRIVATE ${ARROW_GENERATED_SRC_DIR})
