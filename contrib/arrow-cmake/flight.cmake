set (ENABLE_ARROW_FLIGHT_DEFAULT ${ENABLE_LIBRARIES})
option (ENABLE_ARROW_FLIGHT "Enable ArrowFlight" ${ENABLE_ARROW_FLIGHT_DEFAULT})

if (NOT ENABLE_ARROW_FLIGHT)
    message(STATUS "Not using ArrowFlight")
    return()
endif()

if(NOT ENABLE_GRPC)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Can't use ArrowFlight without gRPC")
    return()
endif()

# The Arrow Flight server and client of ClickHouse are implemented on top of gRPC and the native Arrow IPC code
# (`src/Processors/Formats/Impl/ArrowIPC`). Only the protocol is needed from Arrow: the messages and the service
# generated from `Flight.proto`, and the messages of Arrow Flight SQL from `FlightSql.proto`.
set(ARROW_FLIGHT_PROTO_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/format)
set(ARROW_FLIGHT_PROTO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/flight_proto)
set(ARROW_FLIGHT_GENERATED_SRC_DIR ${ARROW_FLIGHT_PROTO_INCLUDE_DIR}/arrow/flight)
set(ARROW_FLIGHT_SQL_GENERATED_SRC_DIR ${ARROW_FLIGHT_PROTO_INCLUDE_DIR}/arrow/flight/sql)

set(PROTOBUF_IMPORT_DIRS ${ARROW_FLIGHT_PROTO_DIR} ${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src)

PROTOBUF_GENERATE_GRPC_CPP(
    flight_sources
    flight_headers
    APPEND_PATH
    PROTOC_OUT_DIR ${ARROW_FLIGHT_GENERATED_SRC_DIR}
    ${ARROW_FLIGHT_PROTO_DIR}/Flight.proto
)

PROTOBUF_GENERATE_GRPC_CPP(
    flight_sql_sources
    flight_sql_headers
    APPEND_PATH
    PROTOC_OUT_DIR ${ARROW_FLIGHT_SQL_GENERATED_SRC_DIR}
    ${ARROW_FLIGHT_PROTO_DIR}/FlightSql.proto
)

add_library(_arrow_flight_proto ${flight_sources} ${flight_sql_sources})
add_library(ch_contrib::arrow_flight_proto ALIAS _arrow_flight_proto)

add_dependencies(_arrow_flight_proto _protoc grpc_cpp_plugin)
target_link_libraries(_arrow_flight_proto PUBLIC _protobuf grpc++)
# The generated `Flight.grpc.pb.cc` includes "Flight.pb.h" from its own directory.
target_include_directories(_arrow_flight_proto SYSTEM PUBLIC ${ARROW_FLIGHT_PROTO_INCLUDE_DIR} ${ARROW_FLIGHT_GENERATED_SRC_DIR})
