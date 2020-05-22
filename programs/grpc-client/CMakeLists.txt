get_filename_component(grpc_server_proto "${CMAKE_CURRENT_SOURCE_DIR}/../server/grpc_protos/GrpcConnection.proto" ABSOLUTE)
protobuf_generate_grpc_cpp(GRPC_SRCS GRPC_HDRS ${grpc_server_proto})

add_executable(grpc-client grpc_client.cpp ${GRPC_SRCS} ${GRPC_HDRS})
target_include_directories(grpc-client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(grpc-client PRIVATE libgrpc++)