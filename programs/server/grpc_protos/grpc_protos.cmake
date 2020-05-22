get_filename_component(grpc_server_proto "${CMAKE_CURRENT_SOURCE_DIR}/grpc_protos/GrpcConnection.proto" ABSOLUTE)
protobuf_generate_grpc_cpp(GRPC_SRCS GRPC_HDRS ${grpc_server_proto})
