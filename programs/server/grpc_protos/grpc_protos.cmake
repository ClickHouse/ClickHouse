include_directories(${CMAKE_CURRENT_SOURCE_DIR}/etcd_protos)
get_filename_component(rpc_proto "${CMAKE_CURRENT_SOURCE_DIR}/grpc_protos/helloworld.proto" ABSOLUTE)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${rpc_proto})
PROTOBUF_GENERATE_GRPC_CPP(GRPC_SRCS GRPC_HDRS ${rpc_proto})