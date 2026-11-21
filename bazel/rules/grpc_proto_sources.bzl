"""Generate C++ gRPC source and header outputs from ProtoInfo."""

load("@protobuf//bazel/common:proto_common.bzl", "proto_common")
load("@protobuf//bazel/common:proto_info.bzl", "ProtoInfo")
load("@protobuf//bazel/common:proto_lang_toolchain_info.bzl", "ProtoLangToolchainInfo")

def _grpc_proto_sources_impl(ctx):
    info = ctx.attr.proto[ProtoInfo]
    srcs = proto_common.declare_generated_files(ctx.actions, info, ".grpc.pb.cc")
    hdrs = proto_common.declare_generated_files(ctx.actions, info, ".grpc.pb.h")
    proto_common.compile(
        actions = ctx.actions,
        proto_info = info,
        proto_lang_toolchain_info = ctx.attr._toolchain[ProtoLangToolchainInfo],
        generated_files = srcs + hdrs,
    )

    return [
        DefaultInfo(files = depset(srcs)),
        OutputGroupInfo(grpc_headers = depset(hdrs)),
    ]

grpc_proto_sources = rule(
    implementation = _grpc_proto_sources_impl,
    attrs = {
        "proto": attr.label(mandatory = True, providers = [ProtoInfo]),
        "_toolchain": attr.label(
            default = Label("//bazel:clickhouse_grpc_proto"),
            providers = [ProtoLangToolchainInfo],
        ),
    },
)
