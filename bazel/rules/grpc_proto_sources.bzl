"""Generate C++ gRPC sources and expose their public headers from ProtoInfo."""

load("@protobuf//bazel/common:proto_common.bzl", "proto_common")
load("@protobuf//bazel/common:proto_info.bzl", "ProtoInfo")
load("@protobuf//bazel/common:proto_lang_toolchain_info.bzl", "ProtoLangToolchainInfo")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

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

    roots = []
    for src, hdr in zip(info.direct_sources, hdrs):
        imported_hdr = proto_common.get_import_path(src).removesuffix(".proto") + ".grpc.pb.h"
        roots.append(hdr.path.removesuffix(imported_hdr).removesuffix("/"))

    return [
        DefaultInfo(files = depset(srcs)),
        CcInfo(compilation_context = cc_common.create_compilation_context(
            headers = depset(hdrs),
            includes = depset(roots),
        )),
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
    provides = [CcInfo],
)
