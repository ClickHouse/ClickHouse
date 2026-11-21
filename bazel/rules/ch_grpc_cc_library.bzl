"""Compile the gRPC outputs paired with a Google API proto_library."""

load(":ch_cc_library.bzl", "ch_cc_library")
load(":googleapis.bzl", "GOOGLE_API_SCHEMAS")

def ch_grpc_cc_library(name, proto, deps = [], **kwargs):
    """Compile *_grpc_sources with public headers and the matching *_cc_proto."""
    proto = native.package_relative_label(proto)
    stem = proto.name.removesuffix("_proto")
    grpc_sources = proto.same_package_label(stem + "_grpc_sources")
    cc_proto = proto.same_package_label(stem + "_cc_proto")
    headers = name + "_grpc_headers"
    native.filegroup(
        name = headers,
        srcs = [grpc_sources],
        output_group = "grpc_headers",
        visibility = ["//visibility:private"],
    )
    ch_cc_library(
        name = name,
        srcs = [grpc_sources],
        hdrs = [":" + headers],
        strip_include_prefix = "/" + proto.package + "/_virtual_imports/" + proto.name,
        deps = [
            cc_proto,
            Label("@clickhouse_grpc//:grpc++"),
        ] + deps,
        **kwargs
    )

def google_cloud_grpc_libraries(local_defines, storage_copts):
    """Declare Google Cloud C++'s gRPC libraries for the pinned Google APIs schemas."""

    # These two CMake target names differ from the names of their schemas.
    target_names = {
        "cloud_common_operation_metadata": "cloud_common_common",
        "storage_v2_storage": "storage",
    }
    for schema in GOOGLE_API_SCHEMAS:
        stem = schema.removeprefix("google/").removesuffix(".proto").replace("/", "_")
        target = target_names.get(stem, stem)
        ch_grpc_cc_library(
            name = "google_cloud_cpp_%s_protos" % target,
            proto = "@clickhouse//bazel:google_%s_proto" % stem,
            local_defines = local_defines,
            **({"copts": storage_copts} if target == "storage" else {})
        )
