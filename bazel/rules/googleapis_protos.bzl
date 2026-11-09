"""Declare the protobuf and gRPC rules for the pinned Google APIs schemas."""

load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load(":googleapis.bzl", "GOOGLE_API_IMPORTS", "GOOGLE_API_PROTO_ROOT")
load(":grpc_proto_sources.bzl", "grpc_proto_sources")

def _proto_dep(schema):
    if schema.startswith("google/protobuf/"):
        return "@clickhouse_google_protobuf//:" + schema.removeprefix("google/protobuf/").removesuffix(".proto") + "_proto"
    return ":" + schema.removesuffix(".proto").replace("/", "_") + "_proto"

def google_api_protos():
    """Declare a proto_library, cc_proto_library and gRPC generator for each schema."""
    for schema, imports in GOOGLE_API_IMPORTS.items():
        stem = schema.removesuffix(".proto").replace("/", "_")
        proto = stem + "_proto"
        kwargs = {}
        if imports:
            kwargs["deps"] = [_proto_dep(path) for path in imports]
        if schema == "google/storage/v2/storage.proto":
            # The generated storage API exposes absl::Cord in its public header.
            kwargs["cc_deps"] = ["@clickhouse_abseil_cpp//:absl_cord"]
            kwargs["cc_implementation_deps"] = ["@clickhouse_abseil_cpp//:absl_strings"]

        proto_library(
            name = proto,
            srcs = [GOOGLE_API_PROTO_ROOT + "/" + schema],
            features = ["-fortify_source", "-default_compile_flags"],
            strip_import_prefix = GOOGLE_API_PROTO_ROOT,
            **kwargs
        )
        cc_proto_library(name = stem + "_cc_proto", deps = [":" + proto])
        grpc_proto_sources(name = stem + "_grpc_sources", proto = ":" + proto)
