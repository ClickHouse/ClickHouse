package(default_visibility = ["//visibility:public"])

load(":config.bzl", "cc_toolchain_config")

cc_toolchain_suite(
    name = "toolchains",
    toolchains = {
        "k8|clang": ":clang-amd64",
    },
)

cc_toolchain_config(name = "clang-amd64-config")

cc_toolchain(
    name = "clang-amd64",
    toolchain_config = ":clang-amd64-config",

    all_files      = ":all",
    ar_files       = ":ar",
    compiler_files = ":compiler_components",
    linker_files   = ":linker_components",
    objcopy_files  = ":objcopy",

    dwp_files   = ":empty",
    strip_files = ":empty",
)

filegroup(name = "empty")

filegroup(
    name = "all",
    srcs = [
        ":binutils_components",
        ":compiler_components",
        ":linker_components",
    ],
)

filegroup(
    name = "binutils_components",
    srcs = [
        ":ar",
        ":as",
    ],
)

filegroup(
    name = "compiler_components",
    srcs = [
        ":clang",
        ":include",
    ],
)

filegroup(
    name = "linker_components",
    srcs = [
        ":ar",
        ":clang",
        ":ld",
        ":lib",
    ],
)

filegroup(
    name = "ar",
    srcs = ["bin/llvm-ar"],
)

filegroup(
    name = "clang",
    srcs = [
        "bin/clang",
        "bin/clang++",
    ],
)

filegroup(
    name = "include",
    srcs = glob(["lib/clang/11.0.0/include/**"]),
)

filegroup(
    name = "ld",
    srcs = ["bin/ld.lld"],
)

filegroup(
    name = "objcopy",
    srcs = ["bin/llvm-objcopy"],
)
