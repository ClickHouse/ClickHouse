"""Resolve external repository names and CMake source filenames."""

load(":repository_labels.bzl", _REPOSITORIES = "REPOSITORIES")

_LLVM_TOOLCHAIN_PATH = Label("@llvm-project//:BUILD.bazel").workspace_root

def repository_path(path):
    return _REPOSITORIES[path].workspace_root

def llvm_toolchain_path():
    return _LLVM_TOOLCHAIN_PATH

_SOURCE_PREFIX_MAPS = [
    "-ffile-prefix-map=" + label.workspace_root + "/=" + path + "/"
    for path, label in _REPOSITORIES.items()
] + [
    "-ffile-prefix-map=" + llvm_toolchain_path() + "/" + name + "/=contrib/llvm-project/" + name + "/"
    for name in ["libc", "libcxx", "libcxxabi", "libunwind", "compiler-rt"]
] + [
    "-ffile-prefix-map=$(GENDIR)/" + llvm_toolchain_path() + "/" + name + "/" + name +
    "_headers_include_search_directory/=contrib/llvm-project/" + name + "/include/"
    for name in ["libcxx", "libcxxabi"]
]

def _generated_prefix_maps(arch):
    build = "build_bazel_reference_" + arch + "/"
    return [
        "-ffile-prefix-map=bazel/config/" + arch + "/=" + build,
        "-ffile-prefix-map=$(GENDIR)/bazel/gen/" + arch + "/=" + build,
    ] + [
        "-ffile-prefix-map=$(GENDIR)/" + label.workspace_root + "/config/" + arch + "/=" + build
        for label in _REPOSITORIES.values()
    ] + [
        "-ffile-prefix-map=$(GENDIR)/" + repository_path("contrib/llvm-project") +
        "/gen/" + arch + "/=" + build + "contrib/llvm-project/",
    ]

# bazel/BUILD.bazel declares these two use_cmake_metadata settings.
_GENERATED_PREFIX_MAPS = {
    arch: _generated_prefix_maps(arch)
    for arch in ["x86_64", "aarch64"]
}

_EMBEDDED_PREFIX_MAPS = {
    arch: ["-ffile-prefix-map=$(GENDIR)/bazel/gen/" + arch + "/embedded/="]
    for arch in _GENERATED_PREFIX_MAPS
}

def cmake_prefix_maps(arches):
    """Use original CMake source names, and saved build names in metadata mode."""
    source_settings = {"//conditions:default": []}
    generated_settings = {"//conditions:default": []}
    embedded_settings = {"//conditions:default": []}
    for arch in arches:
        cpu = str(Label("@platforms//cpu:" + arch))
        source_settings[cpu] = _SOURCE_PREFIX_MAPS
        setting = str(Label("@clickhouse//bazel:use_cmake_metadata_" + arch))
        generated_settings[setting] = _GENERATED_PREFIX_MAPS[arch]
        # #embed copies retain source-tree names even inside bazel/gen. Keep
        # this more specific map after the general generated-directory map.
        embedded_settings[cpu] = _EMBEDDED_PREFIX_MAPS[arch]
    return select(source_settings) + select(generated_settings) + select(embedded_settings)

def cmake_native_compile_flags(groups):
    """Apply the recorded headers and the sysroot only for Clang compilations."""
    choices = {"//conditions:default": []}
    for arch, group in groups.items():
        options = []
        if group["language"] in ["C", "CXX", "ASM"]:
            gcc_root = repository_path("contrib/sysroot") + "/linux-" + arch
            options = [
                "--sysroot=" + gcc_root + "/" + arch + "-linux-gnu/libc",
                "--gcc-toolchain=" + gcc_root,
            ]
        if group["language"] == "CXX":
            # The module already supplies public libc++ headers. Do not let
            # the GCC sysroot add a second C++ standard library.
            options.append("-nostdinc++")
        for flag, kind, directory in group["includes"]:
            raw = repository_path("contrib/llvm-project")
            if kind == "generated":
                raw = "$(GENDIR)/" + raw + "/gen/" + arch
            options.extend([flag, raw + "/" + directory])
        choices[str(Label("@platforms//cpu:" + arch))] = options
    return select(choices)
