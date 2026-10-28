"""Resolve external repository names and CMake source filenames."""

load("//bazel/initial_migration:cmake_metadata.bzl", _CMAKE_METADATA = "CMAKE_METADATA")
load("//bazel/rules:repository_labels.bzl", _REPOSITORIES = "REPOSITORIES")

_LLVM_TOOLCHAIN_PATH = Label("@llvm-project//:BUILD.bazel").workspace_root

# ClickHouse's native C and C++ warnings, followed by each library's exceptions.
CLICKHOUSE_WARNINGS = [
    "-Weverything",
    "-Wno-missing-include-dirs",
    "-Wpedantic",
    "-Wno-return-type-c-linkage",
    "-Wno-zero-length-array",
    "-Wno-c++98-compat-pedantic",
    "-Wno-c++20-compat",
    "-Wno-sign-conversion",
    "-Wno-deprecated-declarations",
    "-Wno-documentation-unknown-command",
    "-Wno-exit-time-destructors",
    "-Wno-float-equal",
    "-Wno-global-constructors",
    "-Wno-missing-variable-declarations",
    "-Wno-padded",
    "-Wno-switch-enum",
    "-Wno-undefined-func-template",
    "-Wno-unused-template",
    "-Wno-weak-template-vtables",
    "-Wno-weak-vtables",
    "-Wno-thread-safety-negative",
    "-Wno-unsafe-buffer-usage",
    "-Wno-switch-default",
    "-Wno-nrvo",
    "-Wno-missing-noreturn",
    "-Wno-c2y-extensions",
    "-Wno-c23-extensions",
]

def repository_path(path):
    return _REPOSITORIES[path].workspace_root

def repository_local_include(directory, owner = None):
    """Express an execroot include relative to the cc rule's package."""
    directory = directory.removeprefix("$(GENDIR)/")
    if owner:
        root = repository_path(owner)
        if directory == root:
            return "."
        if directory.startswith(root + "/"):
            return directory.removeprefix(root + "/")
        return "../../" + directory
    return directory

def llvm_toolchain_path():
    return _LLVM_TOOLCHAIN_PATH

def cmake_cxx_compiler_version(arch):
    # build_options stores C++ string literals, including their quotes.
    return _CMAKE_METADATA[arch]["build_options"]["@BAZEL_CXX_COMPILER_VERSION@"][1:-1]

# Keep the three CppCompile prefix maps for the retired SDK in their original
# argument positions so deleting the repository preserves the compile cache.
_PREFIX_MAP_ROOTS = sorted(
    [(path, label.workspace_root) for path, label in _REPOSITORIES.items()] +
    [("contrib/sysroot", "external/+contrib+clickhouse_sysroot")],
)

_SOURCE_PREFIX_MAPS = [
    "-ffile-prefix-map=" + root + "/=" + path + "/"
    for path, root in _PREFIX_MAP_ROOTS
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
        "-ffile-prefix-map=$(GENDIR)/bazel/gen/base/=" + build + "base/",
        "-ffile-prefix-map=$(GENDIR)/bazel/gen/includes/=" + build + "includes/",
        "-ffile-prefix-map=$(GENDIR)/bazel/gen/src/=" + build + "src/",
        "-ffile-prefix-map=$(GENDIR)/bazel/gen/contrib/=" + build + "contrib/",
    ] + [
        "-ffile-prefix-map=$(GENDIR)/" + root + "/config/=" + build
        for _, root in _PREFIX_MAP_ROOTS
    ] + [
        "-ffile-prefix-map=$(GENDIR)/" + root + "/config/" + arch + "/=" + build
        for _, root in _PREFIX_MAP_ROOTS
    ] + [
        "-ffile-prefix-map=$(GENDIR)/" + repository_path("contrib/llvm-project") +
        "/gen/=" + build + "contrib/llvm-project/",
        "-ffile-prefix-map=$(GENDIR)/" + repository_path("contrib/llvm-project") +
        "/gen/" + arch + "/=" + build + "contrib/llvm-project/",
        "-ffile-prefix-map=$(GENDIR)/" + repository_path("contrib/jemalloc") +
        "/config/contrib/jemalloc-cmake/include_linux/=" +
        build + "contrib/jemalloc-cmake/include_linux_" + arch + "/",
    ]

# bazel/BUILD.bazel declares these two use_cmake_metadata settings.
_GENERATED_PREFIX_MAPS = {
    arch: _generated_prefix_maps(arch)
    for arch in ["x86_64", "aarch64"]
}

_EMBEDDED_PREFIX_MAPS = ["-ffile-prefix-map=$(GENDIR)/bazel/gen/embedded/="]

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

        # #embed inputs retain source-tree names even inside bazel/gen. Keep
        # this more specific map after the general generated-directory map.
        embedded_settings[cpu] = _EMBEDDED_PREFIX_MAPS
    return select(source_settings) + select(generated_settings) + select(embedded_settings)
