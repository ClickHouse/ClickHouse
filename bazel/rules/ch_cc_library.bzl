"""CMake native libraries compiled for the ClickHouse target platform."""

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":repository_paths.bzl", "CLICKHOUSE_WARNINGS", "cmake_prefix_maps", "llvm_toolchain_path", "repository_path")

# Place these before library copts: several libraries require an older ISA.
CLICKHOUSE_TARGET_FLAGS = {
    "x86_64": ["-march=x86-64-v3", "-mpclmul", "-mbranches-within-32B-boundaries"],
    "aarch64": ["-march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16"],
}

CLICKHOUSE_CODE_FLAGS = [
    "-fxray-instrument",
    "-fxray-modes=none",
    "-fxray-instrumentation-bundle=function",
    "-Wall",
    "-Wextra",
]

def ch_cc_library(
        name,
        hdrs = [],
        additional_compiler_inputs = [],
        copts = [],
        clickhouse_code = False,
        language = "CXX",
        arches = ["x86_64", "aarch64"],
        libcxx_headers = True,
        llvm_headers = None,
        link_runtime = False,
        map_source_files = True,
        features = [],
        **kwargs):
    """Compile a CMake library, with CPU defaults before source overrides.

    llvm_headers supplies LLVM compiler-rt, libc and libunwind headers; it
    defaults to clickhouse_code. libcxx_headers supplies the pinned libc++
    headers to C/ASM. link_runtime adds libc++, libc++abi and libunwind libs.
    """
    cpu_flags = select({
        str(Label("@platforms//cpu:" + arch)): flags
        for arch, flags in CLICKHOUSE_TARGET_FLAGS.items()
    } | {"//conditions:default": []})
    code_flags = CLICKHOUSE_CODE_FLAGS if clickhouse_code else []
    if clickhouse_code and language == "CXX":
        defaults = ["-Wframe-larger-than=65536"] + CLICKHOUSE_WARNINGS + ["-std=c++23"]
        code_flags = code_flags + select({
            str(Label("@platforms//cpu:" + arch)): defaults if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": defaults})
        package = native.package_name()
        root = "../" * len(package.split("/")) if package else ""
        includes = [root + llvm_toolchain_path() + directory for directory in [
            "/libunwind/include",
            "/libc",
            "/compiler-rt/include",
        ]]
        kwargs["local_includes"] = kwargs.get("local_includes", []) + select({
            str(Label("@platforms//cpu:" + arch)): includes if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": includes})
    if llvm_headers == None:
        llvm_headers = clickhouse_code
    if llvm_headers:
        headers = [
            Label("@llvm-project//compiler-rt:headers"),
            Label("@llvm-project//libc:libc_headers"),
            Label("@llvm-project//libunwind:headers"),
        ]
        kwargs["deps"] = select({
            str(Label("@platforms//cpu:" + arch)): headers if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": headers}) + kwargs.get("deps", [])
    if link_runtime:
        runtime = [
            Label("@clickhouse_llvm_project//:cxx"),
            Label("@clickhouse_llvm_project//:cxxabi"),
            Label("@clickhouse_llvm_project//:unwind"),
        ]
        kwargs["deps"] = kwargs.get("deps", []) + select({
            str(Label("@platforms//cpu:" + arch)): runtime if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": runtime})
    if language == "C" and libcxx_headers:
        libcxx_files = [
            Label("@clickhouse_llvm_project//:runtime_source_inputs"),
            Label("@clickhouse_llvm_project//:runtime_generated_headers"),
        ]
        additional_compiler_inputs = select({
            str(Label("@platforms//cpu:" + arch)): libcxx_files if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": libcxx_files}) + additional_compiler_inputs
    native_options = {"//conditions:default": []}
    sysroot_headers = {"//conditions:default": []}
    sysroot_files = {"//conditions:default": []}
    for arch in arches:
        cpu = str(Label("@platforms//cpu:" + arch))
        gcc_root = repository_path("contrib/sysroot") + "/linux-" + arch
        options = [
            "--sysroot=" + gcc_root + "/" + arch + "-linux-gnu/libc",
            "--gcc-toolchain=" + gcc_root,
        ]
        if language == "CXX":
            # GCC's sysroot must not supply a second C++ standard library.
            options.append("-nostdinc++")
        elif language in ["C", "ASM"] and libcxx_headers:
            options.extend([
                "-I",
                "$(GENDIR)/" + repository_path("contrib/llvm-project") +
                "/gen/libcxx/include/c++/v1",
            ])
        native_options[cpu] = options
        sysroot_headers[cpu] = [Label("@clickhouse_sysroot//:linux_native_headers")]
        sysroot_files[cpu] = [Label("@clickhouse_sysroot//:linux_compile_files")]
    cc_library(
        name = name,
        hdrs = hdrs + select(sysroot_headers),
        additional_compiler_inputs = additional_compiler_inputs + select(sysroot_files),
        copts = cpu_flags + code_flags + copts +
                (cmake_prefix_maps(arches) if map_source_files else []) + select(native_options),
        features = ["-fortify_source", "-default_compile_flags", "-llvm_system_c_headers"] + features,
        **kwargs
    )
