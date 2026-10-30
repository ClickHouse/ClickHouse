# CMake reference metadata; refreshed by bazel/initial_migration/refresh_cmake_metadata.py.
CMAKE_METADATA = {
  "x86_64": {
    "build_options": {
      "@BAZEL_COMPILER_VERSION_DEFINES@": "",
      "@BAZEL_BUILD_SYSTEM_ENTRY@": "",
      "@BAZEL_C_COMPILER@": "\"/home/buildbuddy/workspace/repo-root/build_bazel_reference_tools/llvm/bin/clang\"",
      "@BAZEL_CXX_COMPILER@": "\"/home/buildbuddy/workspace/repo-root/build_bazel_reference_tools/llvm/bin/clang++\"",
      "@BAZEL_C_FLAGS@": "\" --gcc-toolchain=./cmake/linux/../../contrib/sysroot/linux-x86_64 --no-default-config -fdiagnostics-color=always  -pipe -march=x86-64-v3 -mpclmul -fasynchronous-unwind-tables -fdebug-compilation-dir=. -ffile-prefix-map=./= -falign-functions=64 -falign-loops=32 -ffunction-sections -fdata-sections -faddrsig -mbranches-within-32B-boundaries -fstack-protector-strong -fstack-clash-protection -ffp-contract=off -fno-math-errno  -fdiagnostics-absolute-paths -fPIE -fxray-instrument -fxray-modes=none -fxray-instrumentation-bundle=function -Wframe-larger-than=65536 -Weverything -Wpedantic -Wno-return-type-c-linkage -Wno-zero-length-array -Wno-c++98-compat-pedantic -Wno-c++20-compat -Wno-sign-conversion -Wno-deprecated-declarations -Wno-documentation-unknown-command -Wno-exit-time-destructors -Wno-float-equal -Wno-global-constructors -Wno-missing-variable-declarations -Wno-padded -Wno-switch-enum -Wno-undefined-func-template -Wno-unused-template -Wno-weak-template-vtables -Wno-weak-vtables -Wno-thread-safety-negative -Wno-unsafe-buffer-usage -Wno-switch-default -Wno-nrvo -Wno-missing-noreturn -Wno-c2y-extensions -Wno-c23-extensions -O3 -DNDEBUG\"",
      "@BAZEL_CXX_FLAGS@": "\" --gcc-toolchain=./cmake/linux/../../contrib/sysroot/linux-x86_64 --no-default-config -fdiagnostics-color=always  -pipe -march=x86-64-v3 -mpclmul -fasynchronous-unwind-tables -fdebug-compilation-dir=. -ffile-prefix-map=./= -falign-functions=64 -falign-loops=32 -ffunction-sections -fdata-sections -faddrsig -mbranches-within-32B-boundaries -fstack-protector-strong -fstack-clash-protection -ffp-contract=off -fno-math-errno  -fdiagnostics-absolute-paths -fstrict-vtable-pointers -fPIE -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST -fxray-instrument -fxray-modes=none -fxray-instrumentation-bundle=function -Wall -Wextra -Wframe-larger-than=65536 -Weverything -Wpedantic -Wno-return-type-c-linkage -Wno-zero-length-array -Wno-c++98-compat-pedantic -Wno-c++20-compat -Wno-sign-conversion -Wno-deprecated-declarations -Wno-documentation-unknown-command -Wno-exit-time-destructors -Wno-float-equal -Wno-global-constructors -Wno-missing-variable-declarations -Wno-padded -Wno-switch-enum -Wno-undefined-func-template -Wno-unused-template -Wno-weak-template-vtables -Wno-weak-vtables -Wno-thread-safety-negative -Wno-unsafe-buffer-usage -Wno-switch-default -Wno-nrvo -Wno-missing-noreturn -Wno-c2y-extensions -Wno-c23-extensions -O3 -DNDEBUG\"",
      "@BAZEL_LINK_FLAGS@": "\" --gcc-toolchain=./cmake/linux/../../contrib/sysroot/linux-x86_64 --no-default-config --ld-path=./build_bazel_reference_tools/llvm/bin/ld.lld -Wl,--no-export-dynamic -Wl,--gc-sections -Wl,--fatal-warnings -Wl,--icf=safe -Wl,--strip-debug -Wl,--build-id=sha1 -Wl,-u,logf -Wl,-u,powf -Wl,-u,expf -Wl,-u,exp2f -Wl,-u,log2f -Wl,-u,log -Wl,-u,pow -Wl,-u,exp -Wl,-u,exp2 -Wl,-u,log2 -Wl,-u,lgammaf -pie -Wl,-pie -Wl,--whole-archive ./build_bazel_reference_x86_64/contrib/compiler-rt-cmake/libclang_rt_builtins.a -fno-xray-link-deps ./build_bazel_reference_x86_64/contrib/compiler-rt-cmake/libclang_rt_xray.a -Wl,--no-whole-archive -Wl,--whole-archive -llibllvmlibc -Wl,--no-whole-archive -Wl,-u,memcpy -Wl,-u,memmove -Wl,-u,memset -Wl,-u,memcmp -Wl,-z,now -Wl,-z,relro -Wl,-z,separate-code -Wl,-z,keep-text-section-prefix \"",
      "@BAZEL_CMAKE_VERSION@": "\"3.28.3\"",
      "@BAZEL_C_COMPILER_VERSION@": "\"22.1.1\"",
      "@BAZEL_CXX_COMPILER_VERSION@": "\"22.1.1\"",
    },
    "resource_gzip_mtimes": {
      "ClickStack": 1792282266,
      "SQLConsole": 1792282470,
    },
  },
  "aarch64": {
    "build_options": {
      "@BAZEL_COMPILER_VERSION_DEFINES@": "",
      "@BAZEL_BUILD_SYSTEM_ENTRY@": "",
      "@BAZEL_C_COMPILER@": "\"/home/buildbuddy/workspace/repo-root/build_bazel_reference_tools/llvm/bin/clang\"",
      "@BAZEL_CXX_COMPILER@": "\"/home/buildbuddy/workspace/repo-root/build_bazel_reference_tools/llvm/bin/clang++\"",
      "@BAZEL_C_FLAGS@": "\" --gcc-toolchain=./cmake/linux/../../contrib/sysroot/linux-aarch64 --no-default-config -fdiagnostics-color=always  -pipe -march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16 -fasynchronous-unwind-tables -fdebug-compilation-dir=. -ffile-prefix-map=./= -falign-functions=64 -falign-loops=32 -ffunction-sections -fdata-sections -faddrsig -fstack-protector-strong -fstack-clash-protection -ffp-contract=off -fno-math-errno  -fdiagnostics-absolute-paths -fPIE -fxray-instrument -fxray-modes=none -fxray-instrumentation-bundle=function -Wframe-larger-than=65536 -Weverything -Wpedantic -Wno-return-type-c-linkage -Wno-zero-length-array -Wno-c++98-compat-pedantic -Wno-c++20-compat -Wno-sign-conversion -Wno-deprecated-declarations -Wno-documentation-unknown-command -Wno-exit-time-destructors -Wno-float-equal -Wno-global-constructors -Wno-missing-variable-declarations -Wno-padded -Wno-switch-enum -Wno-undefined-func-template -Wno-unused-template -Wno-weak-template-vtables -Wno-weak-vtables -Wno-thread-safety-negative -Wno-unsafe-buffer-usage -Wno-switch-default -Wno-nrvo -Wno-missing-noreturn -Wno-c2y-extensions -Wno-c23-extensions -O3 -DNDEBUG\"",
      "@BAZEL_CXX_FLAGS@": "\" --gcc-toolchain=./cmake/linux/../../contrib/sysroot/linux-aarch64 --no-default-config -fdiagnostics-color=always  -pipe -march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16 -fasynchronous-unwind-tables -fdebug-compilation-dir=. -ffile-prefix-map=./= -falign-functions=64 -falign-loops=32 -ffunction-sections -fdata-sections -faddrsig -fstack-protector-strong -fstack-clash-protection -ffp-contract=off -fno-math-errno  -fdiagnostics-absolute-paths -fstrict-vtable-pointers -fPIE -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST -fxray-instrument -fxray-modes=none -fxray-instrumentation-bundle=function -Wall -Wextra -Wframe-larger-than=65536 -Weverything -Wpedantic -Wno-return-type-c-linkage -Wno-zero-length-array -Wno-c++98-compat-pedantic -Wno-c++20-compat -Wno-sign-conversion -Wno-deprecated-declarations -Wno-documentation-unknown-command -Wno-exit-time-destructors -Wno-float-equal -Wno-global-constructors -Wno-missing-variable-declarations -Wno-padded -Wno-switch-enum -Wno-undefined-func-template -Wno-unused-template -Wno-weak-template-vtables -Wno-weak-vtables -Wno-thread-safety-negative -Wno-unsafe-buffer-usage -Wno-switch-default -Wno-nrvo -Wno-missing-noreturn -Wno-c2y-extensions -Wno-c23-extensions -O3 -DNDEBUG\"",
      "@BAZEL_LINK_FLAGS@": "\" --no-default-config --ld-path=./build_bazel_reference_tools/llvm/bin/ld.lld -Wl,--no-export-dynamic -Wl,--gc-sections -Wl,--fatal-warnings -Wl,--icf=safe -Wl,--strip-debug -Wl,--build-id=sha1 -Wl,-u,logf -Wl,-u,powf -Wl,-u,expf -Wl,-u,exp2f -Wl,-u,log2f -Wl,-u,log -Wl,-u,pow -Wl,-u,exp -Wl,-u,exp2 -Wl,-u,log2 -Wl,-u,lgammaf -pie -Wl,-pie -Wl,--whole-archive ./build_bazel_reference_aarch64/contrib/compiler-rt-cmake/libclang_rt_builtins.a -fno-xray-link-deps ./build_bazel_reference_aarch64/contrib/compiler-rt-cmake/libclang_rt_xray.a -Wl,--no-whole-archive -Wl,--whole-archive -llibllvmlibc -Wl,--no-whole-archive -Wl,-u,memcpy -Wl,-u,memmove -Wl,-u,memset -Wl,-u,memcmp -Wl,-z,now -Wl,-z,relro -Wl,-z,separate-code -Wl,-z,keep-text-section-prefix \"",
      "@BAZEL_CMAKE_VERSION@": "\"3.28.3\"",
      "@BAZEL_C_COMPILER_VERSION@": "\"22.1.1\"",
      "@BAZEL_CXX_COMPILER_VERSION@": "\"22.1.1\"",
    },
    "resource_gzip_mtimes": {
      "ClickStack": 1780518042,
      "SQLConsole": 1790945103,
    },
    "license_substitutions": "license_substitutions_x86_64.json",
  },
}
