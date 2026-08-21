import argparse
import os
import shutil

from ci.defs.defs import BuildTypes, ToolSet, chcache_secret
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import MetaClasses, Shell, Utils

current_directory = Utils.cwd()
build_dir = f"{current_directory}/ci/tmp/build"
temp_dir = f"{current_directory}/ci/tmp"
repo_path_normalized = "/ClickHouse"
build_dir_normalized = f"{repo_path_normalized}/ci/tmp/build"

BUILD_TYPE_TO_CMAKE = {
    BuildTypes.AMD_DEBUG: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_RELEASE: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1",
    BuildTypes.AMD_BINARY: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_ASAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.AMD_TSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=thread    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_MSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=memory    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_UBSAN: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=undefined -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_RELEASE: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1",
    BuildTypes.ARM_ASAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_TSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=thread    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.LLVM_COVERAGE_BUILD: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DWITH_COVERAGE=ON",
    BuildTypes.AMD_COVERAGE: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSANITIZE_COVERAGE=1",
    BuildTypes.ARM_BINARY: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DENABLE_EXAMPLES=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/darwin/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_AR:FILEPATH=/cctools/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/x86_64-apple-darwin-ld",
    BuildTypes.ARM_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/darwin/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_AR:FILEPATH=/cctools/bin/aarch64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/aarch64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/aarch64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/aarch64-apple-darwin-ld",
    BuildTypes.ARM_V80COMPAT: f"cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DNO_ARMV81_OR_HIGHER=1 -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_FREEBSD: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/freebsd/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.PPC64LE: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-ppc64le.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_COMPAT: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DX86_ARCH_LEVEL=1 -DENABLE_EXAMPLES=1",
    BuildTypes.AMD_MUSL: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64-musl.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.RISCV64: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-riscv64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.S390X: f"        cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-s390x.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.LOONGARCH64: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-loongarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_FUZZERS: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_FUZZING=1 -DENABLE_PROTOBUF=1 -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=0",  # TODO: fix build with -DSANITIZE_COVERAGE=1
}

# TODO: for legacy packaging script - remove
BUILD_TYPE_TO_DEB_PACKAGE_TYPE = {
    BuildTypes.AMD_DEBUG: "debug",
    BuildTypes.AMD_RELEASE: "release",
    BuildTypes.ARM_RELEASE: "release",
    BuildTypes.AMD_ASAN: "asan",
    BuildTypes.ARM_ASAN: "asan",
    BuildTypes.ARM_TSAN: "tsan",
    BuildTypes.AMD_MSAN: "msan",
    BuildTypes.AMD_UBSAN: "ubsan",
    BuildTypes.AMD_TSAN: "tsan",
}


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    UNSHALLOW = "unshallow"
    BUILD = "build"
    PACKAGE = "package"
    UNIT = "unit"
    UPLOAD_PROFILE_DATA = "profile"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument(
        "--build-type",
        help="see BuildTypes.*",
    )
    parser.add_argument(
        "--param",
        help="Optional user-defined job start stage (for local run)",
        default=None,
    )
    return parser.parse_args()


def run_shell(name, command, **kwargs):
    print(f"\n>>>> {name}\n")
    Shell.check(command, **kwargs)
    print(f"\n<<<< {name}\n")


def main():
    args = parse_args()

    stages = list(JobStages)
    stage = args.param or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    build_type = args.build_type.lower()
    assert (
        build_type
    ), "--build-type must be provided either as input argument or as a parameter of parametrized job in CI"
    assert (
        build_type in BUILD_TYPE_TO_CMAKE
    ), f"--build_type option is invalid [{build_type}]"

    cmake_cmd = BUILD_TYPE_TO_CMAKE[build_type]
    info = Info()
    # Global sccache settings for local and CI runs
    os.environ["SCCACHE_DIR"] = f"{temp_dir}/sccache"
    os.environ["SCCACHE_CACHE_SIZE"] = "40G"
    os.environ["SCCACHE_IDLE_TIMEOUT"] = "7200"
    os.environ["SCCACHE_BUCKET"] = Settings.S3_ARTIFACT_PATH
    os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
    os.environ["SCCACHE_ERROR_LOG"] = f"{build_dir}/sccache.log"
    os.environ["SCCACHE_LOG"] = "info"

    os.makedirs(build_dir, exist_ok=True)

    if info.is_local_run:
        os.environ["SCCACHE_S3_NO_CREDENTIALS"] = "true"
    else:
        # Default timeout (10min), can be too low, we run this in docker
        # anyway, will be terminated once the build is finished
        os.environ["CTCACHE_LOG_LEVEL"] = "debug"
        os.environ["CTCACHE_DIR"] = f"{temp_dir}/ccache/clang-tidy-cache"
        os.environ["CTCACHE_S3_BUCKET"] = Settings.S3_ARTIFACT_PATH
        os.environ["CTCACHE_S3_FOLDER"] = "ccache/clang-tidy-cache"

        os.environ["CH_HOSTNAME"] = (
            "https://build-cache.eu-west-1.aws.clickhouse-staging.com"
        )
        os.environ["CH_USER"] = "ci_builder"
        os.environ["CH_PASSWORD"] = chcache_secret.get_value()
        os.environ["CH_USE_LOCAL_CACHE"] = "false"

    if info.pr_number == 0:
        cmake_cmd += " -DCLICKHOUSE_OFFICIAL_BUILD=1"

    is_private = (
        "PRIVATE_BUILDS_TO_CMAKE" in vars() or "PRIVATE_BUILDS_TO_CMAKE" in globals()
    )

    # When building with LTO removing debug symbols makes linking much faster
    # In PRs we disable them to save time and space, but keep them for official builds (master, pr_number = 0)
    # We keep them in private to allow deploying to staging from PRs
    if not is_private and info.pr_number != 0 and "ENABLE_THINLTO=1" in cmake_cmd:
        cmake_cmd += " -DDISABLE_ALL_DEBUG_SYMBOLS=1"

    cmake_cmd += f" {repo_path_normalized} -B {build_dir_normalized}"

    res = True
    results = []

    if os.getuid() == 0:
        res = res and Shell.check(
            f"git config --global --add safe.directory {current_directory}"
        )

    if res and JobStages.CHECKOUT_SUBMODULES in stages:

        def do_checkout():
            res = Shell.check(
                f"mkdir -p {build_dir} && git submodule sync && git submodule init"
            )

            res = res and Shell.check(
                "contrib/update-submodules.sh --max-procs 10",
                retries=3,
            )
            return res

        results.append(
            Result.from_commands_run(name="Checkout Submodules", command=do_checkout)
        )
        res = results[-1].is_ok()

    version_dict = None
    if not info.is_local_run:
        version_dict = info.get_kv_data("version")

    if not version_dict:
        version_dict = CHVersion.get_current_version_as_dict()
        if not info.is_local_run:
            print(
                "WARNING: ClickHouse version has not been found in workflow kv storage - read from repo"
            )
            info.add_workflow_report_message(
                "WARNING: ClickHouse version has not been found in workflow kv storage"
            )
    assert version_dict

    if res and JobStages.CMAKE in stages:
        assert version_dict, "Failed to determine build version"
        CHVersion.set_binary_version(version_dict=version_dict)
        if "darwin" in build_type:
            Shell.check(
                f"rm -rf {current_directory}/cmake/toolchain/darwin-x86_64 {current_directory}/cmake/toolchain/darwin-aarch64"
            )
            Shell.check(
                f"ln -sf /build/cmake/toolchain/darwin-x86_64 {current_directory}/cmake/toolchain/darwin-x86_64"
            )
            Shell.check(
                f"ln -sf /build/cmake/toolchain/darwin-x86_64 {current_directory}/cmake/toolchain/darwin-aarch64"
            )
        elif build_type in (BuildTypes.AMD_TIDY, BuildTypes.ARM_TIDY):
            run_shell("clang-tidy-cache stats", "clang-tidy-cache.py --show-stats")
        # The sccache server sometimes fails to start because of issues with S3
        # It should start before cmake, since cmake can precompile binaries during
        # configuration stage
        results.append(
            Result.from_commands_run(
                name="Start sccache server", command="sccache --start-server", retries=3
            )
        )
        run_shell("sccache stats", "sccache --show-stats")
        results.append(
            Result.from_commands_run(
                name="Cmake configuration",
                command=cmake_cmd,
                workdir=build_dir_normalized,
            )
        )
        res = results[-1].is_ok()

        # Pre-seed .ninja_log from toolchain for timing-based scheduling
        if res:
            ninja_log_seed = "/usr/local/share/clickhouse-build/ninja_log"
            if os.path.exists(ninja_log_seed):
                shutil.copy2(ninja_log_seed, f"{build_dir}/.ninja_log")
                print(f"Pre-seeded .ninja_log from {ninja_log_seed}")
            Shell.check("ninja --version", verbose=True)

    # Activate FIPS-permissive config for OpenSSL
    os.environ["OPENSSL_CONF"] = "/etc/ssl/openssl.cnf"

    files = []
    if res and JobStages.BUILD in stages:
        if build_type == BuildTypes.ARM_FUZZERS:
            targets = "fuzzers"
        elif build_type == BuildTypes.ARM_BINARY:
            targets = "clickhouse-bundle parser_memory_profiler"
        elif build_type in (
            BuildTypes.AMD_TIDY,
            BuildTypes.ARM_TIDY,
            BuildTypes.AMD_COMPAT,
        ):
            # check all targets in BuildTypes.AMD_COMPAT instead of a regular build to not delay tests job
            targets = "-k0 all"
        else:
            targets = "clickhouse-bundle"
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=f"command time -v ninja {targets}",
                workdir=build_dir_normalized,
            )
        )
        run_shell("sccache stats", "sccache --show-stats")
        if build_type in (BuildTypes.AMD_TIDY, BuildTypes.ARM_TIDY):
            run_shell("clang-tidy-cache stats", "clang-tidy-cache.py --show-stats")
            clang_tidy_cache_log = "./ci/tmp/clang-tidy-cache.log"
            Shell.check(f"cp /tmp/clang-tidy-cache.log {clang_tidy_cache_log}")
            files.append(clang_tidy_cache_log)
            run_shell(
                "clang-tidy-cache.log stats",
                f'echo "$(grep "exists in cache" {clang_tidy_cache_log} | wc -l) in cache\n'
                f'$(grep "does not exist in cache" {clang_tidy_cache_log} | wc -l) not in cache"',
            )
        run_shell("Output programs", f"ls -l {build_dir}/programs/", verbose=True)
        Shell.check("pwd")
        res = results[-1].is_ok()

    if (
        res
        and JobStages.PACKAGE in stages
        and build_type in BUILD_TYPE_TO_DEB_PACKAGE_TYPE
        and not info.is_local_run
    ):
        if "amd" in build_type:
            deb_arch = "amd64"
        else:
            deb_arch = "arm64"

        assert Shell.check(f"rm -f {temp_dir}/*.deb")

        results.append(
            Result.from_commands_run(
                name="Build Packages",
                command=[
                    f"DESTDIR={build_dir_normalized}/root command time -v ninja programs/install",
                    f"ln -sf {build_dir_normalized}/root {Utils.cwd()}/packages/root",
                    f"cd {Utils.cwd()}/packages/ && OUTPUT_DIR={temp_dir} BUILD_TYPE={BUILD_TYPE_TO_DEB_PACKAGE_TYPE[build_type]} VERSION_STRING={version_dict['string']} DEB_ARCH={deb_arch} ./build --deb {'--rpm --tgz' if 'release' in build_type else ''}",
                ],
                workdir=build_dir_normalized,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results, files=files).complete_job()


if __name__ == "__main__":
    main()
