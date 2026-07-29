import argparse
import collections
import json
import os
import shlex
import shutil
import subprocess

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
    BuildTypes.AMD_ASAN_UBSAN: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address,undefined -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_TSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=thread    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_MSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=memory    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_RELEASE: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1",
    BuildTypes.ARM_ASAN_UBSAN: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address,undefined -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_DEBUG: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_TSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=thread    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.ARM_MSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=memory    -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.LLVM_COVERAGE_BUILD: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DWITH_COVERAGE=ON",
    BuildTypes.PER_TEST_COVERAGE: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DWITH_COVERAGE=ON -DWITH_COVERAGE_DEPTH=ON",
    BuildTypes.AMD_COVERAGE: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSANITIZE_COVERAGE=1",
    BuildTypes.ARM_BINARY: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_CFI: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CFI=1 -DSPLIT_DEBUG_SYMBOLS=ON -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    # amd_debug plus jemalloc's own sized-deallocation / double-free safety checks.
    BuildTypes.AMD_JEMALLOC_SAFETY: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1 -DENABLE_JEMALLOC_SAFETY_CHECKS=1",
    BuildTypes.AMD_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/darwin/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_AR:FILEPATH=/cctools/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/x86_64-apple-darwin-ld",
    BuildTypes.ARM_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/darwin/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_AR:FILEPATH=/cctools/bin/aarch64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/aarch64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/aarch64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/aarch64-apple-darwin-ld",
    BuildTypes.ARM_V80COMPAT: f"cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DNO_ARMV81_OR_HIGHER=1 -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_FREEBSD: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/freebsd/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.PPC64LE: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-ppc64le.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_COMPAT: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DX86_ARCH_LEVEL=1",
    BuildTypes.AMD_MUSL: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64-musl.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.RISCV64: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-riscv64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.S390X: f"        cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-s390x.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.LOONGARCH64: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-loongarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_FUZZERS: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_FUZZING=1 -DENABLE_PROTOBUF=1 -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=0 -DPARALLEL_LINK_JOBS=1",  # TODO: fix build with -DSANITIZE_COVERAGE=1
}

# sccache-warmup builds (MasterCI only) reuse the cmake configuration of the
# matching release build verbatim - the rest of their special handling (skip
# the official-build flag, strip debug symbols like a PR build, compile without
# linking, do not package) is keyed off PR_CACHE_WARMUP_BUILD_TYPES below.
PR_CACHE_WARMUP_TO_RELEASE = {
    BuildTypes.AMD_RELEASE_PR_CACHE_WARMUP: BuildTypes.AMD_RELEASE,
    BuildTypes.ARM_RELEASE_PR_CACHE_WARMUP: BuildTypes.ARM_RELEASE,
}
PR_CACHE_WARMUP_BUILD_TYPES = set(PR_CACHE_WARMUP_TO_RELEASE)
for _warmup_type, _release_type in PR_CACHE_WARMUP_TO_RELEASE.items():
    BUILD_TYPE_TO_CMAKE[_warmup_type] = BUILD_TYPE_TO_CMAKE[_release_type]

# TODO: for legacy packaging script - remove
BUILD_TYPE_TO_DEB_PACKAGE_TYPE = {
    BuildTypes.AMD_DEBUG: "debug",
    BuildTypes.AMD_RELEASE: "release",
    BuildTypes.ARM_RELEASE: "release",
    BuildTypes.ARM_DEBUG: "debug",
    BuildTypes.AMD_ASAN_UBSAN: "asan_ubsan",
    BuildTypes.ARM_ASAN_UBSAN: "asan_ubsan",
    BuildTypes.ARM_TSAN: "tsan",
    BuildTypes.AMD_MSAN: "msan",
    BuildTypes.ARM_MSAN: "msan",
    BuildTypes.AMD_TSAN: "tsan",
    BuildTypes.AMD_CFI: "cfi",
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


def setup_build_caches_env(info):
    """Configure compiler/clang-tidy cache environment for a build.

    Extracted so that other jobs (e.g. the unit-test bugfix validation job, which
    has to build `unit_tests_dbms` at the merge-base) configure the caches exactly
    like the regular build job and therefore hit the same shared cache entries.
    """
    # Global sccache settings for local and CI runs
    os.environ["SCCACHE_DIR"] = f"{temp_dir}/sccache"
    os.environ["SCCACHE_CACHE_SIZE"] = "40G"
    os.environ["SCCACHE_IDLE_TIMEOUT"] = "7200"
    os.environ["SCCACHE_BUCKET"] = Settings.S3_ARTIFACT_PATH
    os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
    os.environ["SCCACHE_ERROR_LOG"] = f"{build_dir}/sccache.log"
    os.environ["SCCACHE_LOG"] = "info"
    # PR builds must not pollute the shared sccache bucket; only master/release
    # builds (pr_number == 0) are allowed to write entries.
    if info.pr_number > 0:
        os.environ["SCCACHE_S3_READ_ONLY"] = "true"
    os.makedirs(build_dir, exist_ok=True)

    if info.is_local_run:
        if os.environ.get("SCCACHE_ENDPOINT"):
            print(f"NOTE: Using custom sccache endpoint: {os.environ['SCCACHE_ENDPOINT']}")
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            print("NOTE: Using custom AWS credentials for sccache")
        else:
            os.environ["SCCACHE_S3_NO_CREDENTIALS"] = "true"
    else:
        # Default timeout (10min), can be too low, we run this in docker
        # anyway, will be terminated once the build is finished
        os.environ["CTCACHE_LOG_LEVEL"] = "debug"
        os.environ["CTCACHE_DIR"] = f"{temp_dir}/ccache/clang-tidy-cache"
        os.environ["CTCACHE_S3_BUCKET"] = Settings.S3_ARTIFACT_PATH
        os.environ["CTCACHE_S3_FOLDER"] = "ccache/clang-tidy-cache"
        # PR builds run on untrusted runners without S3 write access; only
        # master/release builds (pr_number == 0) are allowed to write entries.
        if info.pr_number > 0:
            os.environ["CTCACHE_S3_READ_ONLY"] = "true"

        os.environ["CH_HOSTNAME"] = (
            "https://build-cache.eu-west-1.aws.clickhouse-staging.com"
        )
        os.environ["CH_USER"] = "ci_builder"
        os.environ["CH_PASSWORD"] = chcache_secret.get_value()
        os.environ["CH_USE_LOCAL_CACHE"] = "false"


JEMALLOC_SAFETY_MACROS = ("JEMALLOC_OPT_SAFETY_CHECKS", "JEMALLOC_OPT_SIZE_CHECKS")
JEMALLOC_SOURCE_MARKER = "/contrib/jemalloc/src/"

# Compiled with a jemalloc translation unit's own flags, so the compiler answers whether
# the two gates are armed. `config_opt_safety_checks` / `config_opt_size_checks` are the
# booleans jemalloc's detector sites read, and `jemalloc_preamble.h` is where each `-D`
# becomes one - after several includes, so a `#undef` arriving through a header counts too.
#
# Both kinds of assertion are needed, because each answers a different question. The
# `#ifdef` pair asks whether *our* macro is the one present: `jemalloc_preamble.h:191` is
# `#elif defined(JEMALLOC_DEBUG)` and `:208` is `|| defined(JEMALLOC_DEBUG)`, so
# `JEMALLOC_DEBUG` alone satisfies both booleans - and it additionally arms jemalloc's
# internal `assert`s and changes inlining, so a lane running on it is not the `amd_debug`
# build plus exactly one option that this build type promises. The `_Static_assert` pair
# asks whether the preamble still converts our macro into the boolean the detector sites
# read, which a rewritten condition would break with the `#ifdef`s still satisfied.
# `JEMALLOC_DEBUG` is deliberately not rejected: adding it on top of both option macros is
# legitimate. The `#ifdef`s must follow the include, or a `#undef` arriving through one of
# the headers `jemalloc_preamble.h:4-54` includes - exactly what this probe exists to
# catch - would be tested before it happens.
JEMALLOC_PROBE_SOURCE = """\
#include "jemalloc/internal/jemalloc_preamble.h"
#ifndef JEMALLOC_OPT_SAFETY_CHECKS
#error "JEMALLOC_OPT_SAFETY_CHECKS is not defined on this compile line"
#endif
#ifndef JEMALLOC_OPT_SIZE_CHECKS
#error "JEMALLOC_OPT_SIZE_CHECKS is not defined on this compile line"
#endif
_Static_assert(config_opt_safety_checks, "JEMALLOC_OPT_SAFETY_CHECKS is not in effect");
_Static_assert(config_opt_size_checks, "JEMALLOC_OPT_SIZE_CHECKS is not in effect");
"""


# The depfile flags that take an operand as the next argv element. `-MD`, `-MMD` and `-MP`
# take none, so they must not swallow the token after them.
DEPFILE_FLAGS_WITH_OPERAND = ("-MT", "-MF", "-MQ", "-MJ")

# Every extension a compiled source operand can carry in this tree's
# `compile_commands.json`, plus the object files. Measured over a configured tree's 17284
# entries: `.cpp` 10387, `.c` 3940, `.cc` 2659, `.asm` 124, `.cxx` 62, `.c++` 49, `.s` 40,
# `.S` 23. Enumerating only some of them leaves the rest as stray input files, which is the
# failure the reduction below exists to prevent - so the list is the whole set, and it is
# only ever applied to *positional* tokens, or a joined flag whose value happens to end this
# way (`-I/opt/dir.c++`) would be dropped as if it were a source.
SOURCE_OPERAND_EXTENSIONS = (
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".c++",
    ".C",
    ".s",
    ".S",
    ".asm",
    ".m",
    ".mm",
    ".o",
)


def jemalloc_probe_flags(command):
    """A compile command reduced to its compiler and flags, ready for the probe.

    The output path, the `-c`, the depfile flags and the source/object operands go; every
    other flag is kept verbatim, so the probe is compiled exactly as the entry itself is.

    The reduction must leave no stray positional operand behind: a leftover path reaches
    clang as an input file, and the resulting `no such file or directory` turns the probe
    into a false verdict pointing at the wrong cause. That is why the operand-taking
    depfile flags are consumed as pairs, the way `-o` is - cmake's Makefile generator emits
    exactly `-MD -MT <target> -MF <path>` - and why the source extension list is the
    complete one rather than just jemalloc's own `.c`. Both probe callers reduce with this,
    and the leak probe's entries are C++: a left-in `.cc` operand costs 2.8s instead of
    0.2s per probe, and for a source cmake has not generated yet (the guards run in the
    CMAKE stage, before BUILD - 9 of a configured tree's protobuf `.pb.cc` files are in
    that state) it fails the probe outright with no diagnostic of its own.
    """
    tokens = shlex.split(command)
    flags = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "-o":
            index += 2
            continue
        if (
            token in DEPFILE_FLAGS_WITH_OPERAND
            and index + 1 < len(tokens)
            and not tokens[index + 1].startswith("-")
        ):
            index += 2
            continue
        if token == "-c" or token.startswith("-M"):
            index += 1
            continue
        if not token.startswith("-") and token.endswith(SOURCE_OPERAND_EXTENSIONS):
            index += 1
            continue
        flags.append(token)
        index += 1
    return flags


def run_jemalloc_probe(flags, directory):
    """Compile `JEMALLOC_PROBE_SOURCE` with `flags`; return the compiler's stderr, or None.

    `-fsyntax-only` keeps it at ~0.05s and `-x c -` feeds the probe on stdin, so nothing
    is written into the build tree. Returns None when both `_Static_assert`s hold.

    A probe that cannot run at all (the recorded compiler gone, the recorded `directory`
    gone) is reported as a failure too: an inconclusive probe must not pass.
    """
    try:
        process = subprocess.run(
            flags + ["-fsyntax-only", "-x", "c", "-"],
            cwd=directory,
            input=JEMALLOC_PROBE_SOURCE,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        return f"the probe could not be run at all: {error}"
    return None if process.returncode == 0 else (process.stderr or "").strip()


# The leak probe's `#error` texts. They are matched in the compiler's stderr to tell a real
# leak from a probe that failed for some unrelated reason, so the two outcomes stay
# distinguishable: only these strings mean "the macro reached this translation unit".
JEMALLOC_LEAK_MARKER = "{macro} reaches this translation unit"
JEMALLOC_LEAK_PROBE_SOURCE = "".join(
    f'#ifdef {macro}\n#error "{JEMALLOC_LEAK_MARKER.format(macro=macro)}"\n#endif\n'
    for macro in JEMALLOC_SAFETY_MACROS
) + "int jemalloc_leak_probe_ok;\n"

# The extensions clang compiles as C++. Everything else is probed as C; the language has to
# match the entry, because a C probe compiled with C++ flags fails for reasons of its own
# and would read as an inconclusive probe on a perfectly clean compile line.
CXX_SOURCE_EXTENSIONS = (".cc", ".cpp", ".cxx", ".c++", ".C")

# The one compiler in this tree that is not a C preprocessor, so it cannot hand these macros
# to a jemalloc translation unit even in principle - and which cannot answer the probe
# either: it rejects `-fsyntax-only` outright (`unrecognised output format 'syntax-only'`),
# which would read as an inconclusive probe forever. Measured over a configured tree: 124
# `nasm` entries, all `.asm`, one flag set between them; jemalloc's own sources are 67
# entries and all `.c`, so nothing nasm compiles shares a translation unit with them. The
# skip is on the *compiler*, not the file extension, so a `.asm` handed to clang would still
# be probed - see `test_the_sweep_skips_only_the_assembler`.
NON_PREPROCESSING_COMPILERS = ("nasm",)


def _cannot_define_by_construction(flags):
    """Whether this compile line's compiler cannot define a C macro at all."""
    return os.path.basename(flags[0]) in NON_PREPROCESSING_COMPILERS if flags else False


def run_jemalloc_leak_probe(flags, directory, language):
    """Ask the compiler whether either safety macro is defined on this compile line.

    Returns `(leaked_macros, stderr)`: the macros whose `#error` fired, and the compiler's
    stderr. An empty list with an empty stderr means the line is clean.

    A nonzero exit that carries neither `#error` marker is *inconclusive*, not a leak: a
    missing generated header, an unrelated diagnostic under `-Werror` or an assembler that
    does not understand `-fsyntax-only` all land here. The caller must not pass such a
    probe, and must not report it as a leak either - hence the two return channels.
    """
    try:
        process = subprocess.run(
            flags + ["-fsyntax-only", "-x", language, "-"],
            cwd=directory,
            input=JEMALLOC_LEAK_PROBE_SOURCE,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        return [], f"the probe could not be run at all: {error}"
    stderr = (process.stderr or "").strip()
    if process.returncode == 0:
        return [], ""
    leaked = [
        macro
        for macro in JEMALLOC_SAFETY_MACROS
        if JEMALLOC_LEAK_MARKER.format(macro=macro) in stderr
    ]
    return leaked, stderr


def assert_jemalloc_macros_stay_private(entries):
    """Fail if either macro reaches a translation unit outside jemalloc's own sources.

    The macros are jemalloc-internal: they must stay PRIVATE to the `_jemalloc` target, or
    the rest of ClickHouse is compiled against a different `config_opt_*` view of
    jemalloc's headers than jemalloc itself.

    Like the arming half, this half lets the **compiler** decide, because nothing else can:
    every driver flag that forwards its operands to the preprocessor is another spelling of
    the same `-D` (`-Wp,-DX`, `-Wp,-D,X`, `-Xpreprocessor -D -Xpreprocessor X`,
    `-Xclang -D -Xclang X`), and a definition can also arrive with no macro name on the line
    at all - out of an `@response` file, a pre-included header, a `-include-pch`'d PCH, or a
    `--config` / `--config-user-dir` configuration file. A prefilter over these routes was
    tried and deleted: four review rounds each found a *narrower* route it did not know
    about, because clang keeps acquiring ways to inject a definition and no finite clause
    list closes that. So there is no prefilter, and every entry's flags are put to the
    compiler.

    That is affordable because the flags of interest are **per-target**, not per-file: one
    probe per distinct `(flag set, directory, language)` covers every entry sharing it.
    Measured on a configured tree, 17217 non-jemalloc entries collapse to 255 such keys,
    probed in 4.8s. The prefiltered sweep this replaces probed 130 entries in 27.7s - fewer
    probes but 5.8x the time, because those 130 were only **3** distinct keys, so it re-ran
    the same three expensive C++ probes ~43 times each. End to end the whole check goes from
    58s to 36s; both arms spend ~31s of that reducing 17217 compile lines, which dominates
    either way and is why probing everything costs less than filtering did.
    """
    # One representative entry per distinct key, and how many entries that key covers - a
    # flag set now stands for many files, so a bare file count would name one file and hide
    # the blast radius.
    probed = {}
    covered = collections.Counter()
    for entry in entries:
        if JEMALLOC_SOURCE_MARKER in entry["file"]:
            continue
        language = "c++" if entry["file"].endswith(CXX_SOURCE_EXTENSIONS) else "c"
        key = (
            tuple(jemalloc_probe_flags(entry["command"])),
            entry["directory"],
            language,
        )
        probed.setdefault(key, entry)
        covered[key] += 1

    leaked = {macro: [] for macro in JEMALLOC_SAFETY_MACROS}
    for key, entry in probed.items():
        flags, directory, language = key
        if _cannot_define_by_construction(flags):
            continue
        macros, stderr = run_jemalloc_leak_probe(list(flags), directory, language)
        if stderr and not macros:
            raise AssertionError(
                "the jemalloc leak probe is inconclusive for "
                f"{entry['file']}: compiling a probe against that translation unit's own "
                "flags fails without reporting either macro, so whether they reach it "
                f"cannot be decided.\ndirectory: {directory}\n{stderr}\n"
                "An inconclusive probe must not pass. Fix the compile line, or - if this "
                "compiler is not a C preprocessor at all - add it to "
                "NON_PREPROCESSING_COMPILERS with the reasoning."
            )
        for macro in macros:
            leaked[macro].append((entry["file"], covered[key]))
    for macro, hits in leaked.items():
        if hits:
            hits = sorted(hits)
            listed = ", ".join(f"{f} (+{n - 1} more with these flags)" for f, n in hits[:5])
            raise AssertionError(
                f"-D{macro} reaches {sum(n for _, n in hits)} non-jemalloc translation "
                f"units, across {len(hits)} distinct flag set(s) (first: {listed}). The "
                "macro is jemalloc-internal: it must stay PRIVATE to the _jemalloc target, "
                "or other code is compiled against a different config_opt_* view of "
                "jemalloc's headers than jemalloc itself."
            )
    return len(probed)


def assert_jemalloc_safety_macros_armed(compile_commands_path):
    """Fail unless every jemalloc TU, and only those, really arm both gates.

    `ENABLE_JEMALLOC_SAFETY_CHECKS` promises two `-D`s that gate jemalloc's
    sized-deallocation and slab-bit detectors, and `JEMALLOC_OPT_SIZE_CHECKS` has no
    mallctl, so a lost definition leaves the lane fuzzing green as an ordinary
    `amd_debug` session.

    Both halves ask the compiler rather than parsing the compile line, so every cmake
    spelling, every `-D`/`-U` spelling (including the forwarded `-Wp,` / `-Xpreprocessor` /
    `-Xclang` forms) and every `#undef` arriving through an include are answered at once.
    Both also dedup by *flag set* rather than by file, because the flags of interest are
    per-target: the arming half compiles one probe per distinct jemalloc flag set, and the
    leak half (`assert_jemalloc_macros_stay_private`) one per distinct non-jemalloc
    `(flag set, directory, language)` - 255 keys for a configured tree's 17217 non-jemalloc
    entries, 4.8s of probing, so no entry has to be accepted unprobed.
    """
    if not os.path.isfile(compile_commands_path):
        raise AssertionError(
            f"{compile_commands_path} is missing, so the jemalloc safety macros "
            "cannot be verified; the root CMakeLists.txt:50 sets "
            "CMAKE_EXPORT_COMPILE_COMMANDS "
            "unconditionally, so a configured tree always has it"
        )
    with open(compile_commands_path, "r", encoding="utf-8") as file:
        entries = json.load(file)

    jemalloc = [e for e in entries if JEMALLOC_SOURCE_MARKER in e["file"]]
    # A rename of jemalloc's source layout must not make this check vacuous.
    if not jemalloc:
        raise AssertionError(
            f"no {JEMALLOC_SOURCE_MARKER!r} translation unit in "
            f"{compile_commands_path} (of {len(entries)} entries); jemalloc's source "
            "layout changed, so re-derive this check rather than letting it pass "
            "vacuously"
        )

    # The flags of interest are per-target, so one probe per *distinct* flag set covers
    # every jemalloc entry (in practice there is one). The count is reported below, so a
    # future per-file divergence is visible rather than silently unprobed.
    probed = {}
    for entry in jemalloc:
        probed.setdefault(tuple(jemalloc_probe_flags(entry["command"])), entry)
    for flags, entry in probed.items():
        stderr = run_jemalloc_probe(list(flags), entry["directory"])
        if stderr:
            raise AssertionError(
                "the jemalloc safety gates are not armed for "
                f"{entry['file']}: compiling a probe against that translation unit's own "
                f"flags fails.\ncompiler: {flags[0]}\ndirectory: {entry['directory']}\n"
                f"{stderr}\n"
                "This build type promises both macros, and losing one disarms a detector "
                "while the lane still fuzzes green. It happens when the cmake option is "
                "not passed at all, when a later -U from another "
                "target_compile_options / add_definitions cancels the -D on the same "
                "compile line, when an include undefines it, or when the ARCH_AMD64 "
                "guard in contrib/jemalloc-cmake/CMakeLists.txt turns the option off."
            )

    leak_keys = assert_jemalloc_macros_stay_private(entries)

    print(
        f"jemalloc safety macros: {', '.join(JEMALLOC_SAFETY_MACROS)} arm both gates for "
        f"all {len(jemalloc)} jemalloc translation units ({len(probed)} distinct flag "
        f"set(s) probed), and reach none of the {len(entries) - len(jemalloc)} other "
        f"translation units ({leak_keys} distinct flag set/directory/language key(s) probed)"
    )


def assert_jemalloc_safety_macros_absent(compile_commands_path):
    """Fail if a build that did not request the option carries either macro.

    The option defaults to OFF, so a build that did not request it compiles jemalloc without
    the two macros. Flipping that default - or widening the `-D`s to a target other builds
    link - would arm both gates in every x86-64 jemalloc build, release included, which is a
    user-visible change no other layer can see: the probe above only runs for the one build
    type that requests the option, and the Python-level assertion reads the `ci/` cmake
    commands rather than a configured tree.

    Called for exactly one ordinary build, `amd_debug` - see the call site for why one is
    enough and why making it mandatory for all 31 non-lane build types is not proportional.
    It is `amd_debug` specifically because that is the lane's own base, so this verdict and
    the armed one differ in exactly the option under test.

    Only jemalloc's own entries are scanned; the leak direction is the positive check's
    business. Like every other direction in this guard, the question is put to the
    **compiler**: a scan is only ever as complete as the list of `-D` spellings someone
    thought of, and each driver flag that forwards its operands to the preprocessor is
    another spelling (`-Wp,-DX`, `-Xpreprocessor -D -Xpreprocessor X`,
    `-Xclang -D -Xclang X`, a `-D` inside an `@response` file or a pre-included header).
    `JEMALLOC_LEAK_PROBE_SOURCE` already `#error`s exactly when a macro is defined, which
    is precisely this check's failure condition, so it needs no probe source of its own.

    Cost is one compile: the flags are per-target, so probing one entry per **distinct**
    flag set covers them all, exactly as the arming half does (a non-safety build has ~67
    jemalloc entries with one flag set between them).

    An **empty** jemalloc set fails closed, exactly as in the positive check. This check
    runs only for `amd_debug`, whose cmake command is Linux x86-64 with `-DSANITIZE=` empty,
    and `contrib/jemalloc-cmake/CMakeLists.txt:1-11` disables jemalloc only when `SANITIZE`
    is set to something other than `undefined` - so `amd_debug` always builds jemalloc. An
    empty set there therefore says the source marker stopped matching, not that jemalloc was
    switched off, and passing on it would report "neither macro is defined" while probing
    nothing.

    An **inconclusive** probe - a nonzero exit carrying neither `#error` marker - raises
    rather than passing, for the same reason a missing file does: it says the question
    could not be answered, which is not an answer of "neither macro is defined".

    A **missing** `compile_commands.json` is a different thing and fails closed, exactly as
    in the positive check. All three of this guard's premises say the file is there:
    the root `CMakeLists.txt:50` sets `CMAKE_EXPORT_COMPILE_COMMANDS` unconditionally, the
    build job
    calls this only after a successful cmake configure, and a configured tree therefore
    always has it. So its absence says the question could not be asked - which is not the
    same as an answer of "neither macro is defined", and reading it as one would let a
    flipped default through on any build whose configure step changed shape.
    """
    if not os.path.isfile(compile_commands_path):
        raise AssertionError(
            f"{compile_commands_path} is missing, so it cannot be verified that this "
            "build carries neither jemalloc safety macro; the root CMakeLists.txt:50 "
            "sets "
            "CMAKE_EXPORT_COMPILE_COMMANDS unconditionally and this check runs only "
            "after a successful cmake configure, so a missing file is inconclusive "
            "rather than evidence that the macros are absent"
        )
    with open(compile_commands_path, "r", encoding="utf-8") as file:
        entries = json.load(file)

    carried = {macro: [] for macro in JEMALLOC_SAFETY_MACROS}
    jemalloc = [e for e in entries if JEMALLOC_SOURCE_MARKER in e["file"]]
    # A rename of jemalloc's source layout must not make this check vacuous.
    if not jemalloc:
        raise AssertionError(
            f"no {JEMALLOC_SOURCE_MARKER!r} translation unit in "
            f"{compile_commands_path} (of {len(entries)} entries); jemalloc's source "
            "layout changed, so re-derive this check rather than letting it pass "
            "vacuously - this check runs only for a build that always compiles jemalloc"
        )

    probed = {}
    for entry in jemalloc:
        probed.setdefault(tuple(jemalloc_probe_flags(entry["command"])), entry)
    for flags, entry in probed.items():
        language = "c++" if entry["file"].endswith(CXX_SOURCE_EXTENSIONS) else "c"
        macros, stderr = run_jemalloc_leak_probe(
            list(flags), entry["directory"], language
        )
        if stderr and not macros:
            raise AssertionError(
                "the jemalloc safety-macro probe is inconclusive for "
                f"{entry['file']}: compiling a probe against that translation unit's own "
                "flags fails without reporting either macro, so whether this build "
                f"carries them cannot be decided.\ndirectory: {entry['directory']}\n"
                f"{stderr}\n"
                "An inconclusive probe must not pass: it is not evidence that the macros "
                "are absent."
            )
        for macro in macros:
            carried[macro].append(entry["file"])
    for macro, files in carried.items():
        if files:
            files = sorted(files)
            raise AssertionError(
                f"{macro} is defined for {len(files)} jemalloc flag set(s) "
                f"(first: {files[:5]}) of a build that did not request "
                f"ENABLE_JEMALLOC_SAFETY_CHECKS. The option defaults to OFF, and arming "
                "the gates outside the diagnostic lane changes every x86-64 jemalloc "
                "build, release included."
            )

    print(
        f"jemalloc safety macros: neither of {', '.join(JEMALLOC_SAFETY_MACROS)} is "
        f"defined for any of the {len(jemalloc)} jemalloc translation units "
        f"({len(probed)} distinct flag set(s) probed), as this build did not request them"
    )


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

    # Cache-warmup build (MasterCI): compile with the PR release build's cmake
    # flags (no official-build flag, debug symbols stripped, no PGO/BOLT) so the
    # object files it compiles share sccache keys with PR builds, while keeping
    # the shared sccache read-write (master, pr_number == 0).
    cache_warmup = build_type in PR_CACHE_WARMUP_BUILD_TYPES
    assert not (
        cache_warmup and info.pr_number > 0
    ), "sccache-warmup builds are only meant to run on master/release (pr_number == 0)"

    setup_build_caches_env(info)

    # The cache-warmup build must match PR compiler flags, so it skips the
    # official-build flag (PR builds do not set it).
    if info.pr_number == 0 and not cache_warmup:
        cmake_cmd += " -DCLICKHOUSE_OFFICIAL_BUILD=1"

    is_private = (
        "PRIVATE_BUILDS_TO_CMAKE" in vars() or "PRIVATE_BUILDS_TO_CMAKE" in globals()
    )

    # When building with LTO removing debug symbols makes linking much faster
    # In PRs we disable them to save time and space, but keep them for official builds (master, pr_number = 0)
    # We keep them in private to allow deploying to staging from PRs
    # The cache-warmup build mirrors the PR build, so it disables them too.
    if (
        not is_private
        and (info.pr_number != 0 or cache_warmup)
        and "ENABLE_THINLTO=1" in cmake_cmd
    ):
        cmake_cmd += " -DDISABLE_ALL_DEBUG_SYMBOLS=1"

    # PGO/BOLT profile integration for release builds. The sccache-warmup builds
    # are deliberately excluded: they exist only to populate the shared compiler
    # cache, and PGO/BOLT belong to the real release builds.
    pgo_profile = "/opt/clickhouse-profiles/clickhouse-pgo.profdata"
    bolt_profile = "/opt/clickhouse-profiles/clickhouse-bolt.fdata"
    use_pgo = build_type in (BuildTypes.AMD_RELEASE, BuildTypes.ARM_RELEASE) and os.path.isfile(pgo_profile)
    use_bolt = build_type in (BuildTypes.AMD_RELEASE, BuildTypes.ARM_RELEASE) and os.path.isfile(bolt_profile) and os.path.getsize(bolt_profile) > 0

    # PGO is best-effort: keep a PGO-free command ready so we can retry without
    # profile-guided optimization if cmake/build fails with a stale/incompatible
    # profile. BOLT has a similar fallback path applied after linking. Apply BOLT
    # before snapshotting `cmake_cmd_no_pgo` so the retry preserves `--emit-relocs`
    # and the later `llvm-bolt` step still has a relocatable binary to operate on.
    if use_bolt:
        print(f"BOLT profile found at {bolt_profile}, enabling BOLT post-link optimization")
        cmake_cmd += " -DENABLE_CLICKHOUSE_BOLT=ON"
    cmake_cmd_no_pgo = cmake_cmd
    if use_pgo:
        print(f"PGO profile found at {pgo_profile}, enabling profile-guided optimization")
        cmake_cmd += f" -DCLICKHOUSE_PGO_PROFILE_PATH={pgo_profile}"

    cmake_cmd += f" {repo_path_normalized} -B {build_dir_normalized}"
    cmake_cmd_no_pgo += f" {repo_path_normalized} -B {build_dir_normalized}"

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

            if os.path.isdir(".git/modules/contrib") and os.listdir(
                ".git/modules/contrib"
            ):
                # Submodule cache was restored by runner.py — just populate working trees
                print("Submodule cache detected, populating working trees from cache")
                res = res and Shell.check(
                    "git submodule update --depth 1 --single-branch",
                    retries=3,
                )
            else:
                res = res and Shell.check(
                    "contrib/update-submodules.sh --max-procs 10",
                    retries=3,
                )
            return res

        results.append(
            Result.from_commands_run(name="Checkout Submodules", command=do_checkout)
        )
        res = results[-1].is_ok()

        # Validate `.gitmodules` (no recursive submodules, valid URLs, name == path).
        # Run it only in the arm_tidy build to avoid adding overhead to every build
        # and to the style check (which does not have submodules available).
        if res and build_type == BuildTypes.ARM_TIDY:
            results.append(
                Result.from_commands_run(
                    name="Check Submodules",
                    command="./ci/jobs/scripts/check_style/check_submodules.sh",
                )
            )
            res = results[-1].is_ok()

    version = None
    if not info.is_local_run:
        version = CHVersion.get_current_version_from_ci_pipeline()

    if not version:
        # Repo-read fallback: the merge-queue workflow runs no version_log hook,
        # so KV storage is empty and this is the only path. The checkout is
        # shallow there, so the tweak cannot be counted from git history -- read
        # non-strict and let it degrade to the placeholder tweak instead of
        # raising, matching the pre-refactor behavior.
        version = CHVersion.get_current_version(no_strict=True)
        if not info.is_local_run:
            print(
                "WARNING: ClickHouse version has not been found in workflow kv storage - read from repo"
            )
            info.add_workflow_warning(
                "ClickHouse version has not been found in workflow kv storage"
            )
    assert version

    if res and JobStages.CMAKE in stages:
        assert version, "Failed to determine build version"
        version.write()
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
        # The sccache server sometimes fails to start because of issues with S3.
        # Start it explicitly with retries before cmake, since cmake can invoke
        # the compiler during configuration. Non-fatal: build can proceed without it.
        if not Shell.check("sccache --start-server", retries=3):
            print("WARNING: sccache server failed to start, build will proceed without it")
        run_shell("sccache stats", "sccache --show-stats")
        cmake_result_index = len(results)
        results.append(
            Result.from_commands_run(
                name="Cmake configuration",
                command=cmake_cmd,
                workdir=build_dir_normalized,
            )
        )
        res = results[-1].is_ok()

        # PGO is best-effort: if cmake failed with a profile (e.g. it is stale
        # or incompatible with the current sources/toolchain), retry once
        # without `-DCLICKHOUSE_PGO_PROFILE_PATH`. If the retry succeeds the
        # fallback must not block the job, so replace the failed first attempt
        # with the successful retry — otherwise `Result.create_from` aggregates
        # the job status as FAIL because of the stranded failed child result.
        if not res and use_pgo:
            print("WARNING: cmake with PGO failed, retrying without profile-guided optimization")
            Shell.check(f"rm -f {build_dir}/CMakeCache.txt")
            retry_result = Result.from_commands_run(
                name="Cmake configuration (retry without PGO)",
                command=cmake_cmd_no_pgo,
                workdir=build_dir_normalized,
            )
            if retry_result.is_ok():
                retry_result.set_info(
                    "PGO profile was stale or incompatible; reconfigured without it (best-effort fallback)"
                )
                results[cmake_result_index] = retry_result
                use_pgo = False
                res = True
            else:
                results.append(retry_result)
                res = False

        # The lane's whole value depends on the two jemalloc safety macros really
        # reaching the compiler, so assert it here rather than after ~40 minutes of
        # compiling. The option defaults to OFF, so arming the gates outside this lane
        # would be a user-visible change to every x86-64 jemalloc build; the absence
        # direction catches that, and one ordinary build is enough to catch it.
        #
        # That build is `amd_debug`, this lane's own base: the armed and absent verdicts are
        # then directly comparable, since the two cmake commands differ in exactly the one
        # option. Running it for all 31 other build types instead would make a fail-closed
        # check - a missing `compile_commands.json`, an inconclusive probe - block builds
        # that have nothing to do with a weekly diagnostic lane, including 24 sanitizer,
        # non-x86 and coverage builds that carry no jemalloc translation units at all or
        # cannot be affected by an x86-64-only cmake guard. The check is cheap (0.31s) but
        # cheap is not a reason to make it mandatory everywhere.
        if res:
            jemalloc_check = {
                BuildTypes.AMD_JEMALLOC_SAFETY: (
                    "jemalloc safety macros",
                    assert_jemalloc_safety_macros_armed,
                ),
                BuildTypes.AMD_DEBUG: (
                    "jemalloc safety macros absent",
                    assert_jemalloc_safety_macros_absent,
                ),
            }.get(build_type)
            if jemalloc_check:
                name, command = jemalloc_check
                results.append(
                    Result.from_commands_run(
                        name=name,
                        command=command,
                        command_args=[f"{build_dir}/compile_commands.json"],
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
            targets = "clickhouse-bundle"
        elif build_type in (
            BuildTypes.AMD_TIDY,
            BuildTypes.ARM_TIDY,
        ):
            targets = "-k0 all"
        else:
            targets = "clickhouse-bundle"

        if cache_warmup:
            # Warm sccache by compiling every translation unit but skip linking
            # the final binaries. sccache caches per-TU compilation; the
            # ThinLTO/PGO link step produces nothing cacheable yet dominates a
            # release build's wall time, so there is no reason to run it here.
            # Build every object-file target ninja knows about; ninja pulls in
            # the generated headers each object depends on. xargs batches the
            # list to stay within the command-line length limit.
            build_command = (
                "ninja -t targets all | cut -d: -f1 | grep -E '[.]o$' "
                "| xargs --no-run-if-empty ninja"
            )
        else:
            build_command = f"command time -v ninja {targets}"

        build_result_index = len(results)
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=build_command,
                workdir=build_dir_normalized,
            )
        )

        # PGO is best-effort: if linking with a stale/incompatible profile fails,
        # reconfigure without `-DCLICKHOUSE_PGO_PROFILE_PATH` and rebuild once.
        # As with the cmake fallback above, on a successful retry we replace
        # the failed first-attempt build result so the job is not blocked by
        # a stranded FAIL child.
        if not results[-1].is_ok() and use_pgo:
            print("WARNING: build with PGO failed, retrying without profile-guided optimization")
            Shell.check(f"rm -f {build_dir}/CMakeCache.txt")
            retry_cmake = Result.from_commands_run(
                name="Cmake configuration (retry without PGO)",
                command=cmake_cmd_no_pgo,
                workdir=build_dir_normalized,
            )
            if retry_cmake.is_ok():
                retry_build = Result.from_commands_run(
                    name="Build ClickHouse (retry without PGO)",
                    command=f"command time -v ninja {targets}",
                    workdir=build_dir_normalized,
                )
                if retry_build.is_ok():
                    retry_build.set_info(
                        "PGO profile was stale or incompatible; rebuilt without it (best-effort fallback)"
                    )
                    results[build_result_index] = retry_build
                    use_pgo = False
                else:
                    results.append(retry_cmake)
                    results.append(retry_build)
            else:
                results.append(retry_cmake)

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

        # Apply BOLT post-link optimization if profiles are available
        if res and use_bolt:
            clickhouse_binary = f"{build_dir}/programs/clickhouse"
            clickhouse_bolted = f"{build_dir}/programs/clickhouse.bolt"
            bolt_cmd = (
                f"llvm-bolt {clickhouse_binary} "
                f"-o {clickhouse_bolted} "
                f"-data={bolt_profile} "
                f"-reorder-blocks=ext-tsp "
                f"-reorder-functions=cdsort "
                f"-split-functions "
                f"-split-all-cold "
                f"-split-eh "
                f"-dyno-stats "
                f"-use-gnu-stack"
            )
            bolt_result = Result.from_commands_run(
                name="BOLT optimization",
                command=bolt_cmd,
            )
            results.append(bolt_result)
            if bolt_result.is_ok():
                # Replace original binary with BOLT-optimized version
                Shell.check(f"mv {clickhouse_bolted} {clickhouse_binary}")
                # Rebuild the self-extracting bundle so uploaded artifacts contain the BOLT-optimized binary
                results.append(
                    Result.from_commands_run(
                        name="Rebuild self-extracting bundle after BOLT",
                        command=f"ninja clickhouse-self-extracting",
                        workdir=build_dir_normalized,
                    )
                )
                if results[-1].is_ok():
                    print("BOLT optimization applied successfully, self-extracting bundle rebuilt")
                else:
                    print("WARNING: Failed to rebuild self-extracting bundle after BOLT")
                    res = False
            else:
                # BOLT is best-effort: if it fails, continue with the unoptimized binary
                print("WARNING: BOLT optimization failed, continuing with unoptimized binary")
                results[-1] = Result(
                    name="BOLT optimization (skipped)",
                    status=Result.Status.OK,
                    info="BOLT post-processing failed (best-effort), using PGO-only binary",
                )

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

        assert Shell.check(f"rm -f {temp_dir}/*.deb {temp_dir}/*.rpm {temp_dir}/*.tgz {temp_dir}/*.tgz.sha512")

        results.append(
            Result.from_commands_run(
                name="Build Packages",
                command=[
                    f"rm -rf {build_dir_normalized}/root",
                    f"DESTDIR={build_dir_normalized}/root command time -v ninja programs/install",
                    f"ln -sf {build_dir_normalized}/root {Utils.cwd()}/packages/root",
                    f"cd {Utils.cwd()}/packages/ && OUTPUT_DIR={temp_dir} BUILD_TYPE={BUILD_TYPE_TO_DEB_PACKAGE_TYPE[build_type]} VERSION_STRING={version.string} DEB_ARCH={deb_arch} ./build --deb {'--rpm --tgz' if 'release' in build_type else ''}",
                ],
                workdir=build_dir_normalized,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results, files=files).complete_job()


if __name__ == "__main__":
    main()
