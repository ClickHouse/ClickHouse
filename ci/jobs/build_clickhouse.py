import argparse
import os
import shutil

<<<<<<< HEAD
from ci.defs.defs import BuildTypes, ToolSet
=======
from ci.defs.defs import BuildTypes, ToolSet, chcache_secret
>>>>>>> origin/master
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
    BuildTypes.ARM_UBSAN: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=undefined -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
    BuildTypes.LLVM_COVERAGE_BUILD: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DWITH_COVERAGE=ON",
    BuildTypes.PER_TEST_COVERAGE: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DWITH_COVERAGE=ON -DWITH_COVERAGE_DEPTH=ON",
    BuildTypes.AMD_COVERAGE: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSANITIZE_COVERAGE=1",
    BuildTypes.ARM_BINARY: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE}        -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/linux/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/darwin/toolchain-x86_64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_AR:FILEPATH=/cctools/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/x86_64-apple-darwin-ld",
    BuildTypes.ARM_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DBUILD_STRIPPED_BINARY=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE_LEGACY} -DCMAKE_TOOLCHAIN_FILE={repo_path_normalized}/cmake/darwin/toolchain-aarch64.cmake -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_AR:FILEPATH=/cctools/bin/aarch64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/aarch64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/aarch64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/aarch64-apple-darwin-ld",
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
    BuildTypes.ARM_UBSAN: "ubsan",
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

        os.environ["CH_HOSTNAME"] = (
            "https://build-cache.eu-west-1.aws.clickhouse-staging.com"
        )
        os.environ["CH_USER"] = "ci_builder"
        os.environ["CH_PASSWORD"] = chcache_secret.get_value()
        os.environ["CH_USE_LOCAL_CACHE"] = "false"

    if info.pr_number == 0:
        cmake_cmd += " -DCLICKHOUSE_OFFICIAL_BUILD=1"

<<<<<<< HEAD
=======
    is_private = (
        "PRIVATE_BUILDS_TO_CMAKE" in vars() or "PRIVATE_BUILDS_TO_CMAKE" in globals()
    )

    # When building with LTO removing debug symbols makes linking much faster
    # In PRs we disable them to save time and space, but keep them for official builds (master, pr_number = 0)
    # We keep them in private to allow deploying to staging from PRs
    if not is_private and info.pr_number != 0 and "ENABLE_THINLTO=1" in cmake_cmd:
        cmake_cmd += " -DDISABLE_ALL_DEBUG_SYMBOLS=1"

    # PGO/BOLT profile integration for release builds
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

>>>>>>> origin/master
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

    version_dict = None
    if not info.is_local_run:
        version_dict = info.get_kv_data("version")

    if not version_dict:
        version_dict = CHVersion.get_current_version_as_dict()
        if not info.is_local_run:
            print(
                "WARNING: ClickHouse version has not been found in workflow kv storage - read from repo"
            )
            info.add_workflow_warning(
                "ClickHouse version has not been found in workflow kv storage"
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
        build_result_index = len(results)
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=f"command time -v ninja {targets}",
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
<<<<<<< HEAD
                    f"DESTDIR={build_dir}/root ninja programs/install",
                    f"ln -sf {build_dir}/root {Utils.cwd()}/packages/root",
=======
                    f"rm -rf {build_dir_normalized}/root",
                    f"DESTDIR={build_dir_normalized}/root command time -v ninja programs/install",
                    f"ln -sf {build_dir_normalized}/root {Utils.cwd()}/packages/root",
>>>>>>> origin/master
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
