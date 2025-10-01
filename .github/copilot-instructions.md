# ClickHouse Copilot Coding Agent Instructions

## Repository Overview
ClickHouse is an open-source column-oriented database management system for real-time analytical data reports. This is a **large C++ codebase** (~430MB, 3200+ lines of docs) with Python CI/test infrastructure.

**Languages & Tools:** C++ (Clang 19+ required), CMake, Python 3, Rust (optional), Ninja build system
**Architecture:** x86_64, AArch64, PowerPC 64 LE, s390/x, RISC-V 64

## Critical Build Requirements

### Prerequisites - ALWAYS Install First
```bash
# Install core dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg

# Install Clang 19+ (REQUIRED - GCC not supported)
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"

# Submodules (REQUIRED - build will fail without)
git submodule update --init --jobs 12
```

**WARNING:** Environment must be clean. ClickHouse **fails to build** if `CFLAGS`, `CXXFLAGS`, or `LDFLAGS` environment variables are set. Check with `env` and unset if needed.

### Build Process
```bash
# Create separate build directory (REQUIRED pattern)
mkdir build && cd build

# Configure for development (Debug recommended for coding)
cmake -D CMAKE_BUILD_TYPE=Debug ..

# Build (expect 20-30 min first time with ccache)
ninja clickhouse                    # Just server/client
ninja                               # All binaries and tests
ninja -j 1 clickhouse-server        # Control parallelism if memory limited
```

**Alternative: Docker Build (recommended for CI-like environment)**
```bash
# Runs build in Docker with all dependencies pre-installed
python -m ci.praktika "Build (amd_debug)"
# Output in ./ci/tmp/
```

### Running Built Binaries
```bash
# After build, binaries are in build/programs/
cd build/programs/
./clickhouse server -C config.xml  # Needs config.xml in current dir or via -C
./clickhouse client --host 127.0.0.1
```

## Testing Infrastructure

### Functional Tests (Primary Test Suite)
Located in `tests/queries/0_stateless/` - SQL scripts that run against ClickHouse server.

**Run tests locally:**
```bash
# Start server first on default port 9000
PATH=/path/to/clickhouse-client:$PATH tests/clickhouse-test 01428_test_name
# Or filter tests:
./clickhouse-test substring_of_test_name
```

**Writing tests:**
- Create `.sql` file in `tests/queries/0_stateless/`
- Generate reference: `clickhouse-client < test.sql > test.reference`
- Tests use database `test` (auto-created), can use temporary tables
- **CRITICAL:** Use explicit timezones for DateTime/DateTime64: `toDateTime64(val, 3, 'Europe/Amsterdam')`

### Integration Tests
Located in `tests/integration/` - Docker-based tests with multiple ClickHouse instances.

**Requirements:**
```bash
# Must install Docker and Python dependencies
sudo apt-get install python3-pip libpq-dev zlib1g-dev libcrypto++-dev libssl-dev
sudo -H pip install pytest docker PyMySQL kazoo minio protobuf pymongo
```

### Style Check (Runs first in CI)
```bash
# Run locally before committing
python -m ci.praktika run "Style check"
python -m ci.praktika run "Style check" --test cpp  # Just C++ style
```

**Style rules:**
- Use `clang-format` (config: `.clang-format`)
- 4 spaces indent, no tabs
- Run `ci/jobs/scripts/check_style/check_cpp.sh` for regex-based checks

### Fast Test (Critical CI Check)
Builds ClickHouse and runs most stateless tests (15-20 min).
```bash
python -m ci.praktika run "Fast test" [--test some_test_name]
```
**If Fast test fails in CI, other checks won't run until fixed.**

## CI/CD Pipeline (praktika-based)

### Key CI Jobs (in order)
1. **Style Check** - Code style, typos, Python mypy (fails fast if violated)
2. **Fast Test** - Quick build + most functional tests (gates other checks)
3. **Build Check** - Multiple build configurations (amd_debug, amd_release, arm_release, etc.)
4. **Functional Tests** - Full stateless test suite
5. **Integration Tests** - Docker-based multi-instance tests
6. **Unit Tests** - C++ unit tests (if enabled with `-DENABLE_TESTS=1`)

### Running CI Jobs Locally
```bash
# General pattern (requires Docker only)
python -m ci.praktika "<JOB_NAME>"
# Examples:
python -m ci.praktika "Build (arm_release)"
python -m ci.praktika "Stateless tests (amd_debug)"
```

### CI Restart (for transient failures)
```bash
git reset
git commit --allow-empty -m "Restart CI"
git push
```

## Repository Layout

### Core Source Directories
- `src/` - Main C++ source (30+ subdirectories: Processors, Parsers, Storages, Functions, etc.)
- `programs/` - Entry points (server, client, keeper, etc.)
- `base/` - Low-level utilities
- `contrib/` - Third-party libraries (256 submodules)

### Build & CI Configuration
- `CMakeLists.txt` - Root build config
- `PreLoad.cmake` - Build environment validation (checks for polluting env vars)
- `cmake/` - CMake modules (tools.cmake enforces Clang 19+, tools.cmake configures linker)
- `ci/` - Praktika CI system (workflows/, jobs/, docker/)
- `.clang-format`, `.clang-tidy` - C++ formatting/linting config

### Tests
- `tests/queries/0_stateless/` - Functional SQL tests (1000s of .sql files)
- `tests/integration/` - Docker integration tests (628 subdirectories)
- `tests/ci/` - Legacy CI scripts (being migrated to ci/)
- `tests/clickhouse-test` - Main test runner script

### Documentation
- `docs/en/development/build.md` - Build instructions
- `docs/en/development/tests.md` - Testing guide
- `docs/en/development/style.md` - C++ style guide (871 lines)
- `docs/en/development/continuous-integration.md` - CI system reference

## Common Issues & Workarounds

### Build Failures
1. **"Submodules are not initialized"** → Run `git submodule update --init --jobs 12`
2. **"Cannot find strip/linker"** → Clang 19+ not installed or not in PATH
3. **CMake complains about CFLAGS/CXXFLAGS** → Environment polluted, run `env` and unset vars (see PreLoad.cmake)
4. **Out of memory during build** → Use `ninja -j 1` or fewer parallel jobs
5. **ccache issues** → Clear with `ccache -C` or disable with `cmake -DCOMPILER_CACHE=disabled`

### Test Failures
1. **DateTime test flaky** → CI randomizes timezones; always specify timezone explicitly
2. **Integration test Docker errors** → Check Docker daemon running, user in docker group
3. **Test reference mismatch** → Regenerate reference file: `clickhouse-client < test.sql > test.reference`

### CI Check Failures  
1. **Style Check fails** → Run `ci/jobs/scripts/check_style/check_cpp.sh` locally, fix regex violations
2. **Merge conflict** → Merge master into your branch: `git pull upstream master`
3. **Docs Check fails** → Look for ERROR/WARNING in report, fix broken cross-links

## Development Workflow Best Practices

1. **Always start with Style Check locally** to avoid failing CI immediately
2. **Use Debug builds** (`-DCMAKE_BUILD_TYPE=Debug`) for development - faster compilation, better debugging
3. **Test timezone-sensitive code** with explicit timezone specifications
4. **Keep PRs focused** - ClickHouse prefers small, targeted changes
5. **Check submodules** after pulling: `git submodule status` (modified submodules will break build)
6. **Clean build on environment issues**: `rm -rf build && mkdir build` then reconfigure

## Quick Reference Commands

```bash
# Setup
git clone --recurse-submodules git@github.com:your_username/ClickHouse.git
cd ClickHouse

# Build & Test Cycle  
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
ninja clickhouse
cd ..
PATH=$(pwd)/build/programs:$PATH tests/clickhouse-test test_name

# CI-like Testing
python -m ci.praktika "Style check"
python -m ci.praktika "Fast test"
python -m ci.praktika "Build (amd_debug)"

# Format Code
clang-format -i --style=file path/to/file.cpp
```

**Trust these instructions.** Only search/explore if information here is incomplete or incorrect. This document is validated against actual build/test runs.
