# Support for Profile-Guided Optimization (PGO) and Binary Optimization and Layout Tool (BOLT)
# for the ClickHouse binary itself.
#
# PGO workflow:
#   1. Build with -DENABLE_CLICKHOUSE_PGO_GENERATE=ON  (instrumented build)
#   2. Run representative workloads (performance tests)
#   3. Merge profiles: llvm-profdata merge -output=clickhouse.profdata *.profraw
#   4. Build with -DCLICKHOUSE_PGO_PROFILE_PATH=/path/to/clickhouse.profdata
#
# BOLT workflow (applied after linking):
#   1. Build with -DENABLE_CLICKHOUSE_BOLT=ON  (adds --emit-relocs to linker flags)
#   2. Instrument: llvm-bolt clickhouse -instrument -o clickhouse.inst
#   3. Run workloads with instrumented binary
#   4. Merge: merge-fdata *.fdata > bolt.fdata
#   5. Optimize: llvm-bolt clickhouse -o clickhouse.bolt -data=bolt.fdata ...

# --- PGO: Profile Generation (instrumented build) ---
option(ENABLE_CLICKHOUSE_PGO_GENERATE
    "Build ClickHouse with PGO instrumentation (-fprofile-generate). Incompatible with ThinLTO." OFF)

# --- PGO: Profile Use (optimized build) ---
set(CLICKHOUSE_PGO_PROFILE_PATH "" CACHE STRING
    "Path to merged .profdata file for PGO-optimized build (-fprofile-use)")

# --- BOLT: Emit relocations for post-link optimization ---
option(ENABLE_CLICKHOUSE_BOLT
    "Add --emit-relocs to linker flags so BOLT can post-process the binary" OFF)

# Validation
if (ENABLE_CLICKHOUSE_PGO_GENERATE AND CLICKHOUSE_PGO_PROFILE_PATH)
    message(FATAL_ERROR "ENABLE_CLICKHOUSE_PGO_GENERATE and CLICKHOUSE_PGO_PROFILE_PATH are mutually exclusive")
endif()

if (ENABLE_CLICKHOUSE_PGO_GENERATE AND ENABLE_THINLTO)
    message(FATAL_ERROR "ENABLE_CLICKHOUSE_PGO_GENERATE requires ENABLE_THINLTO=OFF (IR instrumentation is incompatible with ThinLTO)")
endif()

# Apply PGO generation flags
if (ENABLE_CLICKHOUSE_PGO_GENERATE)
    message(STATUS "ClickHouse PGO: instrumented build enabled (-fprofile-generate)")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-generate")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-generate")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-generate")
endif()

# Apply PGO use flags
if (CLICKHOUSE_PGO_PROFILE_PATH)
    if (NOT EXISTS "${CLICKHOUSE_PGO_PROFILE_PATH}")
        message(FATAL_ERROR "PGO profile not found: ${CLICKHOUSE_PGO_PROFILE_PATH}")
    endif()
    message(STATUS "ClickHouse PGO: using profile ${CLICKHOUSE_PGO_PROFILE_PATH}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-use=${CLICKHOUSE_PGO_PROFILE_PATH}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-use=${CLICKHOUSE_PGO_PROFILE_PATH}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-use=${CLICKHOUSE_PGO_PROFILE_PATH}")
    # When a function's CFG hash at use time does not match what was recorded
    # in the profile, clang's PGO backend emits `-Wbackend-plugin` with text
    # like "function control flow change detected (hash mismatch)" and
    # discards the stale counts for that function. This is expected to
    # happen for some functions because the instrumented build runs without
    # ThinLTO (required by IR instrumentation) while the use build enables
    # ThinLTO, which can perturb individual function bodies before profile
    # matching. Under the project-wide `-Werror` policy these informational
    # warnings would otherwise abort the whole build, so demote them to
    # plain warnings here.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=backend-plugin")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=backend-plugin")
endif()

# Apply BOLT linker flags
if (ENABLE_CLICKHOUSE_BOLT)
    message(STATUS "ClickHouse BOLT: adding --emit-relocs for post-link optimization")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--emit-relocs")
endif()
