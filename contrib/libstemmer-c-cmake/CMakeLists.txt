# Build libstemmer from the upstream Snowball source tree.
#
# At configure time this file:
#   1. Runs `make dist_libstemmer_c` inside the snowball submodule to produce
#      a self-contained libstemmer_c tarball (all C sources pre-generated).
#   2. Extracts that tarball into the CMake build directory.
#   3. Globs all *.c files from src_c/, runtime/, and libstemmer/.
#   4. Compiles those sources into the _stemmer static library.

set(SNOWBALL_DIR "${ClickHouse_SOURCE_DIR}/contrib/snowball")
set(STEMMER_DIST_DIR "${CMAKE_CURRENT_BINARY_DIR}/libstemmer_dist")

# ── Step 1: Generate the libstemmer_c distribution tarball ───────────────────
# The tarball is placed at dist/libstemmer_c-<version>.tar.gz inside the
# snowball source tree. We skip regeneration if it already exists so that
# reconfigures without submodule changes are fast.

if(NOT EXISTS "${STEMMER_DIST_DIR}")
    find_program(MAKE_EXECUTABLE NAMES make gmake
        DOC "GNU make used to run 'make dist_libstemmer_c' in the Snowball tree")
    if(NOT MAKE_EXECUTABLE)
        message(FATAL_ERROR
            "Cannot find 'make' to build the Snowball libstemmer_c distribution. "
            "Install GNU make.")
    endif()

    message(STATUS "Generating Snowball libstemmer_c distribution (make dist_libstemmer_c)")
    execute_process(
        COMMAND "${MAKE_EXECUTABLE}" dist_libstemmer_c
        WORKING_DIRECTORY "${SNOWBALL_DIR}"
        RESULT_VARIABLE _MAKE_RC
        ERROR_VARIABLE  _MAKE_ERR
    )
    if(NOT _MAKE_RC EQUAL 0)
        message(FATAL_ERROR
            "Failed to run 'make dist_libstemmer_c' in ${SNOWBALL_DIR}:\n${_MAKE_ERR}")
    endif()

    # Locate the produced tarball (name includes the version)
    file(GLOB _TARBALL "${SNOWBALL_DIR}/dist/libstemmer_c-*.tar.gz")
    if(NOT _TARBALL)
        message(FATAL_ERROR
            "No libstemmer_c-*.tar.gz found in ${SNOWBALL_DIR}/dist/ after make dist_libstemmer_c")
    endif()
    list(GET _TARBALL 0 _TARBALL)

    # ── Step 2: Extract the tarball ──────────────────────────────────────────
    message(STATUS "Extracting ${_TARBALL}")
    file(MAKE_DIRECTORY "${STEMMER_DIST_DIR}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar xzf "${_TARBALL}"
        WORKING_DIRECTORY "${STEMMER_DIST_DIR}"
        RESULT_VARIABLE _TAR_RC
        ERROR_VARIABLE  _TAR_ERR
    )
    if(NOT _TAR_RC EQUAL 0)
        message(FATAL_ERROR "Failed to extract ${_TARBALL}:\n${_TAR_ERR}")
    endif()
endif()

# The tarball extracts into a versioned subdirectory; locate it.
file(GLOB _DIST_SUBDIRS "${STEMMER_DIST_DIR}/libstemmer_c-*")
if(NOT _DIST_SUBDIRS)
    message(FATAL_ERROR
        "Expected a libstemmer_c-* subdirectory inside ${STEMMER_DIST_DIR}")
endif()
list(GET _DIST_SUBDIRS 0 LIBRARY_DIR)

set(STEMMER_INCLUDE_DIR "${LIBRARY_DIR}/include")

# ── Step 3: Collect sources ───────────────────────────────────────────────────
# The dist contains exactly three directories with C sources:
#   src_c/      – one .c file per stemming algorithm/encoding
#   runtime/    – api.c and utilities.c
#   libstemmer/ – libstemmer.c (the public API dispatcher)
# examples/stemwords.c is intentionally excluded.

file(GLOB _SOURCES
    "${LIBRARY_DIR}/src_c/*.c"
    "${LIBRARY_DIR}/runtime/*.c"
    "${LIBRARY_DIR}/libstemmer/libstemmer.c"
)

# ── Step 4: Build the _stemmer library ────────────────────────────────────────
add_library(_stemmer ${_SOURCES})
target_include_directories(_stemmer SYSTEM PUBLIC "${STEMMER_INCLUDE_DIR}")
add_library(ch_contrib::stemmer ALIAS _stemmer)
