# Note: The hash must match the Google Cloud Api version, otherwise funny things will happen.
# Find the right hash in "strip-prefix" in MODULE.bazel in the subrepository
set(PB_HASH "e60db19f11f94175ac682c5898cce0f77cc508ea")
set(PB_ARCHIVE "${PB_HASH}.tar.gz")
set(PB_DIR "googleapis-${PB_HASH}")

file(ARCHIVE_EXTRACT INPUT
    "${GOOGLE_CLOUD_CPP_CMAKE_DIR}/googleapis/${PB_ARCHIVE}"
    DESTINATION
    "tmp")

file(REMOVE_RECURSE "${EXTERNAL_GOOGLEAPIS_SOURCE}")
file(RENAME
    "tmp/${PB_DIR}"
    "${EXTERNAL_GOOGLEAPIS_SOURCE}"
)