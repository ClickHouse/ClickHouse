set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpqxx")

set (SRCS
    "${LIBRARY_DIR}/src/array.cxx"
    "${LIBRARY_DIR}/src/blob.cxx"
    "${LIBRARY_DIR}/src/connection.cxx"
    "${LIBRARY_DIR}/src/cursor.cxx"
    "${LIBRARY_DIR}/src/encodings.cxx"
    "${LIBRARY_DIR}/src/errorhandler.cxx"
    "${LIBRARY_DIR}/src/except.cxx"
    "${LIBRARY_DIR}/src/field.cxx"
    "${LIBRARY_DIR}/src/largeobject.cxx"
    "${LIBRARY_DIR}/src/notification.cxx"
    "${LIBRARY_DIR}/src/params.cxx"
    "${LIBRARY_DIR}/src/pipeline.cxx"
    "${LIBRARY_DIR}/src/result.cxx"
    "${LIBRARY_DIR}/src/robusttransaction.cxx"
    "${LIBRARY_DIR}/src/row.cxx"
    "${LIBRARY_DIR}/src/sql_cursor.cxx"
    "${LIBRARY_DIR}/src/strconv.cxx"
    "${LIBRARY_DIR}/src/stream_from.cxx"
    "${LIBRARY_DIR}/src/stream_to.cxx"
    "${LIBRARY_DIR}/src/subtransaction.cxx"
    "${LIBRARY_DIR}/src/time.cxx"
    "${LIBRARY_DIR}/src/transaction.cxx"
    "${LIBRARY_DIR}/src/transaction_base.cxx"
    "${LIBRARY_DIR}/src/types.cxx"
    "${LIBRARY_DIR}/src/util.cxx"
    "${LIBRARY_DIR}/src/wait.cxx"
)

# The template only contains `#undef PQXX_HAVE_*` lines; copying it verbatim leaves every optional
# feature disabled, which matches how the library was configured before and keeps the build hermetic
# (no try_compile feature probes).
configure_file(${LIBRARY_DIR}/include/pqxx/internal/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/pqxx/internal/config.h @ONLY)

add_library(_libpqxx ${SRCS})
target_link_libraries(_libpqxx PUBLIC ch_contrib::libpq)
target_include_directories (_libpqxx SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include" ${CMAKE_CURRENT_BINARY_DIR})

add_library(ch_contrib::libpqxx ALIAS _libpqxx)
