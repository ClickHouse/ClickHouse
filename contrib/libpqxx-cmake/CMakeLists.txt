option(ENABLE_LIBPQXX "Enalbe libpqxx" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBPQXX)
    message(STATUS "Not using libpqxx")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpqxx")

set (SRCS
    "${LIBRARY_DIR}/src/strconv.cxx"
    "${LIBRARY_DIR}/src/array.cxx"
    "${LIBRARY_DIR}/src/binarystring.cxx"
    "${LIBRARY_DIR}/src/connection.cxx"
    "${LIBRARY_DIR}/src/cursor.cxx"
    "${LIBRARY_DIR}/src/encodings.cxx"
    "${LIBRARY_DIR}/src/errorhandler.cxx"
    "${LIBRARY_DIR}/src/except.cxx"
    "${LIBRARY_DIR}/src/field.cxx"
    "${LIBRARY_DIR}/src/largeobject.cxx"
    "${LIBRARY_DIR}/src/notification.cxx"
    "${LIBRARY_DIR}/src/pipeline.cxx"
    "${LIBRARY_DIR}/src/result.cxx"
    "${LIBRARY_DIR}/src/robusttransaction.cxx"
    "${LIBRARY_DIR}/src/sql_cursor.cxx"
    "${LIBRARY_DIR}/src/stream_from.cxx"
    "${LIBRARY_DIR}/src/stream_to.cxx"
    "${LIBRARY_DIR}/src/subtransaction.cxx"
    "${LIBRARY_DIR}/src/transaction.cxx"
    "${LIBRARY_DIR}/src/transaction_base.cxx"
    "${LIBRARY_DIR}/src/row.cxx"
    "${LIBRARY_DIR}/src/params.cxx"
    "${LIBRARY_DIR}/src/util.cxx"
    "${LIBRARY_DIR}/src/version.cxx"
)

# Need to explicitly include each header file, because in the directory include/pqxx there are also files
# like just 'array'. So if including the whole directory with `target_include_directories`, it will make
# conflicts with all includes of <array>.
set (HDRS
    "${LIBRARY_DIR}/include/pqxx/array.hxx"
    "${LIBRARY_DIR}/include/pqxx/params.hxx"
    "${LIBRARY_DIR}/include/pqxx/binarystring.hxx"
    "${LIBRARY_DIR}/include/pqxx/composite.hxx"
    "${LIBRARY_DIR}/include/pqxx/connection.hxx"
    "${LIBRARY_DIR}/include/pqxx/cursor.hxx"
    "${LIBRARY_DIR}/include/pqxx/dbtransaction.hxx"
    "${LIBRARY_DIR}/include/pqxx/errorhandler.hxx"
    "${LIBRARY_DIR}/include/pqxx/except.hxx"
    "${LIBRARY_DIR}/include/pqxx/field.hxx"
    "${LIBRARY_DIR}/include/pqxx/isolation.hxx"
    "${LIBRARY_DIR}/include/pqxx/largeobject.hxx"
    "${LIBRARY_DIR}/include/pqxx/nontransaction.hxx"
    "${LIBRARY_DIR}/include/pqxx/notification.hxx"
    "${LIBRARY_DIR}/include/pqxx/pipeline.hxx"
    "${LIBRARY_DIR}/include/pqxx/prepared_statement.hxx"
    "${LIBRARY_DIR}/include/pqxx/result.hxx"
    "${LIBRARY_DIR}/include/pqxx/robusttransaction.hxx"
    "${LIBRARY_DIR}/include/pqxx/row.hxx"
    "${LIBRARY_DIR}/include/pqxx/separated_list.hxx"
    "${LIBRARY_DIR}/include/pqxx/strconv.hxx"
    "${LIBRARY_DIR}/include/pqxx/stream_from.hxx"
    "${LIBRARY_DIR}/include/pqxx/stream_to.hxx"
    "${LIBRARY_DIR}/include/pqxx/subtransaction.hxx"
    "${LIBRARY_DIR}/include/pqxx/transaction.hxx"
    "${LIBRARY_DIR}/include/pqxx/transaction_base.hxx"
    "${LIBRARY_DIR}/include/pqxx/types.hxx"
    "${LIBRARY_DIR}/include/pqxx/util.hxx"
    "${LIBRARY_DIR}/include/pqxx/version.hxx"
    "${LIBRARY_DIR}/include/pqxx/zview.hxx"
)

add_library(_libpqxx ${SRCS} ${HDRS})

target_link_libraries(_libpqxx PUBLIC ch_contrib::libpq)
target_include_directories (_libpqxx SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include")

add_library(ch_contrib::libpqxx ALIAS _libpqxx)
