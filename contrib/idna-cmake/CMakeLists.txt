option(ENABLE_IDNA "Enable idna support" ${ENABLE_LIBRARIES})
if ((NOT ENABLE_IDNA))
    message (STATUS "Not using idna")
    return()
endif()
set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/idna")

set (SRCS
    "${LIBRARY_DIR}/src/idna.cpp"
    "${LIBRARY_DIR}/src/mapping.cpp"
    "${LIBRARY_DIR}/src/mapping_tables.cpp"
    "${LIBRARY_DIR}/src/normalization.cpp"
    "${LIBRARY_DIR}/src/normalization_tables.cpp"
    "${LIBRARY_DIR}/src/punycode.cpp"
    "${LIBRARY_DIR}/src/to_ascii.cpp"
    "${LIBRARY_DIR}/src/to_unicode.cpp"
    "${LIBRARY_DIR}/src/unicode_transcoding.cpp"
    "${LIBRARY_DIR}/src/validity.cpp"
)

add_library (_idna ${SRCS})
target_include_directories(_idna PUBLIC "${LIBRARY_DIR}/include")

add_library (ch_contrib::idna ALIAS _idna)
