option(ENABLE_IDNA "Enable idna support" ${ENABLE_LIBRARIES})

if ((NOT ENABLE_IDNA))
    message (STATUS "Not using idna")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/idna")

# idna.cpp is an amalgamation: it #includes the other .cpp files of the
# library (directly, or via mapping.cpp and normalization.cpp for the tables),
# so it is the only translation unit to compile. Listing the included files as
# well defines every symbol twice in the archive.
set (SRCS
    "${LIBRARY_DIR}/src/idna.cpp"
)

add_library (_idna ${SRCS})
target_include_directories(_idna PUBLIC "${LIBRARY_DIR}/include")

add_library (ch_contrib::idna ALIAS _idna)
