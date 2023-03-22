option (ENABLE_REPLXX "Enable replxx support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_REPLXX)
    message (STATUS "Not using replxx")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/replxx")

set(SRCS
    "${LIBRARY_DIR}/src/conversion.cxx"
    "${LIBRARY_DIR}/src/ConvertUTF.cpp"
    "${LIBRARY_DIR}/src/escape.cxx"
    "${LIBRARY_DIR}/src/history.cxx"
    "${LIBRARY_DIR}/src/terminal.cxx"
    "${LIBRARY_DIR}/src/prompt.cxx"
    "${LIBRARY_DIR}/src/replxx_impl.cxx"
    "${LIBRARY_DIR}/src/replxx.cxx"
    "${LIBRARY_DIR}/src/util.cxx"
    "${LIBRARY_DIR}/src/wcwidth.cpp"
)

add_library (_replxx ${SRCS})
target_include_directories(_replxx SYSTEM PUBLIC "${LIBRARY_DIR}/include")

if (COMPILER_CLANG)
    target_compile_options(_replxx PRIVATE -Wno-documentation)
endif ()

add_library(ch_contrib::replxx ALIAS _replxx)
