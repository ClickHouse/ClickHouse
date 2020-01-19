set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/replxx")

set(SRCS
    ${LIBRARY_DIR}/src/conversion.cxx
    ${LIBRARY_DIR}/src/ConvertUTF.cpp
    ${LIBRARY_DIR}/src/escape.cxx
    ${LIBRARY_DIR}/src/history.cxx
    ${LIBRARY_DIR}/src/io.cxx
    ${LIBRARY_DIR}/src/prompt.cxx
    ${LIBRARY_DIR}/src/replxx.cxx
    ${LIBRARY_DIR}/src/replxx_impl.cxx
    ${LIBRARY_DIR}/src/util.cxx
    ${LIBRARY_DIR}/src/wcwidth.cpp
    ${LIBRARY_DIR}/src/windows.cxx
)

add_library(replxx ${SRCS})
target_include_directories(replxx PUBLIC ${LIBRARY_DIR}/include)
target_compile_options(replxx PUBLIC -Wno-documentation)
