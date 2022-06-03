set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/cpp-dns")

set(SRCS "${LIBRARY_DIR}/DNSResolver.cpp")

set(HDRS "${LIBRARY_DIR}/DNSResolver.hpp")

add_library(_cpp-dns ${SRCS} ${HDRS})

target_link_libraries(_cpp-dns boost::system boost::headers_only ch_contrib::udns)

target_include_directories(_cpp-dns SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

add_library(ch_contrib::cpp-dns ALIAS _cpp-dns)