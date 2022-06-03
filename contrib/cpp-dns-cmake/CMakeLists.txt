set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/cpp-dns")

# If not, ASIO standalone mode.
#find_path(LIBUDNS_INCLUDE_DIR
#        NAME udns.h
#        HINTS /opt/local/include)
#
#find_library(lib_udns udns
#        PATHS /opt/local/lib)

#if (lib_udns)
#    message("Found libudns!")
#else()
#    message(FATAL_ERROR "udns not found")
#endif()

#find_package(Boost 1.78 COMPONENTS system REQUIRED)#[[]]

set(SRCS "${LIBRARY_DIR}/DNSResolver.cpp")

set(HDRS "${LIBRARY_DIR}/DNSResolver.hpp")

add_library(_cpp-dns ${SRCS} ${HDRS})

#message("Libraries: lb: ${LIBRARY_DIR} bid: ${Boost_INCLUDE_DIRS} bl: ${Boost_LIBRARIES} libudns: ${lib_udns}")

target_link_libraries(_cpp-dns boost::system boost::headers_only ch_contrib::udns)

target_include_directories(_cpp-dns SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

add_library(ch_contrib::cpp-dns ALIAS _cpp-dns)