# Choose to build static or shared library for c-ares.
if (USE_STATIC_LIBRARIES)
    set(CARES_STATIC ON CACHE BOOL "" FORCE)
    set(CARES_SHARED OFF CACHE BOOL "" FORCE)
else ()
    set(CARES_STATIC OFF CACHE BOOL "" FORCE)
    set(CARES_SHARED ON CACHE BOOL "" FORCE)
endif ()

# Disable looking for libnsl on a platforms that has gethostbyname in glibc
#
# c-ares searching for gethostbyname in the libnsl library, however in the
# version that shipped with gRPC it doing it wrong [1], since it uses
# CHECK_LIBRARY_EXISTS(), which will return TRUE even if the function exists in
# another dependent library. The upstream already contains correct macro [2],
# but it is not included in gRPC (even upstream gRPC, not the one that is
# shipped with clickhousee).
#
#   [1]: https://github.com/c-ares/c-ares/blob/e982924acee7f7313b4baa4ee5ec000c5e373c30/CMakeLists.txt#L125
#   [2]: https://github.com/c-ares/c-ares/blob/44fbc813685a1fa8aa3f27fcd7544faf612d376a/CMakeLists.txt#L146
#
# And because if you by some reason have libnsl [3] installed, clickhouse will
# reject to start w/o it. While this is completelly different library.
#
#   [3]: https://packages.debian.org/bullseye/libnsl2
if (NOT CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    set(HAVE_LIBNSL OFF CACHE BOOL "" FORCE)
endif()

# Force use of c-ares inet_net_pton instead of libresolv one
set(HAVE_INET_NET_PTON OFF CACHE BOOL "" FORCE)

add_subdirectory("../c-ares/" "../c-ares/")

add_library(ch_contrib::c-ares ALIAS c-ares)