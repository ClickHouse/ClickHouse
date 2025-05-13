option (ENABLE_NGHTTP2 "Use nghttp2" ${ENABLE_LIBRARIES})
if (NOT ENABLE_NGHTTP2)
    message(STATUS "Not using nghttp2")
    return()
endif()

set(NGHTTP2_DIR "${ClickHouse_SOURCE_DIR}/contrib/nghttp2")

# We use this trick because nghttp2 contains a LICENSE file with "See COPYING" in it
# It breaks ClickHouse license checker (because LICENSE is prioritized over COPYING) so we just remove it
# Unfortunately, it actually removes the file from the filesystem but I can't think of a better solution now
file(REMOVE "${NGHTTP2_DIR}/LICENSE")

set(ENABLE_LIB_ONLY ON CACHE BOOL "Build only nghttp2 lib" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "Build nghttp2 as a static lib" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build nghttp2 as a shared lib" FORCE)

add_subdirectory(${NGHTTP2_DIR} ${NGHTTP2_DIR} EXCLUDE_FROM_ALL)
set_target_properties(nghttp2_static PROPERTIES EXCLUDE_FROM_ALL TRUE)
set_target_properties(nghttp2_static PROPERTIES INTERFACE_LINK_LIBRARIES "")
add_library(ch_contrib::nghttp2 ALIAS nghttp2_static)
