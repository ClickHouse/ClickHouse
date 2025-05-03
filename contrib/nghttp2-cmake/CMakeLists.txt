set(NGHTTP2_DIR "${ClickHouse_SOURCE_DIR}/contrib/nghttp2")

set(ENABLE_LIB_ONLY ON CACHE BOOL "Build libnghttp2 only" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "Build nghttp2 as static lib" FORCE)

add_subdirectory(${NGHTTP2_DIR} ${NGHTTP2_DIR} EXCLUDE_FROM_ALL)
set_target_properties(nghttp2_static PROPERTIES EXCLUDE_FROM_ALL TRUE)

set_target_properties(nghttp2_static PROPERTIES INTERFACE_LINK_LIBRARIES "")

target_include_directories(nghttp2_static SYSTEM BEFORE PUBLIC 
    ${NGHTTP2_SOURCE_DIR}/lib/includes
    ${NGHTTP2_SOURCE_DIR}/lib
    ${NGHTTP2_BINARY_DIR}
)

add_library(ch_contrib::nghttp2 ALIAS nghttp2_static)
