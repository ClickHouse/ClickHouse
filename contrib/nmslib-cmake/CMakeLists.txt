option(ENABLE_NMSLIB "Enable NMSLIB index support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_NMSLIB)
    message (STATUS "Not using NMSLIB")
    return()
endif()

set(NMSLIB_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/nmslib")
set(NMSLIB_SOURCE_DIR "${NMSLIB_PROJECT_DIR}/similarity_search")

file(GLOB NMSLIB_PROJ_HDR_FILES ${NMSLIB_SOURCE_DIR}/include/*.h ${NMSLIB_SOURCE_DIR}/include/method/*.h ${NMSLIB_SOURCE_DIR}/include/space/*.h ${NMSLIB_SOURCE_DIR}/include/factory/*.h ${NMSLIB_SOURCE_DIR}/include/factory/*/*.h)
file(GLOB NMSLIB_SRC_FILES ${NMSLIB_SOURCE_DIR}/src/*.cc ${NMSLIB_SOURCE_DIR}/src/space/*.cc ${NMSLIB_SOURCE_DIR}/src/method/*.cc)
list(REMOVE_ITEM NMSLIB_SRC_FILES ${NMSLIB_SOURCE_DIR}/src/space/space_sqfd.cc)

add_library(_nmslib ${NMSLIB_SRC_FILES} ${NMSLIB_PROJ_HDR_FILES})

target_include_directories(_nmslib SYSTEM PUBLIC "${NMSLIB_SOURCE_DIR}/include")
target_link_libraries(_nmslib PRIVATE boost::headers_only)

add_library(ch_contrib::nmslib ALIAS _nmslib)
