option(ENABLE_CURLCPP "Enable curlcpp" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CURLCPP)
    message(STATUS "Not using curlcpp")
    return()
endif ()

set(SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/curlcpp")

set(SRCS
        ${SRC_DIR}/src/cookie_date.cpp
        ${SRC_DIR}/src/cookie_time.cpp
        ${SRC_DIR}/src/curlcpp.pc.in
        ${SRC_DIR}/src/curl_exception.cpp
        ${SRC_DIR}/src/curl_global.cpp
        ${SRC_DIR}/src/curl_info.cpp
        ${SRC_DIR}/src/curl_share.cpp
        ${SRC_DIR}/src/cookie.cpp
        ${SRC_DIR}/src/cookie_datetime.cpp
        ${SRC_DIR}/src/curl_cookie.cpp
        ${SRC_DIR}/src/curl_easy.cpp
        ${SRC_DIR}/src/curl_form.cpp
        ${SRC_DIR}/src/curl_header.cpp
        ${SRC_DIR}/src/curl_multi.cpp
)

add_library(curlcpp ${SRCS})


target_link_libraries(curlcpp PRIVATE ch_contrib::curl)
target_include_directories(curlcpp PUBLIC "${SRC_DIR}/include" PRIVATE "${SRC_DIR}/src")

add_library(ch_contrib::curlcpp ALIAS curlcpp)