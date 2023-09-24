option(ENABLE_CURLPP "Enable curlpp" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CURLPP)
    message(STATUS "Not using curlpp")
    return()
endif ()

set(SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/curlpp")

set(SRCS
        ${SRC_DIR}/src/curlpp/cURLpp.cpp
        ${SRC_DIR}/src/curlpp/Easy.cpp
        ${SRC_DIR}/src/curlpp/Exception.cpp
        ${SRC_DIR}/src/curlpp/Form.cpp
        ${SRC_DIR}/src/curlpp/Multi.cpp
        ${SRC_DIR}/src/curlpp/OptionBase.cpp
        ${SRC_DIR}/src/curlpp/Options.cpp
        ${SRC_DIR}/src/curlpp/internal/CurlHandle.cpp
        ${SRC_DIR}/src/curlpp/internal/OptionList.cpp
        ${SRC_DIR}/src/curlpp/internal/OptionSetter.cpp
        ${SRC_DIR}/src/curlpp/internal/SList.cpp
)

add_library(curlpp ${SRCS})


target_link_libraries(curlpp PRIVATE ch_contrib::curl)
target_include_directories(curlpp PUBLIC "${SRC_DIR}/include" PRIVATE "${SRC_DIR}/src")

add_library(ch_contrib::curlpp ALIAS curlpp)
