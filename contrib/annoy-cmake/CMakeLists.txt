set(ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/annoy")
set(ANNOY_SOURCE_DIR "${ANNOY_PROJECT_DIR}/src")
set(ANNOY_INCLUDE_DIR ${ANNOY_PROJECT_DIR}/include)

set(ANNOY_HEADERS 
    ${ANNOY_INCLUDE_DIR}/annoy.h
    ${ANNOY_INCLUDE_DIR}/node.h
    ${ANNOY_INCLUDE_DIR}/point.h
)

set(ANNOY_SRC
    ${ANNOY_SOURCE_DIR}/annoy.cpp
    ${ANNOY_SOURCE_DIR}/node.cpp
    ${ANNOY_SOURCE_DIR}/point.cpp
    ${ANNOY_SOURCE_DIR}/settings.cpp
)

add_library(_annoy ${ANNOY_SRC})
target_include_directories(_annoy SYSTEM PUBLIC ${ANNOY_INCLUDE_DIR})

add_library(ch_contrib::annoy ALIAS _annoy)
