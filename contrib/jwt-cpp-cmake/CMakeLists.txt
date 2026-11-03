set(ENABLE_JWT_CPP_DEFAULT OFF)
if(ENABLE_LIBRARIES)
    set(ENABLE_JWT_CPP_DEFAULT ON)
endif()

option(ENABLE_JWT_CPP "Enable jwt-cpp library" ${ENABLE_JWT_CPP_DEFAULT})

if (NOT ENABLE_JWT_CPP)
    message(STATUS "Not using jwt-cpp")
    return()
endif()

if(ENABLE_JWT_CPP)
    if(NOT TARGET OpenSSL::Crypto)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use jwt-cpp without OpenSSL")
    endif()
endif()

set (JWT_CPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/jwt-cpp/include")

add_library (_jwt-cpp INTERFACE)
target_include_directories(_jwt-cpp SYSTEM BEFORE INTERFACE ${JWT_CPP_INCLUDE_DIR})
add_library(ch_contrib::jwt-cpp ALIAS _jwt-cpp)
