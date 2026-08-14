set(ENABLE_JWT_CPP_DEFAULT ON)

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

# `PICOJSON_USE_LOCALE=0`: picojson otherwise reads the decimal separator with `localeconv`, which
# `base/harmful` bans, so any `.` in a numeric token aborts debug and sanitizer builds with SIGILL.
# Nothing here changes `LC_NUMERIC`, so the separator is always `.` anyway.
# `PICOJSON_USE_INT64`: `jwt-cpp/jwt.h` defines it before including picojson, and it changes the
# layout of `picojson::value`, so every other consumer must define it too or they violate ODR.
target_compile_definitions(_jwt-cpp INTERFACE PICOJSON_USE_LOCALE=0 PICOJSON_USE_INT64)

add_library(ch_contrib::jwt-cpp ALIAS _jwt-cpp)
