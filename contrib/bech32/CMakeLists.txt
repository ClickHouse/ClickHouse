option(ENABLE_BECH32 "Enable bech32 support" ${ENABLE_LIBRARIES})
if ((NOT ENABLE_BECH32))
    message (STATUS "Not using bech32")
    return()
endif()

add_library(_bech32
    include/bech32.h
    src/bech32.cpp)

target_include_directories(_bech32 PUBLIC include)
target_include_directories(_bech32 PRIVATE src)

add_library(ch_contrib::bech32 ALIAS _bech32)
