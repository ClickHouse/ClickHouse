option (ENABLE_POCKETFFT "Enable pocketfft" ${ENABLE_LIBRARIES})

if (NOT ENABLE_POCKETFFT)
    message(STATUS "Not using pocketfft")
    return()
endif()

add_library(_pocketfft INTERFACE)
target_include_directories(_pocketfft INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/pocketfft)
add_library(ch_contrib::pocketfft ALIAS _pocketfft)
