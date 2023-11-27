option (ENABLE_SEASONAL "Enable seasonal decompose (stl-cpp) library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SEASONAL)
    message(STATUS "Not using seasonal decompose (stl-cpp)")
    return()
endif()
add_library(_stl-cpp INTERFACE)
target_include_directories(_stl-cpp INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/stl-cpp/include)
add_library(ch_contrib::seasonal-decompose ALIAS _stl-cpp)
