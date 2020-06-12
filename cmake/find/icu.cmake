if (OS_LINUX)
    option(ENABLE_ICU "Enable ICU" ${ENABLE_LIBRARIES})
else ()
    option(ENABLE_ICU "Enable ICU" 0)
endif ()

if (ENABLE_ICU)

option (USE_INTERNAL_ICU_LIBRARY "Set to FALSE to use system ICU library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/icu/icu4c/LICENSE")
    if (USE_INTERNAL_ICU_LIBRARY)
        message (WARNING "submodule contrib/icu is missing. to fix try run: \n git submodule update --init --recursive")
        set (USE_INTERNAL_ICU_LIBRARY 0)
    endif ()
    set (MISSING_INTERNAL_ICU_LIBRARY 1)
endif ()

if(NOT USE_INTERNAL_ICU_LIBRARY)
    if (APPLE)
        set(ICU_ROOT "/usr/local/opt/icu4c" CACHE STRING "")
    endif()
    find_package(ICU COMPONENTS i18n uc data) # TODO: remove Modules/FindICU.cmake after cmake 3.7
    #set (ICU_LIBRARIES ${ICU_I18N_LIBRARY} ${ICU_UC_LIBRARY} ${ICU_DATA_LIBRARY} CACHE STRING "")
    if(ICU_FOUND)
        set(USE_ICU 1)
    endif()
endif()

if (ICU_LIBRARY AND ICU_INCLUDE_DIR)
    set (USE_ICU 1)
elseif (NOT MISSING_INTERNAL_ICU_LIBRARY)
    set (USE_INTERNAL_ICU_LIBRARY 1)
    set (ICU_LIBRARIES icui18n icuuc icudata)
    set (USE_ICU 1)
endif ()

endif()

if(USE_ICU)
    message(STATUS "Using icu=${USE_ICU}: ${ICU_INCLUDE_DIR} : ${ICU_LIBRARIES}")
else()
    message(STATUS "Build without ICU (support for collations and charset conversion functions will be disabled)")
endif()
