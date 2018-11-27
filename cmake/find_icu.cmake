option (ENABLE_ICU "Enable ICU" ON)

if (ENABLE_ICU)
    find_package(ICU COMPONENTS data i18n uc) # TODO: remove Modules/FindICU.cmake after cmake 3.7
    #set (ICU_LIBRARIES ${ICU_I18N_LIBRARY} ${ICU_UC_LIBRARY} ${ICU_DATA_LIBRARY} CACHE STRING "")
    set (ICU_LIBRARIES ICU::i18n ICU::uc ICU::data CACHE STRING "")
    if (ICU_FOUND)
        set(USE_ICU 1)
    endif ()
endif ()

if (USE_ICU)
    message (STATUS "Using icu=${USE_ICU}: ${ICU_INCLUDE_DIR} : ${ICU_LIBRARIES}")
else ()
    message (STATUS "Build without ICU (support for collations and charset conversion functions will be disabled)")
endif ()
