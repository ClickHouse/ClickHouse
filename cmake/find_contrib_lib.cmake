macro(find_contrib_lib LIB_NAME)

    string(TOLOWER ${LIB_NAME} LIB_NAME_LC)
    string(TOUPPER ${LIB_NAME} LIB_NAME_UC)
    string(REPLACE "-" "_" LIB_NAME_UC ${LIB_NAME_UC})

    option (USE_INTERNAL_${LIB_NAME_UC}_LIBRARY "Use bundled library ${LIB_NAME} instead of system" ${NOT_UNBUNDLED})

    if (NOT USE_INTERNAL_${LIB_NAME_UC}_LIBRARY)
        find_package ("${LIB_NAME}")
    endif ()

    if (NOT ${LIB_NAME_UC}_FOUND)
        set (USE_INTERNAL_${LIB_NAME_UC}_LIBRARY 1)
        set (${LIB_NAME_UC}_LIBRARIES ${LIB_NAME_LC})
        set (${LIB_NAME_UC}_INCLUDE_DIR ${${LIB_NAME_UC}_CONTRIB_INCLUDE_DIR})
    endif ()

    message (STATUS "Using ${LIB_NAME}: ${${LIB_NAME_UC}_INCLUDE_DIR} : ${${LIB_NAME_UC}_LIBRARIES}")

endmacro()
