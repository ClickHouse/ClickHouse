if (CMAKE_VERSION VERSION_LESS 2.8.9)
    message(FATAL_ERROR "Poco requires at least CMake version 2.8.9")
endif()

if (NOT Poco_FIND_COMPONENTS)
    set(Poco_NOT_FOUND_MESSAGE "The Poco package requires at least one component")
    set(Poco_FOUND False)
    return()
endif()

set(_Poco_FIND_PARTS_REQUIRED)
if (Poco_FIND_REQUIRED)
    set(_Poco_FIND_PARTS_REQUIRED REQUIRED)
endif()
set(_Poco_FIND_PARTS_QUIET)
if (Poco_FIND_QUIETLY)
    set(_Poco_FIND_PARTS_QUIET QUIET)
endif()

get_filename_component(_Poco_install_prefix "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)

set(_Poco_NOTFOUND_MESSAGE)

# Let components find each other, but don't overwrite CMAKE_PREFIX_PATH
set(_Poco_CMAKE_PREFIX_PATH_old ${CMAKE_PREFIX_PATH})
set(CMAKE_PREFIX_PATH ${_Poco_install_prefix})

foreach(module ${Poco_FIND_COMPONENTS})
    find_package(Poco${module}
        ${_Poco_FIND_PARTS_QUIET}
        ${_Poco_FIND_PARTS_REQUIRED}
        PATHS "${_Poco_install_prefix}" NO_DEFAULT_PATH
    )
    if (NOT Poco${module}_FOUND)
        if (Poco_FIND_REQUIRED_${module})
            set(_Poco_NOTFOUND_MESSAGE "${_Poco_NOTFOUND_MESSAGE}Failed to find Poco component \"${module}\" config file at \"${_Poco_install_prefix}/Poco${module}/Poco${module}Config.cmake\"\n")
        elseif(NOT Poco_FIND_QUIETLY)
            message(WARNING "Failed to find Poco component \"${module}\" config file at \"${_Poco_install_prefix}/Poco${module}/Poco${module}Config.cmake\"")
        endif()
    endif()

    # For backward compatibility set the LIBRARIES variable
    list(APPEND Poco_LIBRARIES "Poco::${module}")
endforeach()

# Restore the original CMAKE_PREFIX_PATH value
set(CMAKE_PREFIX_PATH ${_Poco_CMAKE_PREFIX_PATH_old})

if (_Poco_NOTFOUND_MESSAGE)
    set(Poco_NOT_FOUND_MESSAGE "${_Poco_NOTFOUND_MESSAGE}")
    set(Poco_FOUND False)
endif()

