# https://github.com/astahl/poco-cmake/blob/master/cmake/FindPoco.cmake

# - finds the Poco C++ libraries
# This module finds the Applied Informatics Poco libraries.
# It supports the following components:
#
# Util (loaded by default)
# Foundation (loaded by default)
# XML
# Zip
# Crypto
# Data
# Net
# NetSSL
# OSP
#
# Usage:
#    set(ENV{Poco_DIR} path/to/poco/sdk)
#    find_package(Poco REQUIRED OSP Data Crypto)
#
# On completion, the script defines the following variables:
#
#    - Compound variables:
#   Poco_FOUND
#        - true if all requested components were found.
#    Poco_LIBRARIES
#        - contains release (and debug if available) libraries for all requested components.
#          It has the form "optimized LIB1 debug LIBd1 optimized LIB2 ...", ready for use with the target_link_libraries command.
#    Poco_INCLUDE_DIRS
#        - Contains include directories for all requested components.
#
#    - Component variables:
#   Poco_Xxx_FOUND
#        - Where Xxx is the properly cased component name (eg. 'Util', 'OSP').
#          True if a component's library or debug library was found successfully.
#    Poco_Xxx_LIBRARY
#        - Library for component Xxx.
#    Poco_Xxx_LIBRARY_DEBUG
#        - debug library for component Xxx
#   Poco_Xxx_INCLUDE_DIR
#        - include directory for component Xxx
#
#      - OSP BundleCreator variables: (i.e. bundle.exe on windows, bundle on unix-likes)
#        (is only discovered if OSP is a requested component)
#    Poco_OSP_Bundle_EXECUTABLE_FOUND
#        - true if the bundle-creator executable was found.
#    Poco_OSP_Bundle_EXECUTABLE
#        - the path to the bundle-creator executable.
#
# Author: Andreas Stahl andreas.stahl@tu-dresden.de

set(Poco_HINTS
    /usr/local
    /usr/local/include/Poco
    C:/AppliedInformatics
    ${Poco_DIR}
    $ENV{Poco_DIR}
)

if(NOT Poco_ROOT_DIR)
    # look for the root directory, first for the source-tree variant
    find_path(Poco_ROOT_DIR
        NAMES Foundation/include/Poco/Poco.h
        HINTS ${Poco_HINTS}
    )
    if(NOT Poco_ROOT_DIR)
        # this means poco may have a different directory structure, maybe it was installed, let's check for that
        message(STATUS "Looking for Poco install directory structure.")
        find_path(Poco_ROOT_DIR
            NAMES include/Poco/Poco.h
            HINTS ${Poco_HINTS}
        )
        if(NOT Poco_ROOT_DIR)
            # poco was still not found -> Fail
            if(Poco_FIND_REQUIRED)
                message(FATAL_ERROR "Poco: Could not find Poco install directory")
            endif()
            if(NOT Poco_FIND_QUIETLY)
                message(STATUS "Poco: Could not find Poco install directory")
            endif()
            return()
        else()
            # poco was found with the make install directory structure
            message(STATUS "Assuming Poco install directory structure at ${Poco_ROOT_DIR}.")
            set(Poco_INSTALLED true)
        endif()
    endif()
endif()

# add dynamic library directory
if(WIN32)
    find_path(Poco_RUNTIME_LIBRARY_DIRS
        NAMES PocoFoundation.dll
        HINTS ${Poco_ROOT_DIR}
        PATH_SUFFIXES
            bin
            lib
    )
endif()

# if installed directory structure, set full include dir
if(Poco_INSTALLED)
    set(Poco_INCLUDE_DIRS ${Poco_ROOT_DIR}/include/ CACHE PATH "The global include path for Poco")
endif()

# append the default minimum components to the list to find
list(APPEND components
    ${Poco_FIND_COMPONENTS}
    # default components:
    "Util"
    "Foundation"
)
list(REMOVE_DUPLICATES components) # remove duplicate defaults

foreach( component ${components} )
    #if(NOT Poco_${component}_FOUND)

    # include directory for the component
    if(NOT Poco_${component}_INCLUDE_DIR)
        if (${component} STREQUAL "DataODBC")
            set (component_in_data "ODBC")
        else ()
            set (component_in_data "")
        endif ()
        if (${component} STREQUAL "NetSSL")
            set (component_alt "Net")
        else ()
            set (component_alt ${component})
        endif ()
        find_path(Poco_${component}_INCLUDE_DIR
            NAMES
                Poco/${component}.h     # e.g. Foundation.h
                Poco/${component}/${component}.h # e.g. OSP/OSP.h Util/Util.h
                Poco/${component_alt}/${component}.h # e.g. Net/NetSSL.h
                Poco/Data/${component_in_data}/${component_in_data}.h # e.g. Data/ODBC/ODBC.h
            HINTS
                ${Poco_ROOT_DIR}
            PATH_SUFFIXES
                include
                ${component}/include
        )
    endif()
    if(NOT Poco_${component}_INCLUDE_DIR)
        message(WARNING "Poco_${component}_INCLUDE_DIR NOT FOUND")
    else()
        list(APPEND Poco_INCLUDE_DIRS ${Poco_${component}_INCLUDE_DIR})
    endif()

    # release library
    if(NOT Poco_${component}_LIBRARY)
        find_library(
            Poco_${component}_LIBRARY
            NAMES Poco${component}
            HINTS ${Poco_ROOT_DIR}
            PATH_SUFFIXES
                lib
                bin
        )
        if(Poco_${component}_LIBRARY)
            message(STATUS "Found Poco ${component}: ${Poco_${component}_LIBRARY}")
        endif()
    endif()
    if(Poco_${component}_LIBRARY)
        list(APPEND Poco_LIBRARIES "optimized" ${Poco_${component}_LIBRARY} )
        mark_as_advanced(Poco_${component}_LIBRARY)
    endif()

    # debug library
    if(NOT Poco_${component}_LIBRARY_DEBUG)
        find_library(
            Poco_${component}_LIBRARY_DEBUG
            Names Poco${component}d
            HINTS ${Poco_ROOT_DIR}
            PATH_SUFFIXES
                lib
                bin
        )
        if(Poco_${component}_LIBRARY_DEBUG)
            message(STATUS "Found Poco ${component} (debug): ${Poco_${component}_LIBRARY_DEBUG}")
        endif()
    endif(NOT Poco_${component}_LIBRARY_DEBUG)
    if(Poco_${component}_LIBRARY_DEBUG)
        list(APPEND Poco_LIBRARIES "debug" ${Poco_${component}_LIBRARY_DEBUG})
        mark_as_advanced(Poco_${component}_LIBRARY_DEBUG)
    endif()

    # mark component as found or handle not finding it
    if(Poco_${component}_LIBRARY_DEBUG OR Poco_${component}_LIBRARY)
        set(Poco_${component}_FOUND TRUE)
    elseif(NOT Poco_FIND_QUIETLY)
        message(WARNING "Could not find Poco component ${component}!")
    endif()
endforeach()

if(Poco_DataODBC_LIBRARY)
    list(APPEND Poco_DataODBC_LIBRARY ${ODBC_LIBRARIES} ${LTDL_LIBRARY})
    list(APPEND Poco_INCLUDE_DIRS ${ODBC_INCLUDE_DIRECTORIES})
endif()

if(Poco_NetSSL_LIBRARY)
    list(APPEND Poco_NetSSL_LIBRARY ${OPENSSL_LIBRARIES})
    list(APPEND Poco_INCLUDE_DIRS ${OPENSSL_INCLUDE_DIR})
endif()

if(DEFINED Poco_LIBRARIES)
    set(Poco_FOUND true)
endif()

if(${Poco_OSP_FOUND})
    # find the osp bundle program
    find_program(
        Poco_OSP_Bundle_EXECUTABLE
        NAMES bundle
        HINTS
            ${Poco_RUNTIME_LIBRARY_DIRS}
            ${Poco_ROOT_DIR}
        PATH_SUFFIXES
            bin
            OSP/BundleCreator/bin/Darwin/x86_64
            OSP/BundleCreator/bin/Darwin/i386
        DOC "The executable that bundles OSP packages according to a .bndlspec specification."
    )
    if(Poco_OSP_Bundle_EXECUTABLE)
        set(Poco_OSP_Bundle_EXECUTABLE_FOUND true)
    endif()
    # include bundle script file
    find_file(Poco_OSP_Bundles_file NAMES PocoBundles.cmake HINTS ${CMAKE_MODULE_PATH})
    if(${Poco_OSP_Bundles_file})
        include(${Poco_OSP_Bundles_file})
    endif()
endif()

message(STATUS "Found Poco: ${Poco_LIBRARIES}")
