# Find OpenLDAP libraries.
#
# Can be configured with:
#   OPENLDAP_ROOT_DIR           - path to the OpenLDAP installation prefix
#   OPENLDAP_USE_STATIC_LIBS    - look for static version of the libraries
#   OPENLDAP_USE_REENTRANT_LIBS - look for thread-safe version of the libraries
#
# Sets values of:
#   OPENLDAP_FOUND              - TRUE if found
#   OPENLDAP_INCLUDE_DIRS       - paths to the include directories
#   OPENLDAP_LIBRARIES          - paths to the libldap and liblber libraries; libsasl2 (Cyrus SASL) and libgssapi (GSSAPI) libraries
#                                 will be listed here too, if found, if static OpenLDAP libraries are requested
#   OPENLDAP_LDAP_LIBRARY       - paths to the libldap library
#   OPENLDAP_LBER_LIBRARY       - paths to the liblber library
#

if(OPENLDAP_USE_STATIC_LIBS)
    set(_orig_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
    if(WIN32)
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".lib" ".a" ${CMAKE_FIND_LIBRARY_SUFFIXES})
    else()
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    endif()
endif()

set(_r_suffix)
if(OPENLDAP_USE_REENTRANT_LIBS)
    set(_r_suffix "_r")
endif()

if(OPENLDAP_ROOT_DIR)
    find_path(OPENLDAP_INCLUDE_DIRS NAMES "ldap.h" "lber.h" PATHS "${OPENLDAP_ROOT_DIR}" PATH_SUFFIXES "include" NO_DEFAULT_PATH)
    find_library(OPENLDAP_LDAP_LIBRARY NAMES "ldap${_r_suffix}" PATHS "${OPENLDAP_ROOT_DIR}" PATH_SUFFIXES "lib" NO_DEFAULT_PATH)
    find_library(OPENLDAP_LBER_LIBRARY NAMES "lber" PATHS "${OPENLDAP_ROOT_DIR}" PATH_SUFFIXES "lib" NO_DEFAULT_PATH)
    if(OPENLDAP_USE_STATIC_LIBS)
        find_library(_cyrus_sasl_lib NAMES "sasl2" PATHS "${OPENLDAP_ROOT_DIR}" PATH_SUFFIXES "lib" NO_DEFAULT_PATH)
        find_library(_gssapi_lib NAMES "gssapi" PATHS "${OPENLDAP_ROOT_DIR}" PATH_SUFFIXES "lib" NO_DEFAULT_PATH)
    endif()
else()
    find_path(OPENLDAP_INCLUDE_DIRS NAMES "ldap.h" "lber.h")
    find_library(OPENLDAP_LDAP_LIBRARY NAMES "ldap${_r_suffix}")
    find_library(OPENLDAP_LBER_LIBRARY NAMES "lber")
    if(OPENLDAP_USE_STATIC_LIBS)
        find_library(_cyrus_sasl_lib NAMES "sasl2")
        find_library(_gssapi_lib NAMES "gssapi")
    endif()
endif()

unset(_r_suffix)

set(OPENLDAP_LIBRARIES ${OPENLDAP_LDAP_LIBRARY} ${OPENLDAP_LBER_LIBRARY})

if(_cyrus_sasl_lib)
    list(APPEND OPENLDAP_LIBRARIES ${_cyrus_sasl_lib})
    unset(_cyrus_sasl_lib)
endif()

if(_gssapi_lib)
    list(APPEND OPENLDAP_LIBRARIES ${_gssapi_lib})
    unset(_gssapi_lib)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    OpenLDAP DEFAULT_MSG
    OPENLDAP_INCLUDE_DIRS OPENLDAP_LDAP_LIBRARY OPENLDAP_LBER_LIBRARY
)

mark_as_advanced(OPENLDAP_INCLUDE_DIRS OPENLDAP_LIBRARIES OPENLDAP_LDAP_LIBRARY OPENLDAP_LBER_LIBRARY)

if(OPENLDAP_USE_STATIC_LIBS)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${_orig_CMAKE_FIND_LIBRARY_SUFFIXES})
    unset(_orig_CMAKE_FIND_LIBRARY_SUFFIXES)
endif()
