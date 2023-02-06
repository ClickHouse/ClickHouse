set(LIBNAME "WinTestRunner")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( WIN_SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( WIN_SRCS ${HDRS_G})

file(GLOB_RECURSE HDRS_G "src/*.h" )
POCO_HEADERS_AUTO( WIN_SRCS ${HDRS_G})

# Version Resource
if(MSVC AND NOT POCO_STATIC)
    source_group("Resources" FILES ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
    list(APPEND SRCS ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
endif()

# TODO: Is this flag always required?
add_definitions("-D_AFXDLL")
#TODO: Use instead of the flag above: find_package(MFC)

add_library( "${LIBNAME}" ${LIB_MODE} ${WIN_SRCS} )
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION "1" SOVERSION "1"
    DEFINE_SYMBOL WinTestRunner_EXPORTS)
target_link_libraries( "${LIBNAME}" CppUnit )
target_include_directories( "${LIBNAME}"
    SYSTEM PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
target_compile_definitions("${LIBNAME}" PUBLIC ${LIB_MODE_DEFINITIONS})

if(WIN32)
    target_link_libraries( ${LIBNAME} winmm )
endif(WIN32)


