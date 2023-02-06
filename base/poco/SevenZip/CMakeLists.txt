set(LIBNAME "SevenZip")
set(POCO_LIBNAME "Poco${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( SRCS ${HDRS_G})

# TODO: Currently only bundled is supported, in future this should also be possible
# with an unbundled version of 7z
POCO_SOURCES( SRCS 7z
    src/7zAlloc.c
    src/7zBuf2.c
    src/7zBuf.c
    src/7zCrc.c
    src/7zCrcOpt.c
    src/7zDec.c
    src/7zFile.c
    src/7zIn.c
    src/7zStream.c
    src/Alloc.c
    src/Bcj2.c
    src/Bra86.c
    src/Bra.c
#    src/BraIA64.c
    src/CpuArch.c
#    src/Delta.c
#    src/LzFind.c
#    src/LzFindMt.c
    src/Lzma2Dec.c
#    src/Lzma2Enc.c
    src/Lzma86Dec.c
#    src/Lzma86Enc.c
    src/LzmaDec.c
#    src/LzmaEnc.c
#    src/LzmaLib.c
#    src/MtCoder.c
    src/Ppmd7.c
    src/Ppmd7Dec.c
#    src/Ppmd7Enc.c
#    src/Sha256.c
#    src/Threads.c
#    src/Xz.c
#    src/XzCrc64.c
#    src/XzDec.c
#    src/XzEnc.c
#    src/XzIn.c
)

# Version Resource
if(MSVC AND NOT POCO_STATIC)
    source_group("Resources" FILES ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
    list(APPEND SRCS ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
endif()

add_library( "${LIBNAME}" ${LIB_MODE} ${SRCS} )
add_library( "${POCO_LIBNAME}" ALIAS "${LIBNAME}")
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL SevenZip_EXPORTS
    )

target_link_libraries( "${LIBNAME}" Util XML Foundation)
target_include_directories( "${LIBNAME}"
    SYSTEM PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
target_compile_definitions("${LIBNAME}" PUBLIC ${LIB_MODE_DEFINITIONS})

POCO_INSTALL("${LIBNAME}")
POCO_GENERATE_PACKAGE("${LIBNAME}")

if (ENABLE_TESTS)
    add_subdirectory(samples)
    # TODO: Add tests
    #add_subdirectory(testsuite)
endif ()

