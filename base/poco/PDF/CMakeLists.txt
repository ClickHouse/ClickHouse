set(LIBNAME "PDF")
set(POCO_LIBNAME "Poco${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( SRCS ${HDRS_G})

# If POCO_UNBUNDLED is enabled we try to find the required packages
# The configuration will fail if the packages are not found
option(POCO_UNBUNDLED_ZLIB
  "Set to OFF|ON (default is OFF) to control linking dependencies as external" ${POCO_UNBUNDLED})
if (POCO_UNBUNDLED_ZLIB)
    find_package(ZLIB)
    set(SYSLIBS ${SYSLIBS} ${ZLIB_LIBRARIES})
    include_directories(${ZLIB_INCLUDE_DIRS})
else()
    # zlib
    POCO_SOURCES( SRCS zlib
        src/adler32.c
        src/compress.c
        src/crc32.c
        src/deflate.c
        src/gzio.c
        src/infback.c
        src/inffast.c
        src/inflate.c
        src/inftrees.c
        src/trees.c
        src/zutil.c
    )
endif (POCO_UNBUNDLED_ZLIB)

# TODO: Currently only bundled is supported, in future this should also be possible
# with an unbundled version of hpdf
# hpdf
POCO_SOURCES(SRCS hpdf
    src/hpdf_3dmeasure.c
    src/hpdf_annotation.c
    src/hpdf_array.c
    src/hpdf_binary.c
    src/hpdf_boolean.c
    src/hpdf_catalog.c
    src/hpdf_destination.c
    src/hpdf_dict.c
    src/hpdf_doc.c
    src/hpdf_doc_png.c
    src/hpdf_encoder.c
    src/hpdf_encoder_cns.c
    src/hpdf_encoder_cnt.c
    src/hpdf_encoder_jp.c
    src/hpdf_encoder_kr.c
    src/hpdf_encoder_utf.c
    src/hpdf_encrypt.c
    src/hpdf_encryptdict.c
    src/hpdf_error.c
    src/hpdf_exdata.c
    src/hpdf_ext_gstate.c
    src/hpdf_font.c
    src/hpdf_font_cid.c
    src/hpdf_font_tt.c
    src/hpdf_font_type1.c
    src/hpdf_fontdef.c
    src/hpdf_fontdef_base14.c
    src/hpdf_fontdef_cid.c
    src/hpdf_fontdef_cns.c
    src/hpdf_fontdef_cnt.c
    src/hpdf_fontdef_jp.c
    src/hpdf_fontdef_kr.c
    src/hpdf_fontdef_tt.c
    src/hpdf_fontdef_type1.c
    src/hpdf_gstate.c
    src/hpdf_image.c
    src/hpdf_image_ccitt.c
    src/hpdf_image_png.c
    src/hpdf_info.c
    src/hpdf_list.c
    src/hpdf_mmgr.c
    src/hpdf_name.c
    src/hpdf_namedict.c
    src/hpdf_null.c
    src/hpdf_number.c
    src/hpdf_objects.c
    src/hpdf_outline.c
    src/hpdf_page_label.c
    src/hpdf_page_operator.c
    src/hpdf_pages.c
    src/hpdf_pdfa.c
    src/hpdf_real.c
    src/hpdf_streams.c
    src/hpdf_string.c
    src/hpdf_u3d.c
    src/hpdf_utils.c
    src/hpdf_xref.c
)

# TODO: Currently only bundled is supported, in future this should also be possible
# with an unbundled version of libpng
POCO_SOURCES( SRCS libpng
    src/png.c
    src/pngerror.c
    src/pnggccrd.c
    src/pngget.c
    src/pngmem.c
    src/pngpread.c
    src/pngread.c
    src/pngrio.c
    src/pngrtran.c
    src/pngrutil.c
    src/pngset.c
    src/pngtest.c
    src/pngtrans.c
    src/pngvcrd.c
    src/pngwio.c
    src/pngwrite.c
    src/pngwtran.c
    src/pngwutil.c
)

# Version Resource
if(MSVC AND NOT POCO_STATIC)
    source_group("Resources" FILES ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
    list(APPEND SRCS ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
endif()

#TODO: Can we put this with the below includes? PRIVAT eg.
include_directories( "include/Poco/PDF" ) # zip src

add_library( "${LIBNAME}" ${LIB_MODE} ${SRCS} )
add_library( "${POCO_LIBNAME}" ALIAS "${LIBNAME}")
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL PDF_EXPORTS
    )

target_link_libraries( "${LIBNAME}" ${SYSLIBS} XML Util Foundation )
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
    add_subdirectory(testsuite)
endif ()

