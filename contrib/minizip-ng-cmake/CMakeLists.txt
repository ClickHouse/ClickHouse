option(ENABLE_MINIZIP "Enable minizip-ng the zip manipulation library" ${ENABLE_LIBRARIES})
if (NOT ENABLE_MINIZIP)
    message (STATUS "Not using minizip-ng")
    return()
endif()

set(_MINIZIP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/minizip-ng")

# Initial source files
set(MINIZIP_SRC
    ${_MINIZIP_SOURCE_DIR}/mz_crypt.c
    ${_MINIZIP_SOURCE_DIR}/mz_os.c
    ${_MINIZIP_SOURCE_DIR}/mz_strm.c
    ${_MINIZIP_SOURCE_DIR}/mz_strm_buf.c
    ${_MINIZIP_SOURCE_DIR}/mz_strm_mem.c
    ${_MINIZIP_SOURCE_DIR}/mz_strm_split.c
    ${_MINIZIP_SOURCE_DIR}/mz_zip.c
    ${_MINIZIP_SOURCE_DIR}/mz_zip_rw.c)

# Initial header files
set(MINIZIP_HDR
    ${_MINIZIP_SOURCE_DIR}/mz.h
    ${_MINIZIP_SOURCE_DIR}/mz_os.h
    ${_MINIZIP_SOURCE_DIR}/mz_crypt.h
    ${_MINIZIP_SOURCE_DIR}/mz_strm.h
    ${_MINIZIP_SOURCE_DIR}/mz_strm_buf.h
    ${_MINIZIP_SOURCE_DIR}/mz_strm_mem.h
    ${_MINIZIP_SOURCE_DIR}/mz_strm_split.h
    ${_MINIZIP_SOURCE_DIR}/mz_strm_os.h
    ${_MINIZIP_SOURCE_DIR}/mz_zip.h
    ${_MINIZIP_SOURCE_DIR}/mz_zip_rw.h)

set(MINIZIP_INC ${_MINIZIP_SOURCE_DIR})

set(MINIZIP_DEF)
set(MINIZIP_PUBLIC_DEF)
set(MINIZIP_LIB)

# Check if zlib is present
set(MZ_ZLIB ON)
if(MZ_ZLIB)
    # Use zlib from ClickHouse contrib
    list(APPEND MINIZIP_LIB ch_contrib::zlib)

    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_strm_zlib.c)

    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_strm_zlib.h)

    list(APPEND MINIZIP_DEF "-DHAVE_ZLIB")
endif()

# Check if bzip2 is present
set(MZ_BZIP2 ${ENABLE_BZIP2})
if(MZ_BZIP2)
    # Use bzip2 from ClickHouse contrib
    list(APPEND MINIZIP_LIB ch_contrib::bzip2)

    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_strm_bzip.c)

    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_strm_bzip.h)

    list(APPEND MINIZIP_DEF "-DHAVE_BZIP2")
endif()

# Check if liblzma is present
set(MZ_LZMA ON)
if(MZ_LZMA)
    # Use liblzma from ClickHouse contrib
    list(APPEND MINIZIP_LIB ch_contrib::xz)

    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_strm_lzma.c)

    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_strm_lzma.h)

    list(APPEND MINIZIP_DEF "-DHAVE_LZMA")
endif()

# Check if zstd is present
set(MZ_ZSTD ON)
if(MZ_ZSTD)
    # Use zstd from ClickHouse contrib
    list(APPEND MINIZIP_LIB ch_contrib::zstd)

    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_strm_zstd.c)

    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_strm_zstd.h)

    list(APPEND MINIZIP_DEF "-DHAVE_ZSTD")
endif()

if(NOT MZ_ZLIB AND NOT MZ_ZSTD AND NOT MZ_BZIP2 AND NOT MZ_LZMA)
    message(STATUS "Compression not supported due to missing libraries")

    list(APPEND MINIZIP_DEF -DMZ_ZIP_NO_DECOMPRESSION)
    list(APPEND MINIZIP_DEF -DMZ_ZIP_NO_COMPRESSION)
endif()

# Check to see if openssl installation is present
set(MZ_OPENSSL ${ENABLE_SSL})
if(MZ_OPENSSL)
    # Use openssl from ClickHouse contrib
    list(APPEND MINIZIP_LIB OpenSSL::SSL OpenSSL::Crypto)

    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_crypt_openssl.c)
endif()

# Include WinZIP AES encryption
set(MZ_WZAES ${ENABLE_SSL})
if(MZ_WZAES)
    list(APPEND MINIZIP_DEF -DHAVE_WZAES)

    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_strm_wzaes.c)
    
    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_strm_wzaes.h)
endif()

# Include traditional PKWare encryption
set(MZ_PKCRYPT ON)
if(MZ_PKCRYPT)
    list(APPEND MINIZIP_DEF -DHAVE_PKCRYPT)
    
    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_strm_pkcrypt.c)
    
    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_strm_pkcrypt.h)
endif()

# Unix specific
if(UNIX)
    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_os_posix.c
         ${_MINIZIP_SOURCE_DIR}/mz_strm_os_posix.c)
endif()

# Include compatibility layer
set(MZ_COMPAT ON)
if(MZ_COMPAT)
    list(APPEND MINIZIP_SRC
         ${_MINIZIP_SOURCE_DIR}/mz_compat.c)
    
    list(APPEND MINIZIP_HDR
         ${_MINIZIP_SOURCE_DIR}/mz_compat.h
         zip.h
         unzip.h)

    list(APPEND MINIZIP_INC "${CMAKE_CURRENT_SOURCE_DIR}")
    list(APPEND MINIZIP_PUBLIC_DEF "-DMZ_COMPAT_VERSION=110")
endif()

add_library(_minizip ${MINIZIP_SRC} ${MINIZIP_HDR})
target_include_directories(_minizip PUBLIC ${MINIZIP_INC})
target_compile_definitions(_minizip PUBLIC ${MINIZIP_PUBLIC_DEF})
target_compile_definitions(_minizip PRIVATE ${MINIZIP_DEF})
target_link_libraries(_minizip PRIVATE ${MINIZIP_LIB})

add_library(ch_contrib::minizip ALIAS _minizip)
