option (ENABLE_SSL "Enable ssl" ON)

if (ENABLE_SSL)

set (OPENSSL_USE_STATIC_LIBS ${USE_STATIC_LIBRARIES})

    if (APPLE)
        set (OPENSSL_ROOT_DIR "/usr/local/opt/openssl" CACHE INTERNAL "")
        # https://rt.openssl.org/Ticket/Display.html?user=guest&pass=guest&id=2232
        if (USE_STATIC_LIBRARIES)
            message(WARNING "Disable USE_STATIC_LIBRARIES if you have linking problems with OpenSSL on MacOS")
        endif ()
    endif ()
    find_package (OpenSSL)

    if (NOT OPENSSL_FOUND)
        # Try to find manually.
        set (OPENSSL_INCLUDE_PATHS "/usr/local/opt/openssl/include")
        set (OPENSSL_PATHS "/usr/local/opt/openssl/lib")
        find_path (OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h PATHS ${OPENSSL_INCLUDE_PATHS})
        find_library (OPENSSL_SSL_LIBRARY ssl PATHS ${OPENSSL_PATHS})
        find_library (OPENSSL_CRYPTO_LIBRARY crypto PATHS ${OPENSSL_PATHS})
        if (OPENSSL_SSL_LIBRARY AND OPENSSL_CRYPTO_LIBRARY AND OPENSSL_INCLUDE_DIR)
            set (OPENSSL_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
            set (OPENSSL_FOUND 1)
        endif ()
    endif ()

if(OPENSSL_FOUND)
    # we need keep OPENSSL_FOUND for many libs in contrib
    set(USE_SSL 1)
endif()

endif ()

message (STATUS "Using ssl=${USE_SSL}: ${OPENSSL_INCLUDE_DIR} : ${OPENSSL_LIBRARIES}")
