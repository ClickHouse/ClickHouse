option(ENABLE_GSASL_LIBRARY "Enable gsasl library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_GSASL_LIBRARY)
    message(STATUS "Not using gsasl")
    return()
endif()

set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/libgsasl")

set(SRCS
    ${SRC_DIR}/gl/gc-gnulib.c
    ${SRC_DIR}/gl/printf-parse.c
    ${SRC_DIR}/gl/c-ctype.c
    ${SRC_DIR}/gl/float.c
    ${SRC_DIR}/gl/printf-args.c
    ${SRC_DIR}/gl/hmac-sha1.c
    ${SRC_DIR}/gl/itold.c
    ${SRC_DIR}/gl/hmac-md5.c
    ${SRC_DIR}/gl/gc-pbkdf2-sha1.c
    ${SRC_DIR}/gl/md5.c
    ${SRC_DIR}/gl/base64.c
    ${SRC_DIR}/gl/memxor.c
    ${SRC_DIR}/gl/sha1.c
    ${SRC_DIR}/openid20/client.c
    ${SRC_DIR}/openid20/mechinfo.c
    ${SRC_DIR}/openid20/server.c
    ${SRC_DIR}/anonymous/client.c
    ${SRC_DIR}/anonymous/mechinfo.c
    ${SRC_DIR}/anonymous/server.c
    ${SRC_DIR}/saml20/client.c
    ${SRC_DIR}/saml20/mechinfo.c
    ${SRC_DIR}/saml20/server.c
    ${SRC_DIR}/scram/parser.c
    ${SRC_DIR}/scram/printer.c
    ${SRC_DIR}/scram/tokens.c
    ${SRC_DIR}/scram/client.c
    ${SRC_DIR}/scram/mechinfo.c
    ${SRC_DIR}/scram/server.c
    ${SRC_DIR}/scram/validate.c
    ${SRC_DIR}/src/free.c
    ${SRC_DIR}/src/supportp.c
    ${SRC_DIR}/src/init.c
    ${SRC_DIR}/src/mechtools.c
    ${SRC_DIR}/src/error.c
    ${SRC_DIR}/src/property.c
    ${SRC_DIR}/src/done.c
    ${SRC_DIR}/src/callback.c
    ${SRC_DIR}/src/xstart.c
    ${SRC_DIR}/src/xfinish.c
    ${SRC_DIR}/src/version.c
    ${SRC_DIR}/src/xstep.c
    ${SRC_DIR}/src/mechname.c
    ${SRC_DIR}/src/xcode.c
    ${SRC_DIR}/src/crypto.c
    ${SRC_DIR}/src/doxygen.c
    ${SRC_DIR}/src/suggest.c
    ${SRC_DIR}/src/saslprep.c
    ${SRC_DIR}/src/listmech.c
    ${SRC_DIR}/src/register.c
    ${SRC_DIR}/src/base64.c
    ${SRC_DIR}/src/md5pwd.c
    ${SRC_DIR}/external/client.c
    ${SRC_DIR}/external/mechinfo.c
    ${SRC_DIR}/external/server.c
    ${SRC_DIR}/securid/client.c
    ${SRC_DIR}/securid/mechinfo.c
    ${SRC_DIR}/securid/server.c
    ${SRC_DIR}/plain/client.c
    ${SRC_DIR}/plain/mechinfo.c
    ${SRC_DIR}/plain/server.c
    ${SRC_DIR}/cram-md5/client.c
    ${SRC_DIR}/cram-md5/challenge.c
    ${SRC_DIR}/cram-md5/mechinfo.c
    ${SRC_DIR}/cram-md5/server.c
    ${SRC_DIR}/cram-md5/digest.c
    ${SRC_DIR}/digest-md5/client.c
    ${SRC_DIR}/digest-md5/digesthmac.c
    ${SRC_DIR}/digest-md5/free.c
    ${SRC_DIR}/digest-md5/getsubopt.c
    ${SRC_DIR}/digest-md5/mechinfo.c
    ${SRC_DIR}/digest-md5/nonascii.c
    ${SRC_DIR}/digest-md5/parser.c
    ${SRC_DIR}/digest-md5/printer.c
    ${SRC_DIR}/digest-md5/qop.c
    ${SRC_DIR}/digest-md5/server.c
    ${SRC_DIR}/digest-md5/session.c
    ${SRC_DIR}/digest-md5/test-parser.c
    ${SRC_DIR}/digest-md5/validate.c
    ${SRC_DIR}/login/client.c
    ${SRC_DIR}/login/mechinfo.c
    ${SRC_DIR}/login/server.c
)

if (TARGET ch_contrib::krb5)
    set(SRCS ${SRCS}
        ${SRC_DIR}/gssapi/client.c
        ${SRC_DIR}/gssapi/mechinfo.c
        ${SRC_DIR}/gssapi/server.c)
endif()

add_library(_gsasl ${SRCS})

target_include_directories(_gsasl PUBLIC ${SRC_DIR})
target_include_directories(_gsasl PUBLIC ${SRC_DIR}/gl)
target_include_directories(_gsasl PUBLIC ${SRC_DIR}/src)
target_include_directories(_gsasl PUBLIC ${SRC_DIR}/digest-md5)
target_include_directories(_gsasl PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/libgsasl-cmake/linux_x86_64/include")

target_compile_definitions(_gsasl PRIVATE HAVE_CONFIG_H=1)

if (TARGET ch_contrib::krb5)
    target_link_libraries(_gsasl PUBLIC ch_contrib::krb5)
    target_compile_definitions(_gsasl PRIVATE HAVE_GSSAPI_H=1 USE_GSSAPI=1)
endif()

if (TARGET OpenSSL::SSL)
    target_link_libraries(_gsasl PRIVATE OpenSSL::Crypto OpenSSL::SSL)
endif()

add_library(ch_contrib::gsasl ALIAS _gsasl)
