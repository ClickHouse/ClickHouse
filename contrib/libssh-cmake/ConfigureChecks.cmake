include(CheckIncludeFile)
include(CheckIncludeFiles)
include(CheckSymbolExists)
include(CheckFunctionExists)
include(CheckTypeSize)
include(CheckStructHasMember)
include(TestBigEndian)

set(PACKAGE ${PROJECT_NAME})
set(VERSION ${PROJECT_VERSION})
set(SYSCONFDIR ${CMAKE_INSTALL_SYSCONFDIR})

set(BINARYDIR ${CMAKE_BINARY_DIR})
set(SOURCEDIR ${CMAKE_SOURCE_DIR})

check_include_file(pty.h HAVE_PTY_H)
check_include_file(utmp.h HAVE_UTMP_H)
check_include_file(termios.h HAVE_TERMIOS_H)
check_include_file(unistd.h HAVE_UNISTD_H)
check_include_file(stdint.h HAVE_STDINT_H)
check_include_file(util.h HAVE_UTIL_H)
check_include_file(libutil.h HAVE_LIBUTIL_H)
check_include_file(sys/time.h HAVE_SYS_TIME_H)
check_include_file(sys/utime.h HAVE_SYS_UTIME_H)
check_include_file(sys/param.h HAVE_SYS_PARAM_H)
check_include_file(arpa/inet.h HAVE_ARPA_INET_H)
check_include_file(byteswap.h HAVE_BYTESWAP_H)
check_include_file(glob.h HAVE_GLOB_H)
check_include_file(valgrind/valgrind.h HAVE_VALGRIND_VALGRIND_H)

set(HAVE_OPENSSL_DES_H TRUE)

set(HAVE_OPENSSL_AES_H TRUE)

if (WITH_BLOWFISH_CIPHER)
    set(HAVE_OPENSSL_BLOWFISH_H TRUE)
endif()

set(HAVE_OPENSSL_ECDH_H TRUE)

set(HAVE_OPENSSL_EC_H TRUE)

set(HAVE_OPENSSL_ECDSA_H TRUE)

set(HAVE_OPENSSL_EVP_AES_CTR TRUE)

set(HAVE_OPENSSL_EVP_AES_CBC TRUE)

set(HAVE_OPENSSL_EVP_AES_GCM FALSE)

set(HAVE_OPENSSL_CRYPTO_THREADID_SET_CALLBACK TRUE)

set(HAVE_OPENSSL_CRYPTO_CTR128_ENCRYPT TRUE)

set(HAVE_OPENSSL_EVP_CIPHER_CTX_NEW TRUE)

set(HAVE_OPENSSL_EVP_KDF_CTX_NEW_ID FALSE)

set(HAVE_OPENSSL_FIPS_MODE TRUE)

set(HAVE_OPENSSL_RAND_PRIV_BYTES FALSE)

set(HAVE_OPENSSL_EVP_DIGESTSIGN TRUE)

set(HAVE_OPENSSL_EVP_DIGESTVERIFY TRUE)

set(HAVE_OPENSSL_IA32CAP_LOC FALSE)

if (HAVE_OPENSSL_EVP_DIGESTSIGN AND HAVE_OPENSSL_EVP_DIGESTVERIFY)
    set(HAVE_OPENSSL_ED25519 TRUE)
endif()

set(HAVE_OPENSSL_X25519 TRUE)

set(HAVE_PTHREAD_H TRUE)

if (HAVE_OPENSSL_EC_H AND HAVE_OPENSSL_ECDSA_H)
        set(HAVE_OPENSSL_ECC 1)
endif (HAVE_OPENSSL_EC_H AND HAVE_OPENSSL_ECDSA_H)

if (HAVE_OPENSSL_ECC)
        set(HAVE_ECC 1)
endif (HAVE_OPENSSL_ECC)

set(HAVE_DSA FALSE)

# FUNCTIONS

check_function_exists(isblank HAVE_ISBLANK)
check_function_exists(strncpy HAVE_STRNCPY)
check_function_exists(strndup HAVE_STRNDUP)
check_function_exists(strtoull HAVE_STRTOULL)
check_function_exists(explicit_bzero HAVE_EXPLICIT_BZERO)
check_function_exists(memset_s HAVE_MEMSET_S)

if (HAVE_GLOB_H)
    check_struct_has_member(glob_t gl_flags glob.h HAVE_GLOB_GL_FLAGS_MEMBER)
    check_function_exists(glob HAVE_GLOB)
endif (HAVE_GLOB_H)

if (NOT WIN32)
  check_function_exists(vsnprintf HAVE_VSNPRINTF)
  check_function_exists(snprintf HAVE_SNPRINTF)
  check_function_exists(poll HAVE_POLL)
  check_function_exists(select HAVE_SELECT)
  check_function_exists(getaddrinfo HAVE_GETADDRINFO)

  check_symbol_exists(ntohll arpa/inet.h HAVE_NTOHLL)
  check_symbol_exists(htonll arpa/inet.h HAVE_HTONLL)
endif (NOT WIN32)

# LIBRARIES
set(HAVE_LIBCRYPTO 1)

if (CMAKE_USE_PTHREADS_INIT)
    set(HAVE_PTHREAD 1)
endif (CMAKE_USE_PTHREADS_INIT)

# OPTIONS
check_c_source_compiles("
__thread int tls;

int main(void) {
    return 0;
}" HAVE_GCC_THREAD_LOCAL_STORAGE)

check_c_source_compiles("
__declspec(thread) int tls;

int main(void) {
    return 0;
}" HAVE_MSC_THREAD_LOCAL_STORAGE)

###########################################################
# For detecting attributes we need to treat warnings as
# errors
if (UNIX OR MINGW)
    # Get warnings for attributs
    check_c_compiler_flag("-Wattributes" REQUIRED_FLAGS_WERROR)
    if (REQUIRED_FLAGS_WERROR)
        string(APPEND CMAKE_REQUIRED_FLAGS "-Wattributes ")
    endif()

    # Turn warnings into errors
    check_c_compiler_flag("-Werror" REQUIRED_FLAGS_WERROR)
    if (REQUIRED_FLAGS_WERROR)
        string(APPEND CMAKE_REQUIRED_FLAGS "-Werror ")
    endif()
endif ()

check_c_source_compiles("
void test_constructor_attribute(void) __attribute__ ((constructor));

void test_constructor_attribute(void)
{
    return;
}

int main(void) {
    return 0;
}" HAVE_CONSTRUCTOR_ATTRIBUTE)

check_c_source_compiles("
void test_destructor_attribute(void) __attribute__ ((destructor));

void test_destructor_attribute(void)
{
    return;
}

int main(void) {
    return 0;
}" HAVE_DESTRUCTOR_ATTRIBUTE)

check_c_source_compiles("
#define FALL_THROUGH __attribute__((fallthrough))

int main(void) {
    int i = 2;

    switch (i) {
    case 0:
        FALL_THROUGH;
    case 1:
        break;
    default:
        break;
    }

    return 0;
}" HAVE_FALLTHROUGH_ATTRIBUTE)

if (NOT WIN32)
    check_c_source_compiles("
    #define __unused __attribute__((unused))

    static int do_nothing(int i __unused)
    {
        return 0;
    }

    int main(void)
    {
        int i;

        i = do_nothing(5);
        if (i > 5) {
            return 1;
        }

        return 0;
    }" HAVE_UNUSED_ATTRIBUTE)
endif()

check_c_source_compiles("
#include <string.h>

int main(void)
{
    char buf[] = \"This is some content\";

    memset(buf, '\\\\0', sizeof(buf)); __asm__ volatile(\"\" : : \"g\"(&buf) : \"memory\");

    return 0;
}" HAVE_GCC_VOLATILE_MEMORY_PROTECTION)

check_c_source_compiles("
#include <stdio.h>
int main(void) {
    printf(\"%s\", __func__);
    return 0;
}" HAVE_COMPILER__FUNC__)

check_c_source_compiles("
#include <stdio.h>
int main(void) {
    printf(\"%s\", __FUNCTION__);
    return 0;
}" HAVE_COMPILER__FUNCTION__)

# This is only available with OpenBSD's gcc implementation */
if (OPENBSD)
check_c_source_compiles("
#define ARRAY_LEN 16
void test_attr(const unsigned char *k)
    __attribute__((__bounded__(__minbytes__, 2, 16)));

int main(void) {
    return 0;
}" HAVE_GCC_BOUNDED_ATTRIBUTE)
endif(OPENBSD)

# Stop treating warnings as errors
unset(CMAKE_REQUIRED_FLAGS)

# Check for version script support
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/conftest.map" "VERS_1 {
        global: sym;
};
VERS_2 {
        global: sym;
} VERS_1;
")

set(CMAKE_REQUIRED_FLAGS "-Wl,--version-script=\"${CMAKE_CURRENT_BINARY_DIR}/conftest.map\"")
check_c_source_compiles("int main(void) { return 0; }" HAVE_LD_VERSION_SCRIPT)
unset(CMAKE_REQUIRED_FLAGS)
file(REMOVE "${CMAKE_CURRENT_BINARY_DIR}/conftest.map")

if (WITH_DEBUG_CRYPTO)
  set(DEBUG_CRYPTO 1)
endif (WITH_DEBUG_CRYPTO)

if (WITH_DEBUG_PACKET)
  set(DEBUG_PACKET 1)
endif (WITH_DEBUG_PACKET)

if (WITH_DEBUG_CALLTRACE)
  set(DEBUG_CALLTRACE 1)
endif (WITH_DEBUG_CALLTRACE)

if (WITH_GSSAPI AND NOT GSSAPI_FOUND)
    set(WITH_GSSAPI 0)
endif (WITH_GSSAPI AND NOT GSSAPI_FOUND)

# ENDIAN
if (NOT WIN32)
    test_big_endian(WORDS_BIGENDIAN)
endif (NOT WIN32)
