/* config.h.  Generated from build/cmake/config.h.in by cmake configure */
#define __LIBARCHIVE_CONFIG_H_INCLUDED 1

/*
 * Ensure we have C99-style int64_t, etc, all defined.
 */

/* First, we need to know if the system has already defined them. */
#define HAVE_INT16_T
#define HAVE_INT32_T
#define HAVE_INT64_T
#define HAVE_INTMAX_T

#define HAVE_UINT8_T
#define HAVE_UINT16_T
#define HAVE_UINT32_T
#define HAVE_UINT64_T
#define HAVE_UINTMAX_T

/* We might have the types we want under other spellings. */
/* #undef HAVE___INT64 */
/* #undef HAVE_U_INT64_T */
/* #undef HAVE_UNSIGNED___INT64 */

/* The sizes of various standard integer types. */
#define SIZEOF_SHORT 2
#define SIZEOF_INT 4
#define SIZEOF_LONG 8
#define SIZEOF_LONG_LONG 8
#define SIZEOF_UNSIGNED_SHORT 2
#define SIZEOF_UNSIGNED 4
#define SIZEOF_UNSIGNED_LONG 8
#define SIZEOF_UNSIGNED_LONG_LONG 8

/*
 * If we lack int64_t, define it to the first of __int64, int, long, and long long
 * that exists and is the right size.
 */
#if !defined(HAVE_INT64_T) && defined(HAVE___INT64)
typedef __int64 int64_t;
#define HAVE_INT64_T
#endif

#if !defined(HAVE_INT64_T) && SIZEOF_INT == 8
typedef int int64_t;
#define HAVE_INT64_T
#endif

#if !defined(HAVE_INT64_T) && SIZEOF_LONG == 8
typedef long int64_t;
#define HAVE_INT64_T
#endif

#if !defined(HAVE_INT64_T) && SIZEOF_LONG_LONG == 8
typedef long long int64_t;
#define HAVE_INT64_T
#endif

#if !defined(HAVE_INT64_T)
#error No 64-bit integer type was found.
#endif

/*
 * Similarly for int32_t
 */
#if !defined(HAVE_INT32_T) && SIZEOF_INT == 4
typedef int int32_t;
#define HAVE_INT32_T
#endif

#if !defined(HAVE_INT32_T) && SIZEOF_LONG == 4
typedef long int32_t;
#define HAVE_INT32_T
#endif

#if !defined(HAVE_INT32_T)
#error No 32-bit integer type was found.
#endif

/*
 * Similarly for int16_t
 */
#if !defined(HAVE_INT16_T) && SIZEOF_INT == 2
typedef int int16_t;
#define HAVE_INT16_T
#endif

#if !defined(HAVE_INT16_T) && SIZEOF_SHORT == 2
typedef short int16_t;
#define HAVE_INT16_T
#endif

#if !defined(HAVE_INT16_T)
#error No 16-bit integer type was found.
#endif

/*
 * Similarly for uint64_t
 */
#if !defined(HAVE_UINT64_T) && defined(HAVE_UNSIGNED___INT64)
typedef unsigned __int64 uint64_t;
#define HAVE_UINT64_T
#endif

#if !defined(HAVE_UINT64_T) && SIZEOF_UNSIGNED == 8
typedef unsigned uint64_t;
#define HAVE_UINT64_T
#endif

#if !defined(HAVE_UINT64_T) && SIZEOF_UNSIGNED_LONG == 8
typedef unsigned long uint64_t;
#define HAVE_UINT64_T
#endif

#if !defined(HAVE_UINT64_T) && SIZEOF_UNSIGNED_LONG_LONG == 8
typedef unsigned long long uint64_t;
#define HAVE_UINT64_T
#endif

#if !defined(HAVE_UINT64_T)
#error No 64-bit unsigned integer type was found.
#endif


/*
 * Similarly for uint32_t
 */
#if !defined(HAVE_UINT32_T) && SIZEOF_UNSIGNED == 4
typedef unsigned uint32_t;
#define HAVE_UINT32_T
#endif

#if !defined(HAVE_UINT32_T) && SIZEOF_UNSIGNED_LONG == 4
typedef unsigned long uint32_t;
#define HAVE_UINT32_T
#endif

#if !defined(HAVE_UINT32_T)
#error No 32-bit unsigned integer type was found.
#endif

/*
 * Similarly for uint16_t
 */
#if !defined(HAVE_UINT16_T) && SIZEOF_UNSIGNED == 2
typedef unsigned uint16_t;
#define HAVE_UINT16_T
#endif

#if !defined(HAVE_UINT16_T) && SIZEOF_UNSIGNED_SHORT == 2
typedef unsigned short uint16_t;
#define HAVE_UINT16_T
#endif

#if !defined(HAVE_UINT16_T)
#error No 16-bit unsigned integer type was found.
#endif

/*
 * Similarly for uint8_t
 */
#if !defined(HAVE_UINT8_T)
typedef unsigned char uint8_t;
#define HAVE_UINT8_T
#endif

#if !defined(HAVE_UINT8_T)
#error No 8-bit unsigned integer type was found.
#endif

/* Define intmax_t and uintmax_t if they are not already defined. */
#if !defined(HAVE_INTMAX_T)
typedef int64_t intmax_t;
#endif

#if !defined(HAVE_UINTMAX_T)
typedef uint64_t uintmax_t;
#endif

/* Define ZLIB_WINAPI if zlib was built on Visual Studio. */
/* #undef ZLIB_WINAPI */

/* Darwin ACL support */
/* #undef ARCHIVE_ACL_DARWIN */

/* FreeBSD ACL support */
/* #undef ARCHIVE_ACL_FREEBSD */

/* FreeBSD NFSv4 ACL support */
/* #undef ARCHIVE_ACL_FREEBSD_NFS4 */

/* Linux POSIX.1e ACL support via libacl */
/* #undef ARCHIVE_ACL_LIBACL */

/* Linux NFSv4 ACL support via librichacl */
/* #undef ARCHIVE_ACL_LIBRICHACL */

/* Solaris ACL support */
/* #undef ARCHIVE_ACL_SUNOS */

/* Solaris NFSv4 ACL support */
/* #undef ARCHIVE_ACL_SUNOS_NFS4 */

/* MD5 via ARCHIVE_CRYPTO_MD5_LIBC supported. */
/* #undef ARCHIVE_CRYPTO_MD5_LIBC */

/* MD5 via ARCHIVE_CRYPTO_MD5_LIBSYSTEM supported. */
/* #undef ARCHIVE_CRYPTO_MD5_LIBSYSTEM */

/* MD5 via ARCHIVE_CRYPTO_MD5_MBEDTLS supported. */
/* #undef ARCHIVE_CRYPTO_MD5_MBEDTLS */

/* MD5 via ARCHIVE_CRYPTO_MD5_NETTLE supported. */
/* #undef ARCHIVE_CRYPTO_MD5_NETTLE */

/* MD5 via ARCHIVE_CRYPTO_MD5_OPENSSL supported. */
/* #undef ARCHIVE_CRYPTO_MD5_OPENSSL */

/* MD5 via ARCHIVE_CRYPTO_MD5_WIN supported. */
/* #undef ARCHIVE_CRYPTO_MD5_WIN */

/* RMD160 via ARCHIVE_CRYPTO_RMD160_LIBC supported. */
/* #undef ARCHIVE_CRYPTO_RMD160_LIBC */

/* RMD160 via ARCHIVE_CRYPTO_RMD160_NETTLE supported. */
/* #undef ARCHIVE_CRYPTO_RMD160_NETTLE */

/* RMD160 via ARCHIVE_CRYPTO_RMD160_MBEDTLS supported. */
/* #undef ARCHIVE_CRYPTO_RMD160_MBEDTLS */

/* RMD160 via ARCHIVE_CRYPTO_RMD160_OPENSSL supported. */
/* #undef ARCHIVE_CRYPTO_RMD160_OPENSSL */

/* SHA1 via ARCHIVE_CRYPTO_SHA1_LIBC supported. */
/* #undef ARCHIVE_CRYPTO_SHA1_LIBC */

/* SHA1 via ARCHIVE_CRYPTO_SHA1_LIBSYSTEM supported. */
/* #undef ARCHIVE_CRYPTO_SHA1_LIBSYSTEM */

/* SHA1 via ARCHIVE_CRYPTO_SHA1_MBEDTLS supported. */
/* #undef ARCHIVE_CRYPTO_SHA1_MBEDTLS */

/* SHA1 via ARCHIVE_CRYPTO_SHA1_NETTLE supported. */
/* #undef ARCHIVE_CRYPTO_SHA1_NETTLE */

/* SHA1 via ARCHIVE_CRYPTO_SHA1_OPENSSL supported. */
/* #undef ARCHIVE_CRYPTO_SHA1_OPENSSL */

/* SHA1 via ARCHIVE_CRYPTO_SHA1_WIN supported. */
/* #undef ARCHIVE_CRYPTO_SHA1_WIN */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_LIBC supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_LIBC */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_LIBC2 supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_LIBC2 */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_LIBC3 supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_LIBC3 */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_LIBSYSTEM supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_LIBSYSTEM */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_MBEDTLS supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_MBEDTLS */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_NETTLE supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_NETTLE */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_OPENSSL supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_OPENSSL */

/* SHA256 via ARCHIVE_CRYPTO_SHA256_WIN supported. */
/* #undef ARCHIVE_CRYPTO_SHA256_WIN */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_LIBC supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_LIBC */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_LIBC2 supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_LIBC2 */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_LIBC3 supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_LIBC3 */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_LIBSYSTEM supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_LIBSYSTEM */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_MBEDTLS supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_MBEDTLS */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_NETTLE supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_NETTLE */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_OPENSSL supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_OPENSSL */

/* SHA384 via ARCHIVE_CRYPTO_SHA384_WIN supported. */
/* #undef ARCHIVE_CRYPTO_SHA384_WIN */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_LIBC supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_LIBC */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_LIBC2 supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_LIBC2 */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_LIBC3 supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_LIBC3 */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_LIBSYSTEM supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_LIBSYSTEM */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_MBEDTLS supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_MBEDTLS */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_NETTLE supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_NETTLE */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_OPENSSL supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_OPENSSL */

/* SHA512 via ARCHIVE_CRYPTO_SHA512_WIN supported. */
/* #undef ARCHIVE_CRYPTO_SHA512_WIN */

/* AIX xattr support */
/* #undef ARCHIVE_XATTR_AIX */

/* Darwin xattr support */
/* #undef ARCHIVE_XATTR_DARWIN */

/* FreeBSD xattr support */
/* #undef ARCHIVE_XATTR_FREEBSD */

/* Linux xattr support */
/* #undef ARCHIVE_XATTR_LINUX */

/* Version number of bsdcpio */
#define BSDCPIO_VERSION_STRING "3.7.4"

/* Version number of bsdtar */
#define BSDTAR_VERSION_STRING "3.7.4"

/* Version number of bsdcat */
#define BSDCAT_VERSION_STRING "3.7.4"

/* Version number of bsdunzip */
#define BSDUNZIP_VERSION_STRING "3.7.4"

/* Define to 1 if you have the `acl_create_entry' function. */
/* #undef HAVE_ACL_CREATE_ENTRY */

/* Define to 1 if you have the `acl_get_fd_np' function. */
/* #undef HAVE_ACL_GET_FD_NP */

/* Define to 1 if you have the `acl_get_link' function. */
/* #undef HAVE_ACL_GET_LINK */

/* Define to 1 if you have the `acl_get_link_np' function. */
/* #undef HAVE_ACL_GET_LINK_NP */

/* Define to 1 if you have the `acl_get_perm' function. */
/* #undef HAVE_ACL_GET_PERM */

/* Define to 1 if you have the `acl_get_perm_np' function. */
/* #undef HAVE_ACL_GET_PERM_NP */

/* Define to 1 if you have the `acl_init' function. */
/* #undef HAVE_ACL_INIT */

/* Define to 1 if you have the <acl/libacl.h> header file. */
/* #undef HAVE_ACL_LIBACL_H */

/* Define to 1 if the system has the type `acl_permset_t'. */
/* #undef HAVE_ACL_PERMSET_T */

/* Define to 1 if you have the `acl_set_fd' function. */
/* #undef HAVE_ACL_SET_FD */

/* Define to 1 if you have the `acl_set_fd_np' function. */
/* #undef HAVE_ACL_SET_FD_NP */

/* Define to 1 if you have the `acl_set_file' function. */
/* #undef HAVE_ACL_SET_FILE */

/* Define to 1 if you have the `arc4random_buf' function. */
/* #undef HAVE_ARC4RANDOM_BUF */

/* Define to 1 if you have the <attr/xattr.h> header file. */
/* #undef HAVE_ATTR_XATTR_H */

/* Define to 1 if you have the <bcrypt.h> header file. */
/* #undef HAVE_BCRYPT_H */

/* Define to 1 if you have the <bsdxml.h> header file. */
/* #undef HAVE_BSDXML_H */

/* Define to 1 if you have the <bzlib.h> header file. */
/* #undef HAVE_BZLIB_H */

/* Define to 1 if you have the `chflags' function. */
/* #undef HAVE_CHFLAGS */

/* Define to 1 if you have the `chown' function. */
#define HAVE_CHOWN 1

/* Define to 1 if you have the `chroot' function. */
#define HAVE_CHROOT 1

/* Define to 1 if you have the <copyfile.h> header file. */
/* #undef HAVE_COPYFILE_H */

/* Define to 1 if you have the `ctime_r' function. */
#define HAVE_CTIME_R 1

/* Define to 1 if you have the <ctype.h> header file. */
#define HAVE_CTYPE_H 1

/* Define to 1 if you have the `cygwin_conv_path' function. */
/* #undef HAVE_CYGWIN_CONV_PATH */

/* Define to 1 if you have the declaration of `ACE_GETACL', and to 0 if you
   don't. */
/* #undef HAVE_DECL_ACE_GETACL */

/* Define to 1 if you have the declaration of `ACE_GETACLCNT', and to 0 if you
   don't. */
/* #undef HAVE_DECL_ACE_GETACLCNT */

/* Define to 1 if you have the declaration of `ACE_SETACL', and to 0 if you
   don't. */
/* #undef HAVE_DECL_ACE_SETACL */

/* Define to 1 if you have the declaration of `ACL_SYNCHRONIZE', and to 0 if
   you don't. */
/* #undef HAVE_DECL_ACL_SYNCHRONIZE */

/* Define to 1 if you have the declaration of `ACL_TYPE_EXTENDED', and to 0 if
   you don't. */
/* #undef HAVE_DECL_ACL_TYPE_EXTENDED */

/* Define to 1 if you have the declaration of `ACL_TYPE_NFS4', and to 0 if you
   don't. */
/* #undef HAVE_DECL_ACL_TYPE_NFS4 */

/* Define to 1 if you have the declaration of `ACL_USER', and to 0 if you
   don't. */
/* #undef HAVE_DECL_ACL_USER */

/* Define to 1 if you have the declaration of `INT32_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_INT32_MAX 1

/* Define to 1 if you have the declaration of `INT32_MIN', and to 0 if you
   don't. */
#define HAVE_DECL_INT32_MIN 1

/* Define to 1 if you have the declaration of `INT64_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_INT64_MAX 1

/* Define to 1 if you have the declaration of `INT64_MIN', and to 0 if you
   don't. */
#define HAVE_DECL_INT64_MIN 1

/* Define to 1 if you have the declaration of `INTMAX_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_INTMAX_MAX 1

/* Define to 1 if you have the declaration of `INTMAX_MIN', and to 0 if you
   don't. */
#define HAVE_DECL_INTMAX_MIN 1

/* Define to 1 if you have the declaration of `SETACL', and to 0 if you don't.
   */
/* #undef HAVE_DECL_SETACL */

/* Define to 1 if you have the declaration of `SIZE_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_SIZE_MAX 1

/* Define to 1 if you have the declaration of `SSIZE_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_SSIZE_MAX 1

/* Define to 1 if you have the declaration of `strerror_r', and to 0 if you
   don't. */
#define HAVE_DECL_STRERROR_R 1

/* Define to 1 if you have the declaration of `UINT32_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_UINT32_MAX 1

/* Define to 1 if you have the declaration of `UINT64_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_UINT64_MAX 1

/* Define to 1 if you have the declaration of `UINTMAX_MAX', and to 0 if you
   don't. */
#define HAVE_DECL_UINTMAX_MAX 1

/* Define to 1 if you have the declaration of `XATTR_NOFOLLOW', and to 0 if
   you don't. */
/* #undef HAVE_DECL_XATTR_NOFOLLOW */

/* Define to 1 if you have the <direct.h> header file. */
/* #undef HAVE_DIRECT_H */

/* Define to 1 if you have the <dirent.h> header file, and it defines `DIR'.
   */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the `dirfd' function. */
#define HAVE_DIRFD 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you don't have `vprintf' but do have `_doprnt.' */
/* #undef HAVE_DOPRNT */

/* Define to 1 if nl_langinfo supports D_MD_ORDER */
/* #undef HAVE_D_MD_ORDER */

/* A possible errno value for invalid file format errors */
/* #undef HAVE_EFTYPE */

/* A possible errno value for invalid file format errors */
#define HAVE_EILSEQ 1

/* Define to 1 if you have the <errno.h> header file. */
#define HAVE_ERRNO_H 1

/* Define to 1 if you have the <expat.h> header file. */
/* #undef HAVE_EXPAT_H */

/* Define to 1 if you have the <ext2fs/ext2_fs.h> header file. */
/* #undef HAVE_EXT2FS_EXT2_FS_H */

/* Define to 1 if you have the `extattr_get_file' function. */
/* #undef HAVE_EXTATTR_GET_FILE */

/* Define to 1 if you have the `extattr_list_file' function. */
/* #undef HAVE_EXTATTR_LIST_FILE */

/* Define to 1 if you have the `extattr_set_fd' function. */
/* #undef HAVE_EXTATTR_SET_FD */

/* Define to 1 if you have the `extattr_set_file' function. */
/* #undef HAVE_EXTATTR_SET_FILE */

/* Define to 1 if EXTATTR_NAMESPACE_USER is defined in sys/extattr.h. */
/* #undef HAVE_DECL_EXTATTR_NAMESPACE_USER */

/* Define to 1 if you have the declaration of `GETACL', and to 0 if you don't.
   */
/* #undef HAVE_DECL_GETACL */

/* Define to 1 if you have the declaration of `GETACLCNT', and to 0 if you
   don't. */
/* #undef HAVE_DECL_GETACLCNT */

/* Define to 1 if you have the `fchdir' function. */
#define HAVE_FCHDIR 1

/* Define to 1 if you have the `fchflags' function. */
/* #undef HAVE_FCHFLAGS */

/* Define to 1 if you have the `fchmod' function. */
#define HAVE_FCHMOD 1

/* Define to 1 if you have the `fchown' function. */
#define HAVE_FCHOWN 1

/* Define to 1 if you have the `fcntl' function. */
#define HAVE_FCNTL 1

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the `fdopendir' function. */
#define HAVE_FDOPENDIR 1

/* Define to 1 if you have the `fgetea' function. */
/* #undef HAVE_FGETEA */

/* Define to 1 if you have the `fgetxattr' function. */
/* #undef HAVE_FGETXATTR */

/* Define to 1 if you have the `flistea' function. */
/* #undef HAVE_FLISTEA */

/* Define to 1 if you have the `flistxattr' function. */
#define HAVE_FLISTXATTR 1

/* Define to 1 if you have the `fnmatch' function. */
#define HAVE_FNMATCH 1

/* Define to 1 if you have the <fnmatch.h> header file. */
#define HAVE_FNMATCH_H 1

/* Define to 1 if you have the `fork' function. */
#define HAVE_FORK 1

/* Define to 1 if fseeko (and presumably ftello) exists and is declared. */
#define HAVE_FSEEKO 1

/* Define to 1 if you have the `fsetea' function. */
/* #undef HAVE_FSETEA */

/* Define to 1 if you have the `fsetxattr' function. */
/* #undef HAVE_FSETXATTR */

/* Define to 1 if you have the `fstat' function. */
#define HAVE_FSTAT 1

/* Define to 1 if you have the `fstatat' function. */
#define HAVE_FSTATAT 1

/* Define to 1 if you have the `fstatfs' function. */
#define HAVE_FSTATFS 1

/* Define to 1 if you have the `fstatvfs' function. */
#define HAVE_FSTATVFS 1

/* Define to 1 if you have the `ftruncate' function. */
#define HAVE_FTRUNCATE 1

/* Define to 1 if you have the `futimens' function. */
#define HAVE_FUTIMENS 1

/* Define to 1 if you have the `futimes' function. */
#define HAVE_FUTIMES 1

/* Define to 1 if you have the `futimesat' function. */
/* #undef HAVE_FUTIMESAT */

/* Define to 1 if you have the `getea' function. */
/* #undef HAVE_GETEA */

/* Define to 1 if you have the `geteuid' function. */
#define HAVE_GETEUID 1

/* Define to 1 if you have the `getgrgid_r' function. */
#define HAVE_GETGRGID_R 1

/* Define to 1 if you have the `getgrnam_r' function. */
#define HAVE_GETGRNAM_R 1

/* Define to 1 if you have the `getline' function. */
#define HAVE_GETLINE 1

/* Define to 1 if you have the `getpid' function. */
#define HAVE_GETPID 1

/* Define to 1 if you have the `getpwnam_r' function. */
#define HAVE_GETPWNAM_R 1

/* Define to 1 if you have the `getpwuid_r' function. */
#define HAVE_GETPWUID_R 1

/* Define to 1 if you have the `getvfsbyname' function. */
/* #undef HAVE_GETVFSBYNAME */

/* Define to 1 if you have the `getxattr' function. */
#define HAVE_GETXATTR 1

/* Define to 1 if you have the `gmtime_r' function. */
#define HAVE_GMTIME_R 1

/* Define to 1 if you have the <grp.h> header file. */
#define HAVE_GRP_H 1

/* Define to 1 if you have the `iconv' function. */
/* #undef HAVE_ICONV */

/* Define to 1 if you have the <iconv.h> header file. */
#define HAVE_ICONV_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the <io.h> header file. */
/* #undef HAVE_IO_H */

/* Define to 1 if you have the <langinfo.h> header file. */
#define HAVE_LANGINFO_H 1

/* Define to 1 if you have the `lchflags' function. */
/* #undef HAVE_LCHFLAGS */

/* Define to 1 if you have the `lchmod' function. */
#define HAVE_LCHMOD 1

/* Define to 1 if you have the `lchown' function. */
#define HAVE_LCHOWN 1

/* Define to 1 if you have the `lgetea' function. */
/* #undef HAVE_LGETEA */

/* Define to 1 if you have the `lgetxattr' function. */
#define HAVE_LGETXATTR 1

/* Define to 1 if you have the `acl' library (-lacl). */
/* #undef HAVE_LIBACL */

/* Define to 1 if you have the `attr' library (-lattr). */
/* #undef HAVE_LIBATTR */

/* Define to 1 if you have the `bsdxml' library (-lbsdxml). */
/* #undef HAVE_LIBBSDXML */

/* Define to 1 if you have the `bz2' library (-lbz2). */
/* #undef HAVE_LIBBZ2 */

/* Define to 1 if you have the `b2' library (-lb2). */
/* #undef HAVE_LIBB2 */

/* Define to 1 if you have the <blake2.h> header file. */
/* #undef HAVE_BLAKE2_H */

/* Define to 1 if you have the `charset' library (-lcharset). */
/* #undef HAVE_LIBCHARSET */

/* Define to 1 if you have the `crypto' library (-lcrypto). */
/* #undef HAVE_LIBCRYPTO */

/* Define to 1 if you have the `expat' library (-lexpat). */
/* #undef HAVE_LIBEXPAT */

/* Define to 1 if you have the `gcc' library (-lgcc). */
/* #undef HAVE_LIBGCC */

/* Define to 1 if you have the `iconv' library (-liconv). */
/* #undef HAVE_LIBICONV */

/* Define to 1 if you have the `lz4' library (-llz4). */
/* #undef HAVE_LIBLZ4 */

/* Define to 1 if you have the `lzma' library (-llzma). */
/* #undef HAVE_LIBLZMA */

/* Define to 1 if you have the `lzmadec' library (-llzmadec). */
/* #undef HAVE_LIBLZMADEC */

/* Define to 1 if you have the `lzo2' library (-llzo2). */
/* #undef HAVE_LIBLZO2 */

/* Define to 1 if you have the `mbedcrypto' library (-lmbedcrypto). */
/* #undef HAVE_LIBMBEDCRYPTO */

/* Define to 1 if you have the `nettle' library (-lnettle). */
/* #undef HAVE_LIBNETTLE */

/* Define to 1 if you have the `pcre' library (-lpcre). */
/* #undef HAVE_LIBPCRE */

/* Define to 1 if you have the `pcreposix' library (-lpcreposix). */
/* #undef HAVE_LIBPCREPOSIX */

/* Define to 1 if you have the `pcre2-8' library (-lpcre2-8). */
/* #undef HAVE_LIBPCRE2 */

/* Define to 1 if you have the `pcreposix' library (-lpcre2posix). */
/* #undef HAVE_LIBPCRE2POSIX */

/* Define to 1 if you have the `xml2' library (-lxml2). */
#define HAVE_LIBXML2 1

/* Define to 1 if you have the <libxml/xmlreader.h> header file. */
/* #undef HAVE_LIBXML_XMLREADER_H */

/* Define to 1 if you have the <libxml/xmlwriter.h> header file. */
/* #undef HAVE_LIBXML_XMLWRITER_H */

/* Define to 1 if you have the `z' library (-lz). */
/* #undef HAVE_LIBZ */

/* Define to 1 if you have the `zstd' library (-lzstd). */
/* #undef HAVE_LIBZSTD */

/* Define to 1 if you have the ZSTD_compressStream function. */
/* #undef HAVE_ZSTD_compressStream */

/* Define to 1 if you have the ZSTD_minCLevel function. */
/* #undef HAVE_ZSTD_minCLevel */

/* Define to 1 if you have the <limits.h> header file. */
#define HAVE_LIMITS_H 1

/* Define to 1 if you have the `link' function. */
#define HAVE_LINK 1

/* Define to 1 if you have the `linkat' function. */
#define HAVE_LINKAT 1

/* Define to 1 if you have the <linux/fiemap.h> header file. */
/* #undef HAVE_LINUX_FIEMAP_H */

/* Define to 1 if you have the <linux/fs.h> header file. */
/* #undef HAVE_LINUX_FS_H */

/* Define to 1 if you have the <linux/magic.h> header file. */
/* #undef HAVE_LINUX_MAGIC_H */

/* Define to 1 if you have the <linux/types.h> header file. */
/* #undef HAVE_LINUX_TYPES_H */

/* Define to 1 if you have the `listea' function. */
/* #undef HAVE_LISTEA */

/* Define to 1 if you have the `listxattr' function. */
#define HAVE_LISTXATTR 1

/* Define to 1 if you have the `llistea' function. */
/* #undef HAVE_LLISTEA */

/* Define to 1 if you have the `llistxattr' function. */
#define HAVE_LLISTXATTR 1

/* Define to 1 if you have the <localcharset.h> header file. */
/* #undef HAVE_LOCALCHARSET_H */

/* Define to 1 if you have the `locale_charset' function. */
/* #undef HAVE_LOCALE_CHARSET */

/* Define to 1 if you have the <locale.h> header file. */
#define HAVE_LOCALE_H 1

/* Define to 1 if you have the `localtime_r' function. */
#define HAVE_LOCALTIME_R 1

/* Define to 1 if the system has the type `long long int'. */
/* #undef HAVE_LONG_LONG_INT */

/* Define to 1 if you have the `lsetea' function. */
/* #undef HAVE_LSETEA */

/* Define to 1 if you have the `lsetxattr' function. */
#define HAVE_LSETXATTR 1

/* Define to 1 if you have the `lstat' function. */
#define HAVE_LSTAT 1

/* Define to 1 if `lstat' has the bug that it succeeds when given the
   zero-length file name argument. */
/* #undef HAVE_LSTAT_EMPTY_STRING_BUG */

/* Define to 1 if you have the `lutimes' function. */
#define HAVE_LUTIMES 1

/* Define to 1 if you have the <lz4hc.h> header file. */
/* #undef HAVE_LZ4HC_H */

/* Define to 1 if you have the <lz4.h> header file. */
/* #undef HAVE_LZ4_H */

/* Define to 1 if you have the <lzmadec.h> header file. */
/* #undef HAVE_LZMADEC_H */

/* Define to 1 if you have the <lzma.h> header file. */
/* #undef HAVE_LZMA_H */

/* Define to 1 if you have a working `lzma_stream_encoder_mt' function. */
/* #undef HAVE_LZMA_STREAM_ENCODER_MT */

/* Define to 1 if you have the <lzo/lzo1x.h> header file. */
/* #undef HAVE_LZO_LZO1X_H */

/* Define to 1 if you have the <lzo/lzoconf.h> header file. */
/* #undef HAVE_LZO_LZOCONF_H */

/* Define to 1 if you have the <mbedtls/aes.h> header file. */
/* #undef HAVE_MBEDTLS_AES_H */

/* Define to 1 if you have the <mbedtls/md.h> header file. */
/* #undef HAVE_MBEDTLS_MD_H */

/* Define to 1 if you have the <mbedtls/pkcs5.h> header file. */
/* #undef HAVE_MBEDTLS_PKCS5_H */

/* Define to 1 if you have the <mbedtls/pkcs5.h> header file. */
/* #undef HAVE_MBEDTLS_VERSION_H */

/* Define to 1 if you have the `mbrtowc' function. */
/* #undef HAVE_MBRTOWC */

/* Define to 1 if you have the <membership.h> header file. */
/* #undef HAVE_MEMBERSHIP_H */

/* Define to 1 if you have the `memmove' function. */
#define HAVE_MEMMOVE 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mkdir' function. */
#define HAVE_MKDIR 1

/* Define to 1 if you have the `mkfifo' function. */
#define HAVE_MKFIFO 1

/* Define to 1 if you have the `mknod' function. */
#define HAVE_MKNOD 1

/* Define to 1 if you have the `mkstemp' function. */
#define HAVE_MKSTEMP 1

/* Define to 1 if you have the <ndir.h> header file, and it defines `DIR'. */
/* #undef HAVE_NDIR_H */

/* Define to 1 if you have the <nettle/aes.h> header file. */
/* #undef HAVE_NETTLE_AES_H */

/* Define to 1 if you have the <nettle/hmac.h> header file. */
/* #undef HAVE_NETTLE_HMAC_H */

/* Define to 1 if you have the <nettle/md5.h> header file. */
/* #undef HAVE_NETTLE_MD5_H */

/* Define to 1 if you have the <nettle/pbkdf2.h> header file. */
/* #undef HAVE_NETTLE_PBKDF2_H */

/* Define to 1 if you have the <nettle/ripemd160.h> header file. */
/* #undef HAVE_NETTLE_RIPEMD160_H */

/* Define to 1 if you have the <nettle/sha.h> header file. */
/* #undef HAVE_NETTLE_SHA_H */

/* Define to 1 if you have the <nettle/version.h> header file. */
/* #undef HAVE_NETTLE_VERSION_H */

/* Define to 1 if you have the `nl_langinfo' function. */
/* #undef HAVE_NL_LANGINFO */

/* Define to 1 if you have the `openat' function. */
#define HAVE_OPENAT 1

/* Define to 1 if you have the <openssl/evp.h> header file. */
/* #undef HAVE_OPENSSL_EVP_H */

/* Define to 1 if you have the <openssl/opensslv.h> header file. */
/* undef HAVE_OPENSSL_OPENSSLV_H */

/* Define to 1 if you have the <paths.h> header file. */
#define HAVE_PATHS_H 1

/* Define to 1 if you have the <pcreposix.h> header file. */
/* #undef HAVE_PCREPOSIX_H */

/* Define to 1 if you have the <pcre2posix.h> header file. */
/* #undef HAVE_PCRE2POSIX_H */

/* Define to 1 if you have the `pipe' function. */
#define HAVE_PIPE 1

/* Define to 1 if you have the `PKCS5_PBKDF2_HMAC_SHA1' function. */
/* #undef HAVE_PKCS5_PBKDF2_HMAC_SHA1 */

/* Define to 1 if you have the `poll' function. */
#define HAVE_POLL 1

/* Define to 1 if you have the <poll.h> header file. */
#define HAVE_POLL_H 1

/* Define to 1 if you have the `posix_spawnp' function. */
#define HAVE_POSIX_SPAWNP 1

/* Define to 1 if you have the <process.h> header file. */
/* #undef HAVE_PROCESS_H */

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define to 1 if you have the `readdir_r' function. */
#define HAVE_READDIR_R 1

/* Define to 1 if you have the `readlink' function. */
#define HAVE_READLINK 1

/* Define to 1 if you have the `readlinkat' function. */
#define HAVE_READLINKAT 1

/* Define to 1 if you have the `readpassphrase' function. */
/* #undef HAVE_READPASSPHRASE */

/* Define to 1 if you have the <readpassphrase.h> header file. */
/* #undef HAVE_READPASSPHRASE_H */

/* Define to 1 if you have the <regex.h> header file. */
#define HAVE_REGEX_H 1

/* Define to 1 if you have the `select' function. */
#define HAVE_SELECT 1

/* Define to 1 if you have the `setenv' function. */
#define HAVE_SETENV 1

/* Define to 1 if you have the `setlocale' function. */
#define HAVE_SETLOCALE 1

/* Define to 1 if you have the `sigaction' function. */
#define HAVE_SIGACTION 1

/* Define to 1 if you have the <signal.h> header file. */
#define HAVE_SIGNAL_H 1

/* Define to 1 if you have the <spawn.h> header file. */
#define HAVE_SPAWN_H 1

/* Define to 1 if you have the `statfs' function. */
#define HAVE_STATFS 1

/* Define to 1 if you have the `statvfs' function. */
#define HAVE_STATVFS 1

/* Define to 1 if `stat' has the bug that it succeeds when given the
   zero-length file name argument. */
/* #undef HAVE_STAT_EMPTY_STRING_BUG */

/* Define to 1 if you have the <stdarg.h> header file. */
#define HAVE_STDARG_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strchr' function. */
#define HAVE_STRCHR 1

/* Define to 1 if you have the `strnlen' function. */
#define HAVE_STRNLEN 1

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the `strerror' function. */
#define HAVE_STRERROR 1

/* Define to 1 if you have the `strerror_r' function. */
#define HAVE_STRERROR_R 1

/* Define to 1 if you have the `strftime' function. */
#define HAVE_STRFTIME 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strrchr' function. */
#define HAVE_STRRCHR 1

/* Define to 1 if the system has the type `struct statfs'. */
/* #undef HAVE_STRUCT_STATFS */

/* Define to 1 if `f_iosize' is a member of `struct statfs'. */
/* #undef HAVE_STRUCT_STATFS_F_IOSIZE */

/* Define to 1 if `f_namemax' is a member of `struct statfs'. */
/* #undef HAVE_STRUCT_STATFS_F_NAMEMAX */

/* Define to 1 if `f_iosize' is a member of `struct statvfs'. */
/* #undef HAVE_STRUCT_STATVFS_F_IOSIZE */

/* Define to 1 if `st_birthtime' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_BIRTHTIME */

/* Define to 1 if `st_birthtimespec.tv_nsec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_BIRTHTIMESPEC_TV_NSEC */

/* Define to 1 if `st_blksize' is a member of `struct stat'. */
#define HAVE_STRUCT_STAT_ST_BLKSIZE 1

/* Define to 1 if `st_flags' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_FLAGS */

/* Define to 1 if `st_mtimespec.tv_nsec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC */

/* Define to 1 if `st_mtime_n' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_MTIME_N */

/* Define to 1 if `st_mtime_usec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_MTIME_USEC */

/* Define to 1 if `st_mtim.tv_nsec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC */

/* Define to 1 if `st_umtime' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_UMTIME */

/* Define to 1 if `tm_gmtoff' is a member of `struct tm'. */
#define HAVE_STRUCT_TM_TM_GMTOFF 1

/* Define to 1 if `__tm_gmtoff' is a member of `struct tm'. */
/* #undef HAVE_STRUCT_TM___TM_GMTOFF */

/* Define to 1 if you have `struct vfsconf'. */
/* #undef HAVE_STRUCT_VFSCONF */

/* Define to 1 if you have `struct xvfsconf'. */
/* #undef HAVE_STRUCT_XVFSCONF */

/* Define to 1 if you have the `symlink' function. */
#define HAVE_SYMLINK 1

/* Define to 1 if you have the `sysconf' function. */
#define HAVE_SYSCONF 1

/* Define to 1 if you have the <sys/acl.h> header file. */
/* #undef HAVE_SYS_ACL_H */

/* Define to 1 if you have the <sys/cdefs.h> header file. */
/* #undef HAVE_SYS_CDEFS_H */

/* Define to 1 if you have the <sys/dir.h> header file, and it defines `DIR'.
   */
/* #undef HAVE_SYS_DIR_H */

/* Define to 1 if you have the <sys/ea.h> header file. */
/* #undef HAVE_SYS_EA_H */

/* Define to 1 if you have the <sys/extattr.h> header file. */
/* #undef HAVE_SYS_EXTATTR_H */

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/mkdev.h> header file. */
/* #undef HAVE_SYS_MKDEV_H */

/* Define to 1 if you have the <sys/mount.h> header file. */
/* #undef HAVE_SYS_MOUNT_H */

/* Define to 1 if you have the <sys/ndir.h> header file, and it defines `DIR'.
   */
/* #undef HAVE_SYS_NDIR_H */

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/poll.h> header file. */
#define HAVE_SYS_POLL_H 1

/* Define to 1 if you have the <sys/queue.h> header file. */
#define HAVE_SYS_QUEUE_H 1

/* Define to 1 if you have the <sys/richacl.h> header file. */
/* #undef HAVE_SYS_RICHACL_H */

/* Define to 1 if you have the <sys/select.h> header file. */
#define HAVE_SYS_SELECT_H 1

/* Define to 1 if you have the <sys/statfs.h> header file. */
/* #undef HAVE_SYS_STATFS_H */

/* Define to 1 if you have the <sys/statvfs.h> header file. */
#define HAVE_SYS_STATVFS_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1


/* Define to 1 if you have the <sys/sysmacros.h> header file. */
#define HAVE_SYS_SYSMACROS_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/utime.h> header file. */
/* #undef HAVE_SYS_UTIME_H */

/* Define to 1 if you have the <sys/utsname.h> header file. */
#define HAVE_SYS_UTSNAME_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#define HAVE_SYS_VFS_H 1

/* Define to 1 if you have <sys/wait.h> that is POSIX.1 compatible. */
#define HAVE_SYS_WAIT_H 1

/* Define to 1 if you have the <sys/xattr.h> header file. */
/* #undef HAVE_SYS_XATTR_H */

/* Define to 1 if you have the `tcgetattr' function. */
/* #undef HAVE_TCGETATTR */

/* Define to 1 if you have the `tcsetattr' function. */
/* #undef HAVE_TCSETATTR */

/* Define to 1 if you have the `timegm' function. */
#define HAVE_TIMEGM 1

/* Define to 1 if you have the <time.h> header file. */
#define HAVE_TIME_H 1

/* Define to 1 if you have the `tzset' function. */
#define HAVE_TZSET 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the `unlinkat' function. */
#define HAVE_UNLINKAT 1

/* Define to 1 if you have the `unsetenv' function. */
#define HAVE_UNSETENV 1

/* Define to 1 if the system has the type `unsigned long long'. */
/* #undef HAVE_UNSIGNED_LONG_LONG */

/* Define to 1 if the system has the type `unsigned long long int'. */
/* #undef HAVE_UNSIGNED_LONG_LONG_INT */

/* Define to 1 if you have the `utime' function. */
#define HAVE_UTIME 1

/* Define to 1 if you have the `utimensat' function. */
#define HAVE_UTIMENSAT 1

/* Define to 1 if you have the `utimes' function. */
#define HAVE_UTIMES 1

/* Define to 1 if you have the <utime.h> header file. */
#define HAVE_UTIME_H 1

/* Define to 1 if you have the `vfork' function. */
#define HAVE_VFORK 1

/* Define to 1 if you have the `vprintf' function. */
#define HAVE_VPRINTF 1

/* Define to 1 if you have the <wchar.h> header file. */
#define HAVE_WCHAR_H 1

/* Define to 1 if the system has the type `wchar_t'. */
#define HAVE_WCHAR_T 1

/* Define to 1 if you have the `wcrtomb' function. */
#define HAVE_WCRTOMB 1

/* Define to 1 if you have the `wcscmp' function. */
#define HAVE_WCSCMP 1

/* Define to 1 if you have the `wcscpy' function. */
#define HAVE_WCSCPY 1

/* Define to 1 if you have the `wcslen' function. */
#define HAVE_WCSLEN 1

/* Define to 1 if you have the `wctomb' function. */
#define HAVE_WCTOMB 1

/* Define to 1 if you have the <wctype.h> header file. */
#define HAVE_WCTYPE_H 1

/* Define to 1 if you have the <wincrypt.h> header file. */
/* #undef HAVE_WINCRYPT_H */

/* Define to 1 if you have the <windows.h> header file. */
/* #undef HAVE_WINDOWS_H */

/* Define to 1 if you have the <winioctl.h> header file. */
/* #undef HAVE_WINIOCTL_H */

/* Define to 1 if you have _CrtSetReportMode in <crtdbg.h>  */
/* #undef HAVE__CrtSetReportMode */

/* Define to 1 if you have the `wmemcmp' function. */
#define HAVE_WMEMCMP 1

/* Define to 1 if you have the `wmemcpy' function. */
#define HAVE_WMEMCPY 1

/* Define to 1 if you have the `wmemmove' function. */
#define HAVE_WMEMMOVE 1

/* Define to 1 if you have a working EXT2_IOC_GETFLAGS */
/* #undef HAVE_WORKING_EXT2_IOC_GETFLAGS */

/* Define to 1 if you have a working FS_IOC_GETFLAGS */
#define HAVE_WORKING_FS_IOC_GETFLAGS 1

/* Define to 1 if you have the Windows `xmllite' library (-lxmllite). */
/* #undef HAVE_XMLLITE_H */

/* Define to 1 if you have the <zlib.h> header file. */
/* #undef HAVE_ZLIB_H */

/* Define to 1 if you have the <zstd.h> header file. */
/* #undef HAVE_ZSTD_H */

/* Define to 1 if you have the `ctime_s' function. */
/* #undef HAVE_CTIME_S */

/* Define to 1 if you have the `_fseeki64' function. */
/* #undef HAVE__FSEEKI64 */

/* Define to 1 if you have the `_get_timezone' function. */
/* #undef HAVE__GET_TIMEZONE */

/* Define to 1 if you have the `gmtime_s' function. */
/* #undef HAVE_GMTIME_S */

/* Define to 1 if you have the `localtime_s' function. */
/* #undef HAVE_LOCALTIME_S */

/* Define to 1 if you have the `_mkgmtime' function. */
/* #undef HAVE__MKGMTIME */

/* Define as const if the declaration of iconv() needs const. */
#define ICONV_CONST

/* Version number of libarchive as a single integer */
#define LIBARCHIVE_VERSION_NUMBER "3007004"

/* Version number of libarchive */
#define LIBARCHIVE_VERSION_STRING "3.7.4"

/* Define to 1 if `lstat' dereferences a symlink specified with a trailing
   slash. */
/* #undef LSTAT_FOLLOWS_SLASHED_SYMLINK */

/* Define to 1 if `major', `minor', and `makedev' are declared in <mkdev.h>.
   */
/* #undef MAJOR_IN_MKDEV */

/* Define to 1 if `major', `minor', and `makedev' are declared in
   <sysmacros.h>. */
/* #undef MAJOR_IN_SYSMACROS */

/* Define to 1 if your C compiler doesn't accept -c and -o together. */
/* #undef NO_MINUS_C_MINUS_O */

/* The size of `wchar_t', as computed by sizeof. */
#define SIZEOF_WCHAR_T 4

/* Define to 1 if strerror_r returns char *. */
/* #undef STRERROR_R_CHAR_P */

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
/* #undef TIME_WITH_SYS_TIME */

/* Version number of package */
/* #undef LIBATTR_PKGCONFIG_VERSION */

/* Version number of package */
/* #undef LIBACL_PKGCONFIG_VERSION */

/* Version number of package */
/* #undef LIBRICHACL_PKGCONFIG_VERSION */

/*
 * Some platform requires a macro to use extension functions.
 */
#define SAFE_TO_DEFINE_EXTENSIONS 1
#ifdef SAFE_TO_DEFINE_EXTENSIONS
/* Enable extensions on AIX 3, Interix.  */
#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
/* Enable threading extensions on Solaris.  */
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif
/* Enable extensions on HP NonStop.  */
#ifndef _TANDEM_SOURCE
# define _TANDEM_SOURCE 1
#endif
/* Enable general extensions on Solaris.  */
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif
#endif /* SAFE_TO_DEFINE_EXTENSIONS */

/* Version number of package */
#define VERSION "3.7.4"

/* Number of bits in a file offset, on hosts where this is settable. */
/* #undef _FILE_OFFSET_BITS */

/* Define to 1 to make fseeko visible on some hosts (e.g. glibc 2.2). */
/* #undef _LARGEFILE_SOURCE */

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Define to control Windows SDK version */
#ifndef NTDDI_VERSION
/* #undef NTDDI_VERSION */
#endif // NTDDI_VERSION

#ifndef _WIN32_WINNT
/* #undef _WIN32_WINNT */
#endif // _WIN32_WINNT

#ifndef WINVER
/* #undef WINVER */
#endif // WINVER

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to `int' if <sys/types.h> doesn't define. */
/* #undef gid_t */

/* Define to `unsigned long' if <sys/types.h> does not define. */
/* #undef id_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef mode_t */

/* Define to `long long' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `int' if <sys/types.h> doesn't define. */
/* #undef pid_t */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef ssize_t */

/* Define to `int' if <sys/types.h> doesn't define. */
/* #undef uid_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef intptr_t */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef uintptr_t */
