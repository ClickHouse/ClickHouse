#ifndef _POSIX_C_SOURCE
    #define _POSIX_C_SOURCE 200809L
#endif

#if HAVE_CONFIG_H
    #include <config.h>
#endif
#include "data-pool.h"
#include "maxminddb-compat-util.h"
#include "maxminddb.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
    #ifndef UNICODE
        #define UNICODE
    #endif
    #include <windows.h>
    #include <ws2ipdef.h>
    #ifndef SSIZE_MAX
        #define SSIZE_MAX INTPTR_MAX
    #endif
typedef ADDRESS_FAMILY sa_family_t;
#else
    #include <arpa/inet.h>
    #include <sys/mman.h>
    #include <unistd.h>
#endif

#define MMDB_DATA_SECTION_SEPARATOR (16)
// The maximum recursive decoder depth for a single entry. This stops unbounded
// recursion, including a pointer cycle. See "Reader Resource Limits" in the
// MaxMind DB specification.
#ifndef MAXIMUM_DATA_STRUCTURE_DEPTH
    #define MAXIMUM_DATA_STRUCTURE_DEPTH (512)
#endif

#if MAXIMUM_DATA_STRUCTURE_DEPTH < 1 || MAXIMUM_DATA_STRUCTURE_DEPTH > INT_MAX
    #error "MAXIMUM_DATA_STRUCTURE_DEPTH must be between 1 and INT_MAX"
#endif

// The maximum number of data-section values decoded for a single entry. This
// bounds a pointer fan-out, where nested pointers to shared targets would
// otherwise cost 2**depth decode operations. The largest real records decode a
// few hundred values, so this leaves a wide margin. See "Reader Resource
// Limits" in the MaxMind DB specification.
#ifndef MAXIMUM_DATA_STRUCTURE_VALUES
    #define MAXIMUM_DATA_STRUCTURE_VALUES (1U << 16)
#endif

// The upper bound matters on platforms where size_t is narrower than the
// preprocessor's integer arithmetic.
#if MAXIMUM_DATA_STRUCTURE_VALUES < 1 ||                                       \
    MAXIMUM_DATA_STRUCTURE_VALUES > SIZE_MAX
    #error "MAXIMUM_DATA_STRUCTURE_VALUES must be between 1 and SIZE_MAX"
#endif

// The maximum total bytes of string and bytes payloads decoded for a single
// entry. libmaxminddb borrows payload bytes (each node points into the data
// section, it does not copy), so the value count above already bounds the
// library's own memory. But a fan-out of pointers to one large value produces
// many nodes that all reference it. A caller that copies each node into a
// language string then materializes far more than the file holds. This bounds
// that copied total. The largest real records hold about a kilobyte of
// payload, so 2 MiB leaves a wide margin while stopping the amplification. It
// can be raised at build time with -DMAXIMUM_DATA_STRUCTURE_BYTES=<n>.
#ifndef MAXIMUM_DATA_STRUCTURE_BYTES
    #define MAXIMUM_DATA_STRUCTURE_BYTES (1U << 21)
#endif

#if MAXIMUM_DATA_STRUCTURE_BYTES < 1
    #error "MAXIMUM_DATA_STRUCTURE_BYTES must be at least 1"
#endif

#ifdef MMDB_DEBUG
    #define DEBUG_MSG(msg) fprintf(stderr, msg "\n")
    #define DEBUG_MSGF(fmt, ...) fprintf(stderr, fmt "\n", __VA_ARGS__)
    #define DEBUG_BINARY(fmt, byte)                                            \
        do {                                                                   \
            char *binary = byte_to_binary(byte);                               \
            if (NULL == binary) {                                              \
                fprintf(stderr, "Calloc failed in DEBUG_BINARY\n");            \
                abort();                                                       \
            }                                                                  \
            fprintf(stderr, fmt "\n", binary);                                 \
            free(binary);                                                      \
        } while (0)
    #define DEBUG_NL fprintf(stderr, "\n")
#else
    #define DEBUG_MSG(...)
    #define DEBUG_MSGF(...)
    #define DEBUG_BINARY(...)
    #define DEBUG_NL
#endif

#ifdef MMDB_DEBUG
char *byte_to_binary(uint8_t byte) {
    char *bits = calloc(9, sizeof(char));
    if (NULL == bits) {
        return bits;
    }

    for (uint8_t i = 0; i < 8; i++) {
        bits[i] = byte & (128 >> i) ? '1' : '0';
    }
    bits[8] = '\0';

    return bits;
}

char *type_num_to_name(uint8_t num) {
    switch (num) {
        case 0:
            return "extended";
        case 1:
            return "pointer";
        case 2:
            return "utf8_string";
        case 3:
            return "double";
        case 4:
            return "bytes";
        case 5:
            return "uint16";
        case 6:
            return "uint32";
        case 7:
            return "map";
        case 8:
            return "int32";
        case 9:
            return "uint64";
        case 10:
            return "uint128";
        case 11:
            return "array";
        case 12:
            return "container";
        case 13:
            return "end_marker";
        case 14:
            return "boolean";
        case 15:
            return "float";
        default:
            return "unknown type";
    }
}
#endif

/* None of the values we check on the lhs are bigger than uint32_t, so on
 * platforms where SIZE_MAX is a 64-bit integer, this would be a no-op, and it
 * makes the compiler complain if we do the check anyway. */
#if SIZE_MAX == UINT32_MAX
    #define MAYBE_CHECK_SIZE_OVERFLOW(lhs, rhs, error)                         \
        if ((lhs) > (rhs)) {                                                   \
            return error;                                                      \
        }
#else
    #define MAYBE_CHECK_SIZE_OVERFLOW(...)
#endif

typedef struct record_info_s {
    uint16_t record_length;
    uint32_t (*left_record_getter)(const uint8_t *);
    uint32_t (*right_record_getter)(const uint8_t *);
    uint8_t right_record_offset;
} record_info_s;

typedef struct decode_state_s {
    size_t values;
    uint64_t bytes;
} decode_state_s;

#define METADATA_MARKER "\xab\xcd\xefMaxMind.com"
/* This is 128kb */
#define METADATA_BLOCK_MAX_SIZE 131072

// 64 leads us to allocating 4 KiB on a 64bit system.
#define MMDB_POOL_INIT_SIZE 64

static int map_file(MMDB_s *const mmdb);
static const uint8_t *find_metadata(const uint8_t *file_content,
                                    ssize_t file_size,
                                    uint32_t *metadata_size);
static int read_metadata(MMDB_s *mmdb);
static MMDB_s make_fake_metadata_db(const MMDB_s *const mmdb);
static int
value_for_key_as_uint16(MMDB_entry_s *start, char *key, uint16_t *value);
static int
value_for_key_as_uint32(MMDB_entry_s *start, char *key, uint32_t *value);
static int
value_for_key_as_uint64(MMDB_entry_s *start, char *key, uint64_t *value);
static int
value_for_key_as_string(MMDB_entry_s *start, char *key, char const **value);
static int populate_languages_metadata(MMDB_s *mmdb,
                                       MMDB_s *metadata_db,
                                       MMDB_entry_s *metadata_start);
static int populate_description_metadata(MMDB_s *mmdb,
                                         MMDB_s *metadata_db,
                                         MMDB_entry_s *metadata_start);
static int resolve_any_address(const char *ipstr, struct addrinfo **addresses);
static int find_address_in_search_tree(const MMDB_s *const mmdb,
                                       uint8_t const *address,
                                       sa_family_t address_family,
                                       MMDB_lookup_result_s *result);
static record_info_s record_info_for_database(const MMDB_s *const mmdb);
static int find_ipv4_start_node(MMDB_s *const mmdb);
static uint8_t record_type(const MMDB_s *const mmdb, uint64_t record);
static uint32_t get_left_28_bit_record(const uint8_t *record);
static uint32_t get_right_28_bit_record(const uint8_t *record);
static uint32_t data_section_offset_for_record(const MMDB_s *const mmdb,
                                               uint64_t record);
static size_t path_length(va_list va_path);
static int lookup_path_in_array(const char *path_elem,
                                const MMDB_s *const mmdb,
                                MMDB_entry_data_s *entry_data);
static int lookup_path_in_map(const char *path_elem,
                              const MMDB_s *const mmdb,
                              MMDB_entry_data_s *entry_data);
static int skip_map_or_array(const MMDB_s *const mmdb,
                             MMDB_entry_data_s *entry_data,
                             int depth);
static int decode_one_follow(const MMDB_s *const mmdb,
                             uint32_t offset,
                             MMDB_entry_data_s *entry_data);
static int decode_one(const MMDB_s *const mmdb,
                      uint32_t offset,
                      MMDB_entry_data_s *entry_data);
static int get_ext_type(int raw_ext_type);
static uint32_t
get_ptr_from(uint8_t ctrl, uint8_t const *const ptr, int ptr_size);
static int get_entry_data_list(const MMDB_s *const mmdb,
                               uint32_t offset,
                               MMDB_entry_data_list_s *const entry_data_list,
                               MMDB_data_pool_s *const pool,
                               decode_state_s *const decode_state,
                               int depth);
static int
alloc_entry_data_list(MMDB_data_pool_s *const pool,
                      decode_state_s *const decode_state,
                      MMDB_entry_data_list_s **const entry_data_list);
static float get_ieee754_float(const uint8_t *restrict p);
static double get_ieee754_double(const uint8_t *restrict p);
static uint32_t get_uint32(const uint8_t *p);
static uint32_t get_uint24(const uint8_t *p);
static uint32_t get_uint16(const uint8_t *p);
static uint64_t get_uintX(const uint8_t *p, int length);
static int32_t get_sintX(const uint8_t *p, int length);
static void free_mmdb_struct(MMDB_s *const mmdb);
static void free_languages_metadata(MMDB_s *mmdb);
static void free_descriptions_metadata(MMDB_s *mmdb);
static MMDB_entry_data_list_s *
dump_entry_data_list(FILE *stream,
                     MMDB_entry_data_list_s *entry_data_list,
                     int indent,
                     int *status);
static void print_indentation(FILE *stream, int i);
static char *bytes_to_hex(uint8_t const *bytes, uint32_t size);

#define CHECKED_DECODE_ONE(mmdb, offset, entry_data)                           \
    do {                                                                       \
        int status = decode_one(mmdb, offset, entry_data);                     \
        if (MMDB_SUCCESS != status) {                                          \
            DEBUG_MSGF("CHECKED_DECODE_ONE failed."                            \
                       " status = %d (%s)",                                    \
                       status,                                                 \
                       MMDB_strerror(status));                                 \
            return status;                                                     \
        }                                                                      \
    } while (0)

#define CHECKED_DECODE_ONE_FOLLOW(mmdb, offset, entry_data)                    \
    do {                                                                       \
        int status = decode_one_follow(mmdb, offset, entry_data);              \
        if (MMDB_SUCCESS != status) {                                          \
            DEBUG_MSGF("CHECKED_DECODE_ONE_FOLLOW failed."                     \
                       " status = %d (%s)",                                    \
                       status,                                                 \
                       MMDB_strerror(status));                                 \
            return status;                                                     \
        }                                                                      \
    } while (0)

#define FREE_AND_SET_NULL(p)                                                   \
    {                                                                          \
        free((void *)(p));                                                     \
        (p) = NULL;                                                            \
    }

int MMDB_open(const char *const filename, uint32_t flags, MMDB_s *const mmdb) {
    int status = MMDB_SUCCESS;

    mmdb->file_content = NULL;
    mmdb->data_section = NULL;
    mmdb->metadata.database_type = NULL;
    mmdb->metadata.languages.count = 0;
    mmdb->metadata.languages.names = NULL;
    mmdb->metadata.description.count = 0;

    mmdb->filename = mmdb_strdup(filename);
    if (NULL == mmdb->filename) {
        status = MMDB_OUT_OF_MEMORY_ERROR;
        goto cleanup;
    }

    if ((flags & MMDB_MODE_MASK) == 0) {
        flags |= MMDB_MODE_MMAP;
    }
    mmdb->flags = flags;

    if (MMDB_SUCCESS != (status = map_file(mmdb))) {
        goto cleanup;
    }

#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif

    uint32_t metadata_size = 0;
    const uint8_t *metadata =
        find_metadata(mmdb->file_content, mmdb->file_size, &metadata_size);
    if (NULL == metadata) {
        status = MMDB_INVALID_METADATA_ERROR;
        goto cleanup;
    }

    mmdb->metadata_section = metadata;
    mmdb->metadata_section_size = metadata_size;

    status = read_metadata(mmdb);
    if (MMDB_DECODER_LIMIT_ERROR == status) {
        // Metadata that exceeds a decoder limit is invalid metadata.
        status = MMDB_INVALID_METADATA_ERROR;
    }
    if (MMDB_SUCCESS != status) {
        goto cleanup;
    }

    if (mmdb->metadata.binary_format_major_version != 2) {
        status = MMDB_UNKNOWN_DATABASE_FORMAT_ERROR;
        goto cleanup;
    }

    if (!can_multiply(SSIZE_MAX,
                      mmdb->metadata.node_count,
                      mmdb->full_record_byte_size)) {
        status = MMDB_INVALID_METADATA_ERROR;
        goto cleanup;
    }
    ssize_t search_tree_size = (ssize_t)mmdb->metadata.node_count *
                               (ssize_t)mmdb->full_record_byte_size;

    mmdb->data_section =
        mmdb->file_content + search_tree_size + MMDB_DATA_SECTION_SEPARATOR;
    if (mmdb->file_size < MMDB_DATA_SECTION_SEPARATOR ||
        search_tree_size > mmdb->file_size - MMDB_DATA_SECTION_SEPARATOR) {
        status = MMDB_INVALID_METADATA_ERROR;
        goto cleanup;
    }
    ssize_t data_section_size =
        mmdb->file_size - search_tree_size - MMDB_DATA_SECTION_SEPARATOR;
    if (data_section_size <= 0 || (uint64_t)data_section_size > UINT32_MAX) {
        status = MMDB_INVALID_METADATA_ERROR;
        goto cleanup;
    }
    mmdb->data_section_size = (uint32_t)data_section_size;

    // Although it is likely not possible to construct a database with valid
    // metadata, as parsed above, and a data_section_size less than 3, we do
    // this check as later we assume it is at least three when doing bound
    // checks.
    if (mmdb->data_section_size < 3) {
        status = MMDB_INVALID_DATA_ERROR;
        goto cleanup;
    }

    mmdb->metadata_section = metadata;
    mmdb->ipv4_start_node.node_value = 0;
    mmdb->ipv4_start_node.netmask = 0;

    // We do this immediately as otherwise there is a race to set
    // ipv4_start_node.node_value and ipv4_start_node.netmask.
    if (mmdb->metadata.ip_version == 6) {
        status = find_ipv4_start_node(mmdb);
        if (status != MMDB_SUCCESS) {
            goto cleanup;
        }
    }

cleanup:
    if (MMDB_SUCCESS != status) {
        int saved_errno = errno;
        free_mmdb_struct(mmdb);
        errno = saved_errno;
    }
    return status;
}

#ifdef _WIN32

static LPWSTR utf8_to_utf16(const char *utf8_str) {
    int wide_chars = MultiByteToWideChar(CP_UTF8, 0, utf8_str, -1, NULL, 0);
    wchar_t *utf16_str = (wchar_t *)calloc(wide_chars, sizeof(wchar_t));
    if (!utf16_str) {
        return NULL;
    }

    if (MultiByteToWideChar(CP_UTF8, 0, utf8_str, -1, utf16_str, wide_chars) <
        1) {
        free(utf16_str);
        return NULL;
    }

    return utf16_str;
}

static int map_file(MMDB_s *const mmdb) {
    ssize_t size;
    int status = MMDB_SUCCESS;
    HANDLE mmh = NULL;
    HANDLE fd = INVALID_HANDLE_VALUE;
    LPWSTR utf16_filename = utf8_to_utf16(mmdb->filename);
    if (!utf16_filename) {
        status = MMDB_FILE_OPEN_ERROR;
        goto cleanup;
    }
    fd = CreateFileW(utf16_filename,
                     GENERIC_READ,
                     FILE_SHARE_READ,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
    if (fd == INVALID_HANDLE_VALUE) {
        status = MMDB_FILE_OPEN_ERROR;
        goto cleanup;
    }
    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(fd, &file_size)) {
        status = MMDB_IO_ERROR;
        goto cleanup;
    }
    if (file_size.QuadPart < 0 || file_size.QuadPart > SSIZE_MAX) {
        status = MMDB_IO_ERROR;
        goto cleanup;
    }
    size = (ssize_t)file_size.QuadPart;
    mmh = CreateFileMapping(fd, NULL, PAGE_READONLY, 0, 0, NULL);
    /* Microsoft documentation for CreateFileMapping indicates this returns
        NULL not INVALID_HANDLE_VALUE on error */
    if (NULL == mmh) {
        status = MMDB_IO_ERROR;
        goto cleanup;
    }
    uint8_t *file_content =
        (uint8_t *)MapViewOfFile(mmh, FILE_MAP_READ, 0, 0, 0);
    if (file_content == NULL) {
        status = MMDB_IO_ERROR;
        goto cleanup;
    }

    mmdb->file_size = size;
    mmdb->file_content = file_content;

cleanup:;
    int saved_errno = errno;
    if (INVALID_HANDLE_VALUE != fd) {
        CloseHandle(fd);
    }
    if (NULL != mmh) {
        CloseHandle(mmh);
    }
    errno = saved_errno;
    free(utf16_filename);

    return status;
}

#else // _WIN32

static int map_file(MMDB_s *const mmdb) {
    int status = MMDB_SUCCESS;

    int o_flags = O_RDONLY;
    #ifdef O_CLOEXEC
    o_flags |= O_CLOEXEC;
    #endif
    int fd = open(mmdb->filename, o_flags);
    if (fd < 0) {
        status = MMDB_FILE_OPEN_ERROR;
        goto cleanup;
    }

    #if defined(FD_CLOEXEC) && !defined(O_CLOEXEC)
    int fd_flags = fcntl(fd, F_GETFD);
    if (fd_flags >= 0) {
        fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC);
    }
    #endif

    struct stat s;
    if (fstat(fd, &s)) {
        status = MMDB_FILE_OPEN_ERROR;
        goto cleanup;
    }

    off_t size = s.st_size;
    if (size < 0 || size > SSIZE_MAX) {
        status = MMDB_OUT_OF_MEMORY_ERROR;
        goto cleanup;
    }

    uint8_t *file_content =
        (uint8_t *)mmap(NULL, (size_t)size, PROT_READ, MAP_SHARED, fd, 0);
    if (MAP_FAILED == file_content) {
        if (ENOMEM == errno) {
            status = MMDB_OUT_OF_MEMORY_ERROR;
        } else {
            status = MMDB_IO_ERROR;
        }
        goto cleanup;
    }

    mmdb->file_size = (ssize_t)size;
    mmdb->file_content = file_content;

cleanup:;
    int saved_errno = errno;
    if (fd >= 0) {
        close(fd);
    }
    errno = saved_errno;

    return status;
}

#endif // _WIN32

static const uint8_t *find_metadata(const uint8_t *file_content,
                                    ssize_t file_size,
                                    uint32_t *metadata_size) {
    const ssize_t marker_len = sizeof(METADATA_MARKER) - 1;
    ssize_t max_size = file_size > METADATA_BLOCK_MAX_SIZE
                           ? METADATA_BLOCK_MAX_SIZE
                           : file_size;
    if (max_size < 0) {
        return NULL;
    }

    uint8_t const *search_area = (file_content + (file_size - max_size));
    uint8_t const *start = search_area;
    uint8_t const *tmp;
    do {
        tmp = mmdb_memmem(
            search_area, (size_t)max_size, METADATA_MARKER, marker_len);

        if (NULL != tmp) {
            max_size -= tmp - search_area;
            search_area = tmp;

            /* Continue searching just after the marker we just read, in case
             * there are multiple markers in the same file. This would be odd
             * but is certainly not impossible. */
            max_size -= marker_len;
            search_area += marker_len;
        }
    } while (NULL != tmp);

    if (search_area == start || max_size <= 0) {
        return NULL;
    }

    *metadata_size = (uint32_t)max_size;

    return search_area;
}

static int read_metadata(MMDB_s *mmdb) {
    /* We need to create a fake MMDB_s struct in order to decode values from
       the metadata. The metadata is basically just like the data section, so we
       want to use the same functions we use for the data section to get
       metadata values. */
    MMDB_s metadata_db = make_fake_metadata_db(mmdb);

    MMDB_entry_s metadata_start = {.mmdb = &metadata_db, .offset = 0};

    int status = value_for_key_as_uint32(
        &metadata_start, "node_count", &mmdb->metadata.node_count);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (!mmdb->metadata.node_count) {
        DEBUG_MSG("could not find node_count value in metadata");
        return MMDB_INVALID_METADATA_ERROR;
    }

    status = value_for_key_as_uint16(
        &metadata_start, "record_size", &mmdb->metadata.record_size);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (!mmdb->metadata.record_size) {
        DEBUG_MSG("could not find record_size value in metadata");
        return MMDB_INVALID_METADATA_ERROR;
    }

    if (mmdb->metadata.record_size != 24 && mmdb->metadata.record_size != 28 &&
        mmdb->metadata.record_size != 32) {
        DEBUG_MSGF("bad record size in metadata: %i",
                   mmdb->metadata.record_size);
        return MMDB_UNKNOWN_DATABASE_FORMAT_ERROR;
    }

    status = value_for_key_as_uint16(
        &metadata_start, "ip_version", &mmdb->metadata.ip_version);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (!mmdb->metadata.ip_version) {
        DEBUG_MSG("could not find ip_version value in metadata");
        return MMDB_INVALID_METADATA_ERROR;
    }
    if (!(mmdb->metadata.ip_version == 4 || mmdb->metadata.ip_version == 6)) {
        DEBUG_MSGF("ip_version value in metadata is not 4 or 6 - it was %i",
                   mmdb->metadata.ip_version);
        return MMDB_INVALID_METADATA_ERROR;
    }

    status = value_for_key_as_string(
        &metadata_start, "database_type", &mmdb->metadata.database_type);
    if (MMDB_SUCCESS != status) {
        DEBUG_MSG("error finding database_type value in metadata");
        return status;
    }

    status = populate_languages_metadata(mmdb, &metadata_db, &metadata_start);
    if (MMDB_SUCCESS != status) {
        DEBUG_MSG("could not populate languages from metadata");
        return status;
    }

    status =
        value_for_key_as_uint16(&metadata_start,
                                "binary_format_major_version",
                                &mmdb->metadata.binary_format_major_version);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (!mmdb->metadata.binary_format_major_version) {
        DEBUG_MSG(
            "could not find binary_format_major_version value in metadata");
        return MMDB_INVALID_METADATA_ERROR;
    }

    status =
        value_for_key_as_uint16(&metadata_start,
                                "binary_format_minor_version",
                                &mmdb->metadata.binary_format_minor_version);
    if (MMDB_SUCCESS != status) {
        return status;
    }

    status = value_for_key_as_uint64(
        &metadata_start, "build_epoch", &mmdb->metadata.build_epoch);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (!mmdb->metadata.build_epoch) {
        DEBUG_MSG("could not find build_epoch value in metadata");
        return MMDB_INVALID_METADATA_ERROR;
    }

    status = populate_description_metadata(mmdb, &metadata_db, &metadata_start);
    if (MMDB_SUCCESS != status) {
        DEBUG_MSG("could not populate description from metadata");
        return status;
    }

    mmdb->full_record_byte_size = mmdb->metadata.record_size * 2 / 8U;

    mmdb->depth = mmdb->metadata.ip_version == 4 ? 32 : 128;

    return MMDB_SUCCESS;
}

static MMDB_s make_fake_metadata_db(const MMDB_s *const mmdb) {
    MMDB_s fake_metadata_db = {.data_section = mmdb->metadata_section,
                               .data_section_size =
                                   mmdb->metadata_section_size};

    return fake_metadata_db;
}

static int
value_for_key_as_uint16(MMDB_entry_s *start, char *key, uint16_t *value) {
    MMDB_entry_data_s entry_data;
    const char *path[] = {key, NULL};
    int status = MMDB_aget_value(start, &entry_data, path);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (MMDB_DATA_TYPE_UINT16 != entry_data.type) {
        DEBUG_MSGF("expect uint16 for %s but received %s",
                   key,
                   type_num_to_name(entry_data.type));
        return MMDB_INVALID_METADATA_ERROR;
    }
    *value = entry_data.uint16;
    return MMDB_SUCCESS;
}

static int
value_for_key_as_uint32(MMDB_entry_s *start, char *key, uint32_t *value) {
    MMDB_entry_data_s entry_data;
    const char *path[] = {key, NULL};
    int status = MMDB_aget_value(start, &entry_data, path);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (MMDB_DATA_TYPE_UINT32 != entry_data.type) {
        DEBUG_MSGF("expect uint32 for %s but received %s",
                   key,
                   type_num_to_name(entry_data.type));
        return MMDB_INVALID_METADATA_ERROR;
    }
    *value = entry_data.uint32;
    return MMDB_SUCCESS;
}

static int
value_for_key_as_uint64(MMDB_entry_s *start, char *key, uint64_t *value) {
    MMDB_entry_data_s entry_data;
    const char *path[] = {key, NULL};
    int status = MMDB_aget_value(start, &entry_data, path);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (MMDB_DATA_TYPE_UINT64 != entry_data.type) {
        DEBUG_MSGF("expect uint64 for %s but received %s",
                   key,
                   type_num_to_name(entry_data.type));
        return MMDB_INVALID_METADATA_ERROR;
    }
    *value = entry_data.uint64;
    return MMDB_SUCCESS;
}

static int
value_for_key_as_string(MMDB_entry_s *start, char *key, char const **value) {
    MMDB_entry_data_s entry_data;
    const char *path[] = {key, NULL};
    int status = MMDB_aget_value(start, &entry_data, path);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (MMDB_DATA_TYPE_UTF8_STRING != entry_data.type) {
        DEBUG_MSGF("expect string for %s but received %s",
                   key,
                   type_num_to_name(entry_data.type));
        return MMDB_INVALID_METADATA_ERROR;
    }
    *value = mmdb_strndup(entry_data.utf8_string, entry_data.data_size);
    if (NULL == *value) {
        return MMDB_OUT_OF_MEMORY_ERROR;
    }
    return MMDB_SUCCESS;
}

static int populate_languages_metadata(MMDB_s *mmdb,
                                       MMDB_s *metadata_db,
                                       MMDB_entry_s *metadata_start) {
    MMDB_entry_data_s entry_data;

    const char *path[] = {"languages", NULL};
    int status = MMDB_aget_value(metadata_start, &entry_data, path);
    if (MMDB_SUCCESS != status) {
        return status;
    }
    if (MMDB_DATA_TYPE_ARRAY != entry_data.type) {
        return MMDB_INVALID_METADATA_ERROR;
    }

    MMDB_entry_s array_start = {.mmdb = metadata_db,
                                .offset = entry_data.offset};

    MMDB_entry_data_list_s *member;
    status = MMDB_get_entry_data_list(&array_start, &member);
    if (MMDB_SUCCESS != status) {
        return status;
    }

    MMDB_entry_data_list_s *first_member = member;

    uint32_t array_size = member->entry_data.data_size;
    MAYBE_CHECK_SIZE_OVERFLOW(
        array_size, SIZE_MAX / sizeof(char *), MMDB_INVALID_METADATA_ERROR);

    mmdb->metadata.languages.count = 0;
    mmdb->metadata.languages.names = calloc(array_size, sizeof(char *));
    if (NULL == mmdb->metadata.languages.names) {
        MMDB_free_entry_data_list(first_member);
        return MMDB_OUT_OF_MEMORY_ERROR;
    }

    for (uint32_t i = 0; i < array_size; i++) {
        member = member->next;
        if (MMDB_DATA_TYPE_UTF8_STRING != member->entry_data.type) {
            MMDB_free_entry_data_list(first_member);
            return MMDB_INVALID_METADATA_ERROR;
        }

        mmdb->metadata.languages.names[i] = mmdb_strndup(
            member->entry_data.utf8_string, member->entry_data.data_size);

        if (NULL == mmdb->metadata.languages.names[i]) {
            MMDB_free_entry_data_list(first_member);
            return MMDB_OUT_OF_MEMORY_ERROR;
        }
        // We assign this as we go so that if we fail a calloc and need to
        // free it, the count is right.
        mmdb->metadata.languages.count = i + 1;
    }

    MMDB_free_entry_data_list(first_member);

    return MMDB_SUCCESS;
}

static int populate_description_metadata(MMDB_s *mmdb,
                                         MMDB_s *metadata_db,
                                         MMDB_entry_s *metadata_start) {
    MMDB_entry_data_s entry_data;

    const char *path[] = {"description", NULL};
    int status = MMDB_aget_value(metadata_start, &entry_data, path);
    if (MMDB_SUCCESS != status) {
        return status;
    }

    if (MMDB_DATA_TYPE_MAP != entry_data.type) {
        DEBUG_MSGF("Unexpected entry_data type: %d", entry_data.type);
        return MMDB_INVALID_METADATA_ERROR;
    }

    MMDB_entry_s map_start = {.mmdb = metadata_db, .offset = entry_data.offset};

    MMDB_entry_data_list_s *member;
    status = MMDB_get_entry_data_list(&map_start, &member);
    if (MMDB_SUCCESS != status) {
        DEBUG_MSGF(
            "MMDB_get_entry_data_list failed while populating description."
            " status = %d (%s)",
            status,
            MMDB_strerror(status));
        return status;
    }

    MMDB_entry_data_list_s *first_member = member;

    uint32_t map_size = member->entry_data.data_size;
    mmdb->metadata.description.count = 0;
    if (0 == map_size) {
        mmdb->metadata.description.descriptions = NULL;
        goto cleanup;
    }
    MAYBE_CHECK_SIZE_OVERFLOW(map_size,
                              SIZE_MAX / sizeof(MMDB_description_s *),
                              MMDB_INVALID_METADATA_ERROR);

    mmdb->metadata.description.descriptions =
        calloc(map_size, sizeof(MMDB_description_s *));
    if (NULL == mmdb->metadata.description.descriptions) {
        status = MMDB_OUT_OF_MEMORY_ERROR;
        goto cleanup;
    }

    for (uint32_t i = 0; i < map_size; i++) {
        mmdb->metadata.description.descriptions[i] =
            calloc(1, sizeof(MMDB_description_s));
        if (NULL == mmdb->metadata.description.descriptions[i]) {
            status = MMDB_OUT_OF_MEMORY_ERROR;
            goto cleanup;
        }

        mmdb->metadata.description.count = i + 1;
        mmdb->metadata.description.descriptions[i]->language = NULL;
        mmdb->metadata.description.descriptions[i]->description = NULL;

        member = member->next;

        if (MMDB_DATA_TYPE_UTF8_STRING != member->entry_data.type) {
            status = MMDB_INVALID_METADATA_ERROR;
            goto cleanup;
        }

        mmdb->metadata.description.descriptions[i]->language = mmdb_strndup(
            member->entry_data.utf8_string, member->entry_data.data_size);

        if (NULL == mmdb->metadata.description.descriptions[i]->language) {
            status = MMDB_OUT_OF_MEMORY_ERROR;
            goto cleanup;
        }

        member = member->next;

        if (MMDB_DATA_TYPE_UTF8_STRING != member->entry_data.type) {
            status = MMDB_INVALID_METADATA_ERROR;
            goto cleanup;
        }

        mmdb->metadata.description.descriptions[i]->description = mmdb_strndup(
            member->entry_data.utf8_string, member->entry_data.data_size);

        if (NULL == mmdb->metadata.description.descriptions[i]->description) {
            status = MMDB_OUT_OF_MEMORY_ERROR;
            goto cleanup;
        }
    }

cleanup:
    MMDB_free_entry_data_list(first_member);

    return status;
}

MMDB_lookup_result_s MMDB_lookup_string(const MMDB_s *const mmdb,
                                        const char *const ipstr,
                                        int *const gai_error,
                                        int *const mmdb_error) {
    MMDB_lookup_result_s result = {.found_entry = false,
                                   .netmask = 0,
                                   .entry = {.mmdb = mmdb, .offset = 0}};

    struct addrinfo *addresses = NULL;
    *gai_error = resolve_any_address(ipstr, &addresses);

    if (!*gai_error) {
        result = MMDB_lookup_sockaddr(mmdb, addresses->ai_addr, mmdb_error);
    } else {
        /* No MMDB error occurred; the GAI failure is reported via
         * *gai_error. Set *mmdb_error to a defined value so callers
         * don't read indeterminate memory. */
        *mmdb_error = MMDB_SUCCESS;
    }

    if (NULL != addresses) {
        freeaddrinfo(addresses);
    }

    return result;
}

static int resolve_any_address(const char *ipstr, struct addrinfo **addresses) {
    struct addrinfo hints = {
        .ai_family = AF_UNSPEC,
        .ai_flags = AI_NUMERICHOST,
        // We set ai_socktype so that we only get one result back
        .ai_socktype = SOCK_STREAM};

    int gai_status = getaddrinfo(ipstr, NULL, &hints, addresses);
    if (gai_status) {
        return gai_status;
    }

    return 0;
}

MMDB_lookup_result_s MMDB_lookup_sockaddr(const MMDB_s *const mmdb,
                                          const struct sockaddr *const sockaddr,
                                          int *const mmdb_error) {
    MMDB_lookup_result_s result = {.found_entry = false,
                                   .netmask = 0,
                                   .entry = {.mmdb = mmdb, .offset = 0}};

    uint8_t mapped_address[16];
    uint8_t const *address;
    // Reject families other than AF_INET/AF_INET6 before casting to
    // sockaddr_in/sockaddr_in6, which would otherwise read past the
    // truncated struct sockaddr the caller passed in.
    if (mmdb->metadata.ip_version == 4) {
        if (sockaddr->sa_family == AF_INET6) {
            *mmdb_error = MMDB_IPV6_LOOKUP_IN_IPV4_DATABASE_ERROR;
            return result;
        }
        if (sockaddr->sa_family != AF_INET) {
            *mmdb_error = MMDB_INVALID_NETWORK_ADDRESS_ERROR;
            return result;
        }
        address = (uint8_t const *)&((struct sockaddr_in const *)sockaddr)
                      ->sin_addr.s_addr;
    } else {
        if (sockaddr->sa_family == AF_INET6) {
            address = (uint8_t const *)&((struct sockaddr_in6 const *)sockaddr)
                          ->sin6_addr.s6_addr;
        } else if (sockaddr->sa_family == AF_INET) {
            address = mapped_address;
            memset(mapped_address, 0, 12);
            memcpy(mapped_address + 12,
                   &((struct sockaddr_in const *)sockaddr)->sin_addr.s_addr,
                   4);
        } else {
            *mmdb_error = MMDB_INVALID_NETWORK_ADDRESS_ERROR;
            return result;
        }
    }

    *mmdb_error = find_address_in_search_tree(
        mmdb, address, sockaddr->sa_family, &result);

    return result;
}

static int find_address_in_search_tree(const MMDB_s *const mmdb,
                                       uint8_t const *address,
                                       sa_family_t address_family,
                                       MMDB_lookup_result_s *result) {
    record_info_s record_info = record_info_for_database(mmdb);
    if (record_info.right_record_offset == 0) {
        return MMDB_UNKNOWN_DATABASE_FORMAT_ERROR;
    }

    uint64_t value = 0;
    uint16_t current_bit = 0;
    if (mmdb->metadata.ip_version == 6 && address_family == AF_INET) {
        value = mmdb->ipv4_start_node.node_value;
        current_bit = mmdb->ipv4_start_node.netmask;
    }

    uint32_t node_count = mmdb->metadata.node_count;
    const uint8_t *search_tree = mmdb->file_content;
    const uint8_t *record_pointer;
    for (; current_bit < mmdb->depth && value < node_count; current_bit++) {
        uint8_t bit =
            1U & (address[current_bit >> 3] >> (7 - (current_bit % 8)));

        // Note that value*record_info.record_length can be larger than 2**32
        record_pointer = &search_tree[value * record_info.record_length];
        if (record_pointer + record_info.record_length > mmdb->data_section) {
            return MMDB_CORRUPT_SEARCH_TREE_ERROR;
        }
        if (bit) {
            record_pointer += record_info.right_record_offset;
            value = record_info.right_record_getter(record_pointer);
        } else {
            value = record_info.left_record_getter(record_pointer);
        }
    }

    result->netmask = current_bit;

    uint8_t type = record_type(mmdb, value);
    if (type == MMDB_RECORD_TYPE_INVALID) {
        return MMDB_CORRUPT_SEARCH_TREE_ERROR;
    }

    if (type == MMDB_RECORD_TYPE_EMPTY) {
        // record is empty
        result->found_entry = false;
        return MMDB_SUCCESS;
    }
    result->found_entry = true;
    result->entry.offset = data_section_offset_for_record(mmdb, value);

    return MMDB_SUCCESS;
}

static record_info_s record_info_for_database(const MMDB_s *const mmdb) {
    record_info_s record_info = {.record_length = mmdb->full_record_byte_size,
                                 .right_record_offset = 0};

    if (record_info.record_length == 6) {
        record_info.left_record_getter = &get_uint24;
        record_info.right_record_getter = &get_uint24;
        record_info.right_record_offset = 3;
    } else if (record_info.record_length == 7) {
        record_info.left_record_getter = &get_left_28_bit_record;
        record_info.right_record_getter = &get_right_28_bit_record;
        record_info.right_record_offset = 3;
    } else if (record_info.record_length == 8) {
        record_info.left_record_getter = &get_uint32;
        record_info.right_record_getter = &get_uint32;
        record_info.right_record_offset = 4;
    }

    // Callers must check that right_record_offset is non-zero in case none of
    // the above conditions matched.

    return record_info;
}

static int find_ipv4_start_node(MMDB_s *const mmdb) {
    /* In a pathological case of a database with a single node search tree,
     * this check will be true even after we've found the IPv4 start node, but
     * that doesn't seem worth trying to fix. */
    if (mmdb->ipv4_start_node.node_value != 0) {
        return MMDB_SUCCESS;
    }

    record_info_s record_info = record_info_for_database(mmdb);
    if (record_info.right_record_offset == 0) {
        return MMDB_UNKNOWN_DATABASE_FORMAT_ERROR;
    }

    const uint8_t *search_tree = mmdb->file_content;
    uint32_t node_value = 0;
    const uint8_t *record_pointer;
    uint16_t netmask;
    uint32_t node_count = mmdb->metadata.node_count;

    for (netmask = 0; netmask < 96 && node_value < node_count; netmask++) {
        record_pointer =
            &search_tree[(uint64_t)node_value * record_info.record_length];
        if (record_pointer + record_info.record_length > mmdb->data_section) {
            return MMDB_CORRUPT_SEARCH_TREE_ERROR;
        }
        node_value = record_info.left_record_getter(record_pointer);
    }

    mmdb->ipv4_start_node.node_value = node_value;
    mmdb->ipv4_start_node.netmask = netmask;

    return MMDB_SUCCESS;
}

static uint8_t record_type(const MMDB_s *const mmdb, uint64_t record) {
    uint32_t node_count = mmdb->metadata.node_count;

    /* Ideally we'd check to make sure that a record never points to a
     * previously seen value, but that's more complicated. For now, we can
     * at least check that we don't end up at the top of the tree again. */
    if (record == 0) {
        DEBUG_MSG("record has a value of 0");
        return MMDB_RECORD_TYPE_INVALID;
    }

    if (record < node_count) {
        return MMDB_RECORD_TYPE_SEARCH_NODE;
    }

    if (record == node_count) {
        return MMDB_RECORD_TYPE_EMPTY;
    }

    uint64_t data_offset = record - node_count;
    if (data_offset < MMDB_DATA_SECTION_SEPARATOR) {
        DEBUG_MSG("record points into the data section separator");
        return MMDB_RECORD_TYPE_INVALID;
    }

    data_offset -= MMDB_DATA_SECTION_SEPARATOR;
    if (data_offset < mmdb->data_section_size) {
        return MMDB_RECORD_TYPE_DATA;
    }

    DEBUG_MSG("record has a value that points outside of the database");
    return MMDB_RECORD_TYPE_INVALID;
}

static uint32_t get_left_28_bit_record(const uint8_t *record) {
    return record[0] * 65536 + record[1] * 256 + record[2] +
           (uint32_t)((record[3] & 0xf0) << 20);
}

static uint32_t get_right_28_bit_record(const uint8_t *record) {
    uint32_t value = get_uint32(record);
    return value & 0xfffffff;
}

int MMDB_read_node(const MMDB_s *const mmdb,
                   uint32_t node_number,
                   MMDB_search_node_s *const node) {
    record_info_s record_info = record_info_for_database(mmdb);
    if (record_info.right_record_offset == 0) {
        return MMDB_UNKNOWN_DATABASE_FORMAT_ERROR;
    }

    if (node_number >= mmdb->metadata.node_count) {
        return MMDB_INVALID_NODE_NUMBER_ERROR;
    }

    const uint8_t *search_tree = mmdb->file_content;
    const uint8_t *record_pointer =
        &search_tree[(uint64_t)node_number * record_info.record_length];
    if (record_pointer + record_info.record_length > mmdb->data_section) {
        return MMDB_CORRUPT_SEARCH_TREE_ERROR;
    }
    node->left_record = record_info.left_record_getter(record_pointer);
    record_pointer += record_info.right_record_offset;
    node->right_record = record_info.right_record_getter(record_pointer);

    node->left_record_type = record_type(mmdb, node->left_record);
    node->right_record_type = record_type(mmdb, node->right_record);

    if (node->left_record_type == MMDB_RECORD_TYPE_INVALID ||
        node->right_record_type == MMDB_RECORD_TYPE_INVALID) {
        return MMDB_CORRUPT_SEARCH_TREE_ERROR;
    }

    // Note that offset will be invalid if the record type is not
    // MMDB_RECORD_TYPE_DATA, but that's ok. Any use of the record entry
    // for other data types is a programming error.
    node->left_record_entry = (struct MMDB_entry_s){
        .mmdb = mmdb,
        .offset = data_section_offset_for_record(mmdb, node->left_record),
    };
    node->right_record_entry = (struct MMDB_entry_s){
        .mmdb = mmdb,
        .offset = data_section_offset_for_record(mmdb, node->right_record),
    };

    return MMDB_SUCCESS;
}

static uint32_t data_section_offset_for_record(const MMDB_s *const mmdb,
                                               uint64_t record) {
    return (uint32_t)record - mmdb->metadata.node_count -
           MMDB_DATA_SECTION_SEPARATOR;
}

int MMDB_get_value(MMDB_entry_s *const start,
                   MMDB_entry_data_s *const entry_data,
                   ...) {
    va_list path;
    va_start(path, entry_data);
    int status = MMDB_vget_value(start, entry_data, path);
    va_end(path);
    return status;
}

int MMDB_vget_value(MMDB_entry_s *const start,
                    MMDB_entry_data_s *const entry_data,
                    va_list va_path) {
    size_t length = path_length(va_path);
    const char *path_elem;
    int i = 0;

    if (length == SIZE_MAX) {
        return MMDB_INVALID_METADATA_ERROR;
    }

    const char **path = calloc(length + 1, sizeof(const char *));
    if (NULL == path) {
        return MMDB_OUT_OF_MEMORY_ERROR;
    }

    while (NULL != (path_elem = va_arg(va_path, char *))) {
        path[i] = path_elem;
        i++;
    }
    path[i] = NULL;

    int status = MMDB_aget_value(start, entry_data, path);

    free(path);

    return status;
}

static size_t path_length(va_list va_path) {
    size_t i = 0;
    va_list path_copy;
    va_copy(path_copy, va_path);

    while (NULL != va_arg(path_copy, char *)) {
        i++;
    }

    va_end(path_copy);

    return i;
}

int MMDB_aget_value(MMDB_entry_s *const start,
                    MMDB_entry_data_s *const entry_data,
                    const char *const *const path) {
    const MMDB_s *const mmdb = start->mmdb;
    uint32_t offset = start->offset;

    memset(entry_data, 0, sizeof(MMDB_entry_data_s));
    DEBUG_NL;
    DEBUG_MSG("looking up value by path");

    CHECKED_DECODE_ONE_FOLLOW(mmdb, offset, entry_data);

    DEBUG_NL;
    DEBUG_MSGF("top level element is a %s", type_num_to_name(entry_data->type));

    /* Can this happen? It'd probably represent a pathological case under
     * normal use, but there's nothing preventing someone from passing an
     * invalid MMDB_entry_s struct to this function */
    if (!entry_data->has_data) {
        return MMDB_INVALID_LOOKUP_PATH_ERROR;
    }

    const char *path_elem;
    int i = 0;
    while (NULL != (path_elem = path[i++])) {
        DEBUG_NL;
        DEBUG_MSGF("path elem = %s", path_elem);

        /* XXX - it'd be good to find a quicker way to skip through these
           entries that doesn't involve decoding them
           completely. Basically we need to just use the size from the
           control byte to advance our pointer rather than calling
           decode_one(). */
        if (entry_data->type == MMDB_DATA_TYPE_ARRAY) {
            int status = lookup_path_in_array(path_elem, mmdb, entry_data);
            if (MMDB_SUCCESS != status) {
                memset(entry_data, 0, sizeof(MMDB_entry_data_s));
                return status;
            }
        } else if (entry_data->type == MMDB_DATA_TYPE_MAP) {
            int status = lookup_path_in_map(path_elem, mmdb, entry_data);
            if (MMDB_SUCCESS != status) {
                memset(entry_data, 0, sizeof(MMDB_entry_data_s));
                return status;
            }
        } else {
            /* Once we make the code traverse maps & arrays without calling
             * decode_one() we can get rid of this. */
            memset(entry_data, 0, sizeof(MMDB_entry_data_s));
            return MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR;
        }
    }

    return MMDB_SUCCESS;
}

static int lookup_path_in_array(const char *path_elem,
                                const MMDB_s *const mmdb,
                                MMDB_entry_data_s *entry_data) {
    uint32_t size = entry_data->data_size;
    char *first_invalid;

    int saved_errno = errno;
    errno = 0;
    long array_index = strtol(path_elem, &first_invalid, 10);
    if (ERANGE == errno) {
        errno = saved_errno;
        return MMDB_INVALID_LOOKUP_PATH_ERROR;
    }
    errno = saved_errno;

    if (array_index < 0) {
        array_index += size;

        if (array_index < 0) {
            return MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR;
        }
    }

    if (*first_invalid || (unsigned long)array_index >= size) {
        return MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR;
    }

    for (long i = 0; i < array_index; i++) {
        /* We don't want to follow a pointer here. If the next element is a
         * pointer we simply skip it and keep going */
        CHECKED_DECODE_ONE(mmdb, entry_data->offset_to_next, entry_data);
        int status = skip_map_or_array(mmdb, entry_data, 0);
        if (MMDB_SUCCESS != status) {
            return status;
        }
    }

    MMDB_entry_data_s value;
    CHECKED_DECODE_ONE_FOLLOW(mmdb, entry_data->offset_to_next, &value);
    memcpy(entry_data, &value, sizeof(MMDB_entry_data_s));

    return MMDB_SUCCESS;
}

static int lookup_path_in_map(const char *path_elem,
                              const MMDB_s *const mmdb,
                              MMDB_entry_data_s *entry_data) {
    uint32_t size = entry_data->data_size;
    uint32_t offset = entry_data->offset_to_next;
    size_t path_elem_len = strlen(path_elem);

    while (size-- > 0) {
        MMDB_entry_data_s key, value;
        CHECKED_DECODE_ONE_FOLLOW(mmdb, offset, &key);

        uint32_t offset_to_value = key.offset_to_next;

        if (MMDB_DATA_TYPE_UTF8_STRING != key.type) {
            return MMDB_INVALID_DATA_ERROR;
        }

        if (key.data_size == path_elem_len &&
            !memcmp(path_elem, key.utf8_string, path_elem_len)) {

            DEBUG_MSG("found key matching path elem");

            CHECKED_DECODE_ONE_FOLLOW(mmdb, offset_to_value, &value);
            memcpy(entry_data, &value, sizeof(MMDB_entry_data_s));
            return MMDB_SUCCESS;
        } else {
            /* We don't want to follow a pointer here. If the next element is
             * a pointer we simply skip it and keep going */
            CHECKED_DECODE_ONE(mmdb, offset_to_value, &value);
            int status = skip_map_or_array(mmdb, &value, 0);
            if (MMDB_SUCCESS != status) {
                return status;
            }
            offset = value.offset_to_next;
        }
    }

    memset(entry_data, 0, sizeof(MMDB_entry_data_s));
    return MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR;
}

static int skip_map_or_array(const MMDB_s *const mmdb,
                             MMDB_entry_data_s *entry_data,
                             int depth) {
    if (depth >= MAXIMUM_DATA_STRUCTURE_DEPTH) {
        DEBUG_MSG("reached the maximum data structure depth");
        return MMDB_DECODER_LIMIT_ERROR;
    }

    if (entry_data->type == MMDB_DATA_TYPE_MAP) {
        uint32_t size = entry_data->data_size;
        while (size-- > 0) {
            CHECKED_DECODE_ONE(
                mmdb, entry_data->offset_to_next, entry_data); // key
            CHECKED_DECODE_ONE(
                mmdb, entry_data->offset_to_next, entry_data); // value
            int status = skip_map_or_array(mmdb, entry_data, depth + 1);
            if (MMDB_SUCCESS != status) {
                return status;
            }
        }
    } else if (entry_data->type == MMDB_DATA_TYPE_ARRAY) {
        uint32_t size = entry_data->data_size;
        while (size-- > 0) {
            CHECKED_DECODE_ONE(
                mmdb, entry_data->offset_to_next, entry_data); // value
            int status = skip_map_or_array(mmdb, entry_data, depth + 1);
            if (MMDB_SUCCESS != status) {
                return status;
            }
        }
    }

    return MMDB_SUCCESS;
}

static int decode_one_follow(const MMDB_s *const mmdb,
                             uint32_t offset,
                             MMDB_entry_data_s *entry_data) {
    CHECKED_DECODE_ONE(mmdb, offset, entry_data);
    if (entry_data->type == MMDB_DATA_TYPE_POINTER) {
        uint32_t next = entry_data->offset_to_next;
        CHECKED_DECODE_ONE(mmdb, entry_data->pointer, entry_data);
        /* Pointers to pointers are illegal under the spec */
        if (entry_data->type == MMDB_DATA_TYPE_POINTER) {
            DEBUG_MSG("pointer points to another pointer");
            return MMDB_INVALID_DATA_ERROR;
        }

        /* The pointer could point to any part of the data section but the
         * next entry for this particular offset may be the one after the
         * pointer, not the one after whatever the pointer points to. This
         * depends on whether the pointer points to something that is a simple
         * value or a compound value. For a compound value, the next one is
         * the one after the pointer result, not the one after the pointer. */
        if (entry_data->type != MMDB_DATA_TYPE_MAP &&
            entry_data->type != MMDB_DATA_TYPE_ARRAY) {

            entry_data->offset_to_next = next;
        }
    }

    return MMDB_SUCCESS;
}

#if !MMDB_UINT128_IS_BYTE_ARRAY
static mmdb_uint128_t get_uint128(const uint8_t *p, int length) {
    mmdb_uint128_t value = 0;
    while (length-- > 0) {
        value <<= 8;
        value += *p++;
    }
    return value;
}
#endif

static int decode_one(const MMDB_s *const mmdb,
                      uint32_t offset,
                      MMDB_entry_data_s *entry_data) {
    const uint8_t *mem = mmdb->data_section;

    if (mmdb->data_section_size == 0) {
        // decode_one is also called with a fake mmdb whose data_section
        // points at the metadata; either way an empty section is invalid.
        DEBUG_MSG("decode_one called with an empty section");
        return MMDB_INVALID_DATA_ERROR;
    }

    // We subtract rather than add as it possible that offset + 1
    // could overflow for a corrupt database while an underflow
    // from data_section_size - 1 should not be possible.
    if (offset > mmdb->data_section_size - 1) {
        DEBUG_MSGF("Offset (%d) past data section (%d)",
                   offset,
                   mmdb->data_section_size);
        return MMDB_INVALID_DATA_ERROR;
    }

    entry_data->offset = offset;
    entry_data->has_data = true;

    DEBUG_NL;
    DEBUG_MSGF("Offset: %i", offset);

    uint8_t ctrl = mem[offset++];
    DEBUG_BINARY("Control byte: %s", ctrl);

    int type = (ctrl >> 5) & 7;
    DEBUG_MSGF("Type: %i (%s)", type, type_num_to_name(type));

    if (type == MMDB_DATA_TYPE_EXTENDED) {
        // Subtracting 1 to avoid possible overflow on offset + 1
        if (offset > mmdb->data_section_size - 1) {
            DEBUG_MSGF("Extended type offset (%d) past data section (%d)",
                       offset,
                       mmdb->data_section_size);
            return MMDB_INVALID_DATA_ERROR;
        }
        type = get_ext_type(mem[offset++]);
        DEBUG_MSGF("Extended type: %i (%s)", type, type_num_to_name(type));
    }

    entry_data->type = (uint32_t)type;

    if (type == MMDB_DATA_TYPE_POINTER) {
        uint8_t psize = ((ctrl >> 3) & 3) + 1;
        DEBUG_MSGF("Pointer size: %i", psize);

        // We check that the offset does not extend past the end of the
        // database and that the subtraction of psize did not underflow.
        if (offset > mmdb->data_section_size - psize ||
            mmdb->data_section_size < psize) {
            DEBUG_MSGF("Pointer offset (%d) past data section (%d)",
                       offset + psize,
                       mmdb->data_section_size);
            return MMDB_INVALID_DATA_ERROR;
        }
        entry_data->pointer = get_ptr_from(ctrl, &mem[offset], psize);
        DEBUG_MSGF("Pointer to: %i", entry_data->pointer);

        entry_data->data_size = psize;
        entry_data->offset_to_next = offset + psize;
        return MMDB_SUCCESS;
    }

    uint32_t size = ctrl & 31;
    switch (size) {
        case 29:
            // We subtract when checking offset to avoid possible overflow
            if (offset > mmdb->data_section_size - 1) {
                DEBUG_MSGF("String end (%d, case 29) past data section (%d)",
                           offset,
                           mmdb->data_section_size);
                return MMDB_INVALID_DATA_ERROR;
            }
            size = 29 + mem[offset++];
            break;
        case 30:
            // We subtract when checking offset to avoid possible overflow
            if (offset > mmdb->data_section_size - 2) {
                DEBUG_MSGF("String end (%d, case 30) past data section (%d)",
                           offset,
                           mmdb->data_section_size);
                return MMDB_INVALID_DATA_ERROR;
            }
            size = 285 + get_uint16(&mem[offset]);
            offset += 2;
            break;
        case 31:
            // We subtract when checking offset to avoid possible overflow
            if (offset > mmdb->data_section_size - 3) {
                DEBUG_MSGF("String end (%d, case 31) past data section (%d)",
                           offset,
                           mmdb->data_section_size);
                return MMDB_INVALID_DATA_ERROR;
            }
            size = 65821 + get_uint24(&mem[offset]);
            offset += 3;
            break;
        default:
            break;
    }

    DEBUG_MSGF("Size: %i", size);

    if (type == MMDB_DATA_TYPE_MAP || type == MMDB_DATA_TYPE_ARRAY) {
        entry_data->data_size = size;
        entry_data->offset_to_next = offset;
        return MMDB_SUCCESS;
    }

    if (type == MMDB_DATA_TYPE_BOOLEAN) {
        entry_data->boolean = size ? true : false;
        entry_data->data_size = 0;
        entry_data->offset_to_next = offset;
        DEBUG_MSGF("boolean value: %s", entry_data->boolean ? "true" : "false");
        return MMDB_SUCCESS;
    }

    // Check that the data doesn't extend past the end of the memory
    // buffer and that the calculation in doing this did not underflow.
    if (offset > mmdb->data_section_size - size ||
        mmdb->data_section_size < size) {
        DEBUG_MSGF("Data end (%d) past data section (%d)",
                   offset + size,
                   mmdb->data_section_size);
        return MMDB_INVALID_DATA_ERROR;
    }

    if (type == MMDB_DATA_TYPE_UINT16) {
        if (size > 2) {
            DEBUG_MSGF("uint16 of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
        entry_data->uint16 = (uint16_t)get_uintX(&mem[offset], (int)size);
        DEBUG_MSGF("uint16 value: %u", entry_data->uint16);
    } else if (type == MMDB_DATA_TYPE_UINT32) {
        if (size > 4) {
            DEBUG_MSGF("uint32 of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
        entry_data->uint32 = (uint32_t)get_uintX(&mem[offset], (int)size);
        DEBUG_MSGF("uint32 value: %u", entry_data->uint32);
    } else if (type == MMDB_DATA_TYPE_INT32) {
        if (size > 4) {
            DEBUG_MSGF("int32 of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
        entry_data->int32 = get_sintX(&mem[offset], (int)size);
        DEBUG_MSGF("int32 value: %i", entry_data->int32);
    } else if (type == MMDB_DATA_TYPE_UINT64) {
        if (size > 8) {
            DEBUG_MSGF("uint64 of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
        entry_data->uint64 = get_uintX(&mem[offset], (int)size);
        DEBUG_MSGF("uint64 value: %" PRIu64, entry_data->uint64);
    } else if (type == MMDB_DATA_TYPE_UINT128) {
        if (size > 16) {
            DEBUG_MSGF("uint128 of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
#if MMDB_UINT128_IS_BYTE_ARRAY
        memset(entry_data->uint128, 0, 16);
        if (size > 0) {
            memcpy(entry_data->uint128 + 16 - size, &mem[offset], size);
        }
#else
        entry_data->uint128 = get_uint128(&mem[offset], (int)size);
#endif
    } else if (type == MMDB_DATA_TYPE_FLOAT) {
        if (size != 4) {
            DEBUG_MSGF("float of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
        size = 4;
        entry_data->float_value = get_ieee754_float(&mem[offset]);
        DEBUG_MSGF("float value: %f", entry_data->float_value);
    } else if (type == MMDB_DATA_TYPE_DOUBLE) {
        if (size != 8) {
            DEBUG_MSGF("double of size %d", size);
            return MMDB_INVALID_DATA_ERROR;
        }
        size = 8;
        entry_data->double_value = get_ieee754_double(&mem[offset]);
        DEBUG_MSGF("double value: %f", entry_data->double_value);
    } else if (type == MMDB_DATA_TYPE_UTF8_STRING) {
        entry_data->utf8_string = size == 0 ? "" : (char const *)&mem[offset];
        entry_data->data_size = size;
#ifdef MMDB_DEBUG
        char *string =
            mmdb_strndup(entry_data->utf8_string, size > 50 ? 50 : size);
        if (NULL == string) {
            abort();
        }
        DEBUG_MSGF("string value: %s", string);
        free(string);
#endif
    } else if (type == MMDB_DATA_TYPE_BYTES) {
        entry_data->bytes = &mem[offset];
        entry_data->data_size = size;
    }

    entry_data->offset_to_next = offset + size;

    return MMDB_SUCCESS;
}

static int get_ext_type(int raw_ext_type) { return 7 + raw_ext_type; }

static uint32_t
get_ptr_from(uint8_t ctrl, uint8_t const *const ptr, int ptr_size) {
    uint32_t new_offset;
    switch (ptr_size) {
        case 1:
            new_offset = (uint32_t)((ctrl & 7) << 8) + ptr[0];
            break;
        case 2:
            new_offset = 2048 + (uint32_t)((ctrl & 7) << 16) +
                         (uint32_t)(ptr[0] << 8) + ptr[1];
            break;
        case 3:
            new_offset =
                2048 + 524288 + (uint32_t)((ctrl & 7) << 24) + get_uint24(ptr);
            break;
        case 4:
        default:
            new_offset = get_uint32(ptr);
            break;
    }
    return new_offset;
}

int MMDB_get_metadata_as_entry_data_list(
    const MMDB_s *const mmdb, MMDB_entry_data_list_s **const entry_data_list) {
    MMDB_s metadata_db = make_fake_metadata_db(mmdb);

    MMDB_entry_s metadata_start = {.mmdb = &metadata_db, .offset = 0};

    return MMDB_get_entry_data_list(&metadata_start, entry_data_list);
}

int MMDB_get_entry_data_list(MMDB_entry_s *start,
                             MMDB_entry_data_list_s **const entry_data_list) {
    *entry_data_list = NULL;

    size_t const maximum_values = (size_t)(MAXIMUM_DATA_STRUCTURE_VALUES);
    MMDB_data_pool_s *const pool =
        data_pool_new(MMDB_POOL_INIT_SIZE, maximum_values);
    if (!pool) {
        return MMDB_OUT_OF_MEMORY_ERROR;
    }

    decode_state_s decode_state = {0};
    MMDB_entry_data_list_s *list = NULL;
    int status = alloc_entry_data_list(pool, &decode_state, &list);
    if (MMDB_SUCCESS != status) {
        data_pool_destroy(pool);
        return status;
    }

    status = get_entry_data_list(
        start->mmdb, start->offset, list, pool, &decode_state, 0);
    if (MMDB_SUCCESS != status) {
        data_pool_destroy(pool);
        return status;
    }

    *entry_data_list = data_pool_to_list(pool);
    if (!*entry_data_list) {
        data_pool_destroy(pool);
        return MMDB_OUT_OF_MEMORY_ERROR;
    }

    return status;
}

static int
alloc_entry_data_list(MMDB_data_pool_s *const pool,
                      decode_state_s *const decode_state,
                      MMDB_entry_data_list_s **const entry_data_list) {
    size_t const maximum_values = (size_t)(MAXIMUM_DATA_STRUCTURE_VALUES);
    if (decode_state->values >= maximum_values) {
        DEBUG_MSG("reached the maximum number of data structure values");
        return MMDB_DECODER_LIMIT_ERROR;
    }

    *entry_data_list = data_pool_alloc(pool);
    if (!*entry_data_list) {
        return MMDB_OUT_OF_MEMORY_ERROR;
    }
    decode_state->values++;
    return MMDB_SUCCESS;
}

static int get_entry_data_list(const MMDB_s *const mmdb,
                               uint32_t offset,
                               MMDB_entry_data_list_s *const entry_data_list,
                               MMDB_data_pool_s *const pool,
                               decode_state_s *const decode_state,
                               int depth) {
    if (depth >= MAXIMUM_DATA_STRUCTURE_DEPTH) {
        DEBUG_MSG("reached the maximum data structure depth");
        return MMDB_DECODER_LIMIT_ERROR;
    }
    depth++;
    CHECKED_DECODE_ONE(mmdb, offset, &entry_data_list->entry_data);

    switch (entry_data_list->entry_data.type) {
        case MMDB_DATA_TYPE_POINTER: {
            uint32_t next_offset = entry_data_list->entry_data.offset_to_next;
            uint32_t last_offset;
            CHECKED_DECODE_ONE(mmdb,
                               last_offset =
                                   entry_data_list->entry_data.pointer,
                               &entry_data_list->entry_data);

            /* Pointers to pointers are illegal under the spec */
            if (entry_data_list->entry_data.type == MMDB_DATA_TYPE_POINTER) {
                DEBUG_MSG("pointer points to another pointer");
                return MMDB_INVALID_DATA_ERROR;
            }

            if (entry_data_list->entry_data.type == MMDB_DATA_TYPE_ARRAY ||
                entry_data_list->entry_data.type == MMDB_DATA_TYPE_MAP) {

                int status = get_entry_data_list(mmdb,
                                                 last_offset,
                                                 entry_data_list,
                                                 pool,
                                                 decode_state,
                                                 depth);
                if (MMDB_SUCCESS != status) {
                    DEBUG_MSG("get_entry_data_list on pointer failed.");
                    return status;
                }
            }
            entry_data_list->entry_data.offset_to_next = next_offset;
        } break;
        case MMDB_DATA_TYPE_ARRAY: {
            uint32_t array_size = entry_data_list->entry_data.data_size;
            uint32_t array_offset = entry_data_list->entry_data.offset_to_next;
            /* Each array element needs at least 1 byte. */
            if (array_offset > mmdb->data_section_size ||
                array_size > mmdb->data_section_size - array_offset) {
                DEBUG_MSG("array size exceeds remaining data section");
                return MMDB_INVALID_DATA_ERROR;
            }
            while (array_size-- > 0) {
                MMDB_entry_data_list_s *entry_data_list_to = NULL;
                int status = alloc_entry_data_list(
                    pool, decode_state, &entry_data_list_to);
                if (MMDB_SUCCESS != status) {
                    return status;
                }

                status = get_entry_data_list(mmdb,
                                             array_offset,
                                             entry_data_list_to,
                                             pool,
                                             decode_state,
                                             depth);
                if (MMDB_SUCCESS != status) {
                    DEBUG_MSG("get_entry_data_list on array element failed.");
                    return status;
                }

                array_offset = entry_data_list_to->entry_data.offset_to_next;
            }
            entry_data_list->entry_data.offset_to_next = array_offset;

        } break;
        case MMDB_DATA_TYPE_MAP: {
            uint32_t size = entry_data_list->entry_data.data_size;

            offset = entry_data_list->entry_data.offset_to_next;
            /* Each map entry needs at least a key and a value (1 byte each). */
            if (offset > mmdb->data_section_size ||
                size > (mmdb->data_section_size - offset) / 2) {
                DEBUG_MSG("map size exceeds remaining data section");
                return MMDB_INVALID_DATA_ERROR;
            }
            while (size-- > 0) {
                MMDB_entry_data_list_s *list_key = NULL;
                int status =
                    alloc_entry_data_list(pool, decode_state, &list_key);
                if (MMDB_SUCCESS != status) {
                    return status;
                }

                status = get_entry_data_list(
                    mmdb, offset, list_key, pool, decode_state, depth);
                if (MMDB_SUCCESS != status) {
                    DEBUG_MSG("get_entry_data_list on map key failed.");
                    return status;
                }

                offset = list_key->entry_data.offset_to_next;

                MMDB_entry_data_list_s *list_value = NULL;
                status = alloc_entry_data_list(pool, decode_state, &list_value);
                if (MMDB_SUCCESS != status) {
                    return status;
                }

                status = get_entry_data_list(
                    mmdb, offset, list_value, pool, decode_state, depth);
                if (MMDB_SUCCESS != status) {
                    DEBUG_MSG("get_entry_data_list on map element failed.");
                    return status;
                }
                offset = list_value->entry_data.offset_to_next;
            }
            entry_data_list->entry_data.offset_to_next = offset;
        } break;
        default:
            break;
    }

    // Charge the copied payload. Only string and bytes carry a variable-length
    // payload that a caller copies. Integers are size-validated and tiny,
    // floats are fixed width, and container data_size is an element count, not
    // bytes. Pointers have been resolved to their target above, so a pointer to
    // a string is charged here as the string. This runs once per node, so a
    // fan-out that references one large value many times is charged each time.
    // Check before adding so even an overridden maximum cannot make the
    // uint64 counter wrap.
    if (entry_data_list->entry_data.type == MMDB_DATA_TYPE_UTF8_STRING ||
        entry_data_list->entry_data.type == MMDB_DATA_TYPE_BYTES) {
        uint64_t const maximum_bytes = (uint64_t)(MAXIMUM_DATA_STRUCTURE_BYTES);
        uint64_t const data_size = entry_data_list->entry_data.data_size;
        if (data_size > maximum_bytes ||
            decode_state->bytes > maximum_bytes - data_size) {
            DEBUG_MSG("reached the maximum data structure bytes");
            return MMDB_DECODER_LIMIT_ERROR;
        }
        decode_state->bytes += data_size;
    }

    return MMDB_SUCCESS;
}

#ifndef __has_builtin
    #define __has_builtin(x) 0
#endif

static inline uint32_t mmdb_bswap32(uint32_t x) {
#if defined(_MSC_VER)
    return _byteswap_ulong(x);
#elif __has_builtin(__builtin_bswap32)
    return __builtin_bswap32(x);
#else
    x = ((x << 8) & 0xFF00FF00) | ((x >> 8) & 0x00FF00FF);
    return (x << 16) | (x >> 16);
#endif
}

static inline uint64_t mmdb_bswap64(uint64_t x) {
#if defined(_MSC_VER)
    return _byteswap_uint64(x);
#elif __has_builtin(__builtin_bswap64)
    return __builtin_bswap64(x);
#else
    x = ((x & 0x00000000FFFFFFFFULL) << 32) |
        ((x & 0xFFFFFFFF00000000ULL) >> 32);
    x = ((x & 0x0000FFFF0000FFFFULL) << 16) |
        ((x & 0xFFFF0000FFFF0000ULL) >> 16);
    return ((x & 0x00FF00FF00FF00FFULL) << 8) |
           ((x & 0xFF00FF00FF00FF00ULL) >> 8);
#endif
}

static float get_ieee754_float(const uint8_t *restrict p) {
    float f;
    uint32_t i;

    memcpy(&i, p, sizeof(uint32_t));

#if MMDB_LITTLE_ENDIAN
    i = mmdb_bswap32(i);
#endif

    memcpy(&f, &i, sizeof(float));

    return f;
}

static double get_ieee754_double(const uint8_t *restrict p) {
    double d;
    uint64_t i;

    memcpy(&i, p, sizeof(uint64_t));

#if MMDB_LITTLE_ENDIAN
    i = mmdb_bswap64(i);
#endif

    memcpy(&d, &i, sizeof(double));

    return d;
}

static uint32_t get_uint32(const uint8_t *p) {
    return p[0] * 16777216U + p[1] * 65536 + p[2] * 256 + p[3];
}

static uint32_t get_uint24(const uint8_t *p) {
    return p[0] * 65536U + p[1] * 256 + p[2];
}

static uint32_t get_uint16(const uint8_t *p) { return p[0] * 256U + p[1]; }

static uint64_t get_uintX(const uint8_t *p, int length) {
    uint64_t value = 0;
    while (length-- > 0) {
        value <<= 8;
        value += *p++;
    }
    return value;
}

static int32_t get_sintX(const uint8_t *p, int length) {
    return (int32_t)get_uintX(p, length);
}

void MMDB_free_entry_data_list(MMDB_entry_data_list_s *const entry_data_list) {
    if (entry_data_list == NULL) {
        return;
    }
    data_pool_destroy(entry_data_list->pool);
}

void MMDB_close(MMDB_s *const mmdb) { free_mmdb_struct(mmdb); }

static void free_mmdb_struct(MMDB_s *const mmdb) {
    if (!mmdb) {
        return;
    }

    if (NULL != mmdb->filename) {
#if defined(__clang__)
    // This is a const char * that we need to free, which isn't valid. However
    // it would mean changing the public API to fix this.
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wcast-qual"
#endif
        FREE_AND_SET_NULL(mmdb->filename);
#if defined(__clang__)
    #pragma clang diagnostic pop
#endif
    }
    if (NULL != mmdb->file_content) {
#ifdef _WIN32
        UnmapViewOfFile(mmdb->file_content);
        /* Winsock is only initialized if open was successful so we only have
         * to cleanup then. */
        WSACleanup();
#else
    #if defined(__clang__)
        // This is a const char * that we need to free, which isn't valid.
        // However it would mean changing the public API to fix this.
        #pragma clang diagnostic push
        #pragma clang diagnostic ignored "-Wcast-qual"
    #endif
        munmap((void *)mmdb->file_content, (size_t)mmdb->file_size);
    #if defined(__clang__)
        #pragma clang diagnostic pop
    #endif
#endif
        mmdb->file_content = NULL;
        mmdb->file_size = 0;
        mmdb->data_section = NULL;
        mmdb->data_section_size = 0;
        mmdb->metadata_section = NULL;
        mmdb->metadata_section_size = 0;
    }

    if (NULL != mmdb->metadata.database_type) {
#if defined(__clang__)
    // This is a const char * that we need to free, which isn't valid. However
    // it would mean changing the public API to fix this.
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wcast-qual"
#endif
        FREE_AND_SET_NULL(mmdb->metadata.database_type);
#if defined(__clang__)
    #pragma clang diagnostic pop
#endif
    }

    free_languages_metadata(mmdb);
    free_descriptions_metadata(mmdb);
}

static void free_languages_metadata(MMDB_s *mmdb) {
    if (!mmdb->metadata.languages.names) {
        return;
    }

    for (size_t i = 0; i < mmdb->metadata.languages.count; i++) {
#if defined(__clang__)
    // This is a const char * that we need to free, which isn't valid. However
    // it would mean changing the public API to fix this.
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wcast-qual"
#endif
        FREE_AND_SET_NULL(mmdb->metadata.languages.names[i]);
#if defined(__clang__)
    #pragma clang diagnostic pop
#endif
    }
    FREE_AND_SET_NULL(mmdb->metadata.languages.names);
    mmdb->metadata.languages.count = 0;
}

static void free_descriptions_metadata(MMDB_s *mmdb) {
    if (!mmdb->metadata.description.count) {
        return;
    }

    for (size_t i = 0; i < mmdb->metadata.description.count; i++) {
        if (NULL != mmdb->metadata.description.descriptions[i]) {
            if (NULL != mmdb->metadata.description.descriptions[i]->language) {
#if defined(__clang__)
    // This is a const char * that we need to free, which isn't valid. However
    // it would mean changing the public API to fix this.
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wcast-qual"
#endif
                FREE_AND_SET_NULL(
                    mmdb->metadata.description.descriptions[i]->language);
#if defined(__clang__)
    #pragma clang diagnostic pop
#endif
            }

            if (NULL !=
                mmdb->metadata.description.descriptions[i]->description) {
#if defined(__clang__)
    // This is a const char * that we need to free, which isn't valid. However
    // it would mean changing the public API to fix this.
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wcast-qual"
#endif
                FREE_AND_SET_NULL(
                    mmdb->metadata.description.descriptions[i]->description);
#if defined(__clang__)
    #pragma clang diagnostic pop
#endif
            }
            FREE_AND_SET_NULL(mmdb->metadata.description.descriptions[i]);
        }
    }

    FREE_AND_SET_NULL(mmdb->metadata.description.descriptions);
    mmdb->metadata.description.count = 0;
}

const char *MMDB_lib_version(void) { return PACKAGE_VERSION; }

int MMDB_dump_entry_data_list(FILE *const stream,
                              MMDB_entry_data_list_s *const entry_data_list,
                              int indent) {
    int status;
    dump_entry_data_list(stream, entry_data_list, indent, &status);
    return status;
}

static MMDB_entry_data_list_s *
dump_entry_data_list(FILE *stream,
                     MMDB_entry_data_list_s *entry_data_list,
                     int indent,
                     int *status) {
    switch (entry_data_list->entry_data.type) {
        case MMDB_DATA_TYPE_MAP: {
            uint32_t size = entry_data_list->entry_data.data_size;

            print_indentation(stream, indent);
            fprintf(stream, "{\n");
            indent += 2;

            for (entry_data_list = entry_data_list->next;
                 size && entry_data_list;
                 size--) {

                if (MMDB_DATA_TYPE_UTF8_STRING !=
                    entry_data_list->entry_data.type) {
                    *status = MMDB_INVALID_DATA_ERROR;
                    return NULL;
                }
                char *key =
                    mmdb_strndup(entry_data_list->entry_data.utf8_string,
                                 entry_data_list->entry_data.data_size);
                if (NULL == key) {
                    *status = MMDB_OUT_OF_MEMORY_ERROR;
                    return NULL;
                }

                print_indentation(stream, indent);
                fprintf(stream, "\"%s\": \n", key);
                free(key);

                entry_data_list = entry_data_list->next;
                entry_data_list = dump_entry_data_list(
                    stream, entry_data_list, indent + 2, status);

                if (MMDB_SUCCESS != *status) {
                    return NULL;
                }
            }

            indent -= 2;
            print_indentation(stream, indent);
            fprintf(stream, "}\n");
        } break;
        case MMDB_DATA_TYPE_ARRAY: {
            uint32_t size = entry_data_list->entry_data.data_size;

            print_indentation(stream, indent);
            fprintf(stream, "[\n");
            indent += 2;

            for (entry_data_list = entry_data_list->next;
                 size && entry_data_list;
                 size--) {
                entry_data_list = dump_entry_data_list(
                    stream, entry_data_list, indent, status);
                if (MMDB_SUCCESS != *status) {
                    return NULL;
                }
            }

            indent -= 2;
            print_indentation(stream, indent);
            fprintf(stream, "]\n");
        } break;
        case MMDB_DATA_TYPE_UTF8_STRING: {
            char *string = mmdb_strndup(entry_data_list->entry_data.utf8_string,
                                        entry_data_list->entry_data.data_size);
            if (NULL == string) {
                *status = MMDB_OUT_OF_MEMORY_ERROR;
                return NULL;
            }
            print_indentation(stream, indent);
            fprintf(stream, "\"%s\" <utf8_string>\n", string);
            free(string);
            entry_data_list = entry_data_list->next;
        } break;
        case MMDB_DATA_TYPE_BYTES: {
            char *hex_string =
                bytes_to_hex(entry_data_list->entry_data.bytes,
                             entry_data_list->entry_data.data_size);

            if (NULL == hex_string) {
                *status = MMDB_OUT_OF_MEMORY_ERROR;
                return NULL;
            }

            print_indentation(stream, indent);
            fprintf(stream, "%s <bytes>\n", hex_string);
            free(hex_string);

            entry_data_list = entry_data_list->next;
        } break;
        case MMDB_DATA_TYPE_DOUBLE:
            print_indentation(stream, indent);
            fprintf(stream,
                    "%f <double>\n",
                    entry_data_list->entry_data.double_value);
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_FLOAT:
            print_indentation(stream, indent);
            fprintf(stream,
                    "%f <float>\n",
                    entry_data_list->entry_data.float_value);
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_UINT16:
            print_indentation(stream, indent);
            fprintf(
                stream, "%u <uint16>\n", entry_data_list->entry_data.uint16);
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_UINT32:
            print_indentation(stream, indent);
            fprintf(
                stream, "%u <uint32>\n", entry_data_list->entry_data.uint32);
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_BOOLEAN:
            print_indentation(stream, indent);
            fprintf(stream,
                    "%s <boolean>\n",
                    entry_data_list->entry_data.boolean ? "true" : "false");
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_UINT64:
            print_indentation(stream, indent);
            fprintf(stream,
                    "%" PRIu64 " <uint64>\n",
                    entry_data_list->entry_data.uint64);
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_UINT128:
            print_indentation(stream, indent);
#if MMDB_UINT128_IS_BYTE_ARRAY
            char *hex_string = bytes_to_hex(
                (uint8_t *)entry_data_list->entry_data.uint128, 16);
            if (NULL == hex_string) {
                *status = MMDB_OUT_OF_MEMORY_ERROR;
                return NULL;
            }
            fprintf(stream, "0x%s <uint128>\n", hex_string);
            free(hex_string);
#else
            uint64_t high = entry_data_list->entry_data.uint128 >> 64;
            uint64_t low = (uint64_t)entry_data_list->entry_data.uint128;
            fprintf(stream,
                    "0x%016" PRIX64 "%016" PRIX64 " <uint128>\n",
                    high,
                    low);
#endif
            entry_data_list = entry_data_list->next;
            break;
        case MMDB_DATA_TYPE_INT32:
            print_indentation(stream, indent);
            fprintf(stream, "%d <int32>\n", entry_data_list->entry_data.int32);
            entry_data_list = entry_data_list->next;
            break;
        default:
            *status = MMDB_INVALID_DATA_ERROR;
            return NULL;
    }

    *status = MMDB_SUCCESS;
    return entry_data_list;
}

static void print_indentation(FILE *stream, int i) {
    char buffer[1024];
    int size = i < 0 ? 0 : (i >= 1024 ? 1023 : i);
    memset(buffer, 32, (size_t)size);
    buffer[size] = '\0';
    fputs(buffer, stream);
}

static char *bytes_to_hex(uint8_t const *bytes, uint32_t size) {
    char *hex_string;
    MAYBE_CHECK_SIZE_OVERFLOW(size, SIZE_MAX / 2 - 1, NULL);

    hex_string = calloc((size * 2) + 1, sizeof(char));
    if (NULL == hex_string) {
        return NULL;
    }

    for (uint32_t i = 0; i < size; i++) {
        sprintf(hex_string + (2 * i), "%02X", bytes[i]);
    }

    return hex_string;
}

const char *MMDB_strerror(int error_code) {
    switch (error_code) {
        case MMDB_SUCCESS:
            return "Success (not an error)";
        case MMDB_FILE_OPEN_ERROR:
            return "Error opening the specified MaxMind DB file";
        case MMDB_CORRUPT_SEARCH_TREE_ERROR:
            return "The MaxMind DB file's search tree is corrupt";
        case MMDB_INVALID_METADATA_ERROR:
            return "The MaxMind DB file contains invalid metadata";
        case MMDB_IO_ERROR:
            return "An attempt to read data from the MaxMind DB file failed";
        case MMDB_OUT_OF_MEMORY_ERROR:
            return "A memory allocation call failed";
        case MMDB_UNKNOWN_DATABASE_FORMAT_ERROR:
            return "The MaxMind DB file is in a format this library can't "
                   "handle (unknown record size or binary format version)";
        case MMDB_INVALID_DATA_ERROR:
            return "The MaxMind DB file's data section contains bad data "
                   "(unknown data type or corrupt data)";
        case MMDB_INVALID_LOOKUP_PATH_ERROR:
            return "The lookup path contained an invalid value (like a "
                   "negative integer for an array index)";
        case MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR:
            return "The lookup path does not match the data (key that doesn't "
                   "exist, array index bigger than the array, expected array "
                   "or map where none exists)";
        case MMDB_INVALID_NODE_NUMBER_ERROR:
            return "The MMDB_read_node function was called with a node number "
                   "that does not exist in the search tree";
        case MMDB_IPV6_LOOKUP_IN_IPV4_DATABASE_ERROR:
            return "You attempted to look up an IPv6 address in an IPv4-only "
                   "database";
        case MMDB_INVALID_NETWORK_ADDRESS_ERROR:
            return "The sockaddr family is unsupported; only AF_INET and "
                   "AF_INET6 are accepted";
        case MMDB_DECODER_LIMIT_ERROR:
            return "The decoded data structure exceeds the configured resource "
                   "limits";
        default:
            return "Unknown error code";
    }
}
