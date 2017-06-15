#ifndef _ma_pvio_h_
#define _ma_pvio_h_
#define cio_defined

#ifdef HAVE_TLS
#include <ma_tls.h>
#else
#define MARIADB_TLS void
#endif

#define PVIO_SET_ERROR if (pvio->set_error) \
                        pvio->set_error

#define PVIO_READ_AHEAD_CACHE_SIZE 16384
#define PVIO_READ_AHEAD_CACHE_MIN_SIZE 2048
#define PVIO_EINTR_TRIES 2

struct st_ma_pvio_methods;
typedef struct st_ma_pvio_methods PVIO_METHODS;

#define IS_PVIO_ASYNC(a) \
  ((a)->mysql && (a)->mysql->options.extension && (a)->mysql->options.extension->async_context)

#define IS_PVIO_ASYNC_ACTIVE(a) \
  (IS_PVIO_ASYNC(a)&& (a)->mysql->options.extension->async_context->active)

#define IS_MYSQL_ASYNC(a) \
  ((a)->options.extension && (a)->options.extension->async_context)

#define IS_MYSQL_ASYNC_ACTIVE(a) \
  (IS_MYSQL_ASYNC(a)&& (a)->options.extension->async_context->active)

enum enum_pvio_timeout {
  PVIO_CONNECT_TIMEOUT= 0,
  PVIO_READ_TIMEOUT,
  PVIO_WRITE_TIMEOUT 
};

enum enum_pvio_io_event
{
  VIO_IO_EVENT_READ,
  VIO_IO_EVENT_WRITE,
  VIO_IO_EVENT_CONNECT
};

enum enum_pvio_type {
  PVIO_TYPE_UNIXSOCKET= 0,
  PVIO_TYPE_SOCKET,
  PVIO_TYPE_NAMEDPIPE,
  PVIO_TYPE_SHAREDMEM,
};

enum enum_pvio_operation {
  PVIO_READ= 0,
  PVIO_WRITE=1
};

#define SHM_DEFAULT_NAME "MYSQL"

struct st_pvio_callback;

typedef struct st_pvio_callback {
  void (*callback)(MYSQL *mysql, uchar *buffer, size_t size);
  struct st_pvio_callback *next;
} PVIO_CALLBACK;

struct st_ma_pvio {
  void *data;
  /* read ahead cache */
  uchar *cache;
  uchar *cache_pos;
  size_t cache_size;
  enum enum_pvio_type type;
  int timeout[3];
  int ssl_type;  /* todo: change to enum (ssl plugins) */
  MARIADB_TLS *ctls;
  MYSQL *mysql;
  PVIO_METHODS *methods;
  void (*set_error)(MYSQL *mysql, unsigned int error_nr, const char *sqlstate, const char *format, ...);
  void (*callback)(MARIADB_PVIO *pvio, my_bool is_read, const uchar *buffer, size_t length);
};

typedef struct st_ma_pvio_cinfo
{
  const char *host;
  const char *unix_socket;
  int port;
  enum enum_pvio_type type;
  MYSQL *mysql;
} MA_PVIO_CINFO;

struct st_ma_pvio_methods
{
  my_bool (*set_timeout)(MARIADB_PVIO *pvio, enum enum_pvio_timeout type, int timeout);
  int (*get_timeout)(MARIADB_PVIO *pvio, enum enum_pvio_timeout type);
  ssize_t (*read)(MARIADB_PVIO *pvio, uchar *buffer, size_t length);
  ssize_t (*async_read)(MARIADB_PVIO *pvio, uchar *buffer, size_t length);
  ssize_t (*write)(MARIADB_PVIO *pvio, const uchar *buffer, size_t length);
  ssize_t (*async_write)(MARIADB_PVIO *pvio, const uchar *buffer, size_t length);
  int (*wait_io_or_timeout)(MARIADB_PVIO *pvio, my_bool is_read, int timeout);
  my_bool (*blocking)(MARIADB_PVIO *pvio, my_bool value, my_bool *old_value);
  my_bool (*connect)(MARIADB_PVIO *pvio, MA_PVIO_CINFO *cinfo);
  my_bool (*close)(MARIADB_PVIO *pvio);
  int (*fast_send)(MARIADB_PVIO *pvio);
  int (*keepalive)(MARIADB_PVIO *pvio);
  my_bool (*get_handle)(MARIADB_PVIO *pvio, void *handle);
  my_bool (*is_blocking)(MARIADB_PVIO *pvio);
  my_bool (*is_alive)(MARIADB_PVIO *pvio);
  my_bool (*has_data)(MARIADB_PVIO *pvio, ssize_t *data_len);
  int(*shutdown)(MARIADB_PVIO *pvio);
};

/* Function prototypes */
MARIADB_PVIO *ma_pvio_init(MA_PVIO_CINFO *cinfo);
void ma_pvio_close(MARIADB_PVIO *pvio);
ssize_t ma_pvio_cache_read(MARIADB_PVIO *pvio, uchar *buffer, size_t length);
ssize_t ma_pvio_read(MARIADB_PVIO *pvio, uchar *buffer, size_t length);
ssize_t ma_pvio_write(MARIADB_PVIO *pvio, const uchar *buffer, size_t length);
int ma_pvio_get_timeout(MARIADB_PVIO *pvio, enum enum_pvio_timeout type);
my_bool ma_pvio_set_timeout(MARIADB_PVIO *pvio, enum enum_pvio_timeout type, int timeout);
int ma_pvio_fast_send(MARIADB_PVIO *pvio);
int ma_pvio_keepalive(MARIADB_PVIO *pvio);
my_socket ma_pvio_get_socket(MARIADB_PVIO *pvio);
my_bool ma_pvio_is_blocking(MARIADB_PVIO *pvio);
my_bool ma_pvio_blocking(MARIADB_PVIO *pvio, my_bool block, my_bool *previous_mode);
my_bool ma_pvio_is_blocking(MARIADB_PVIO *pvio);
int ma_pvio_wait_io_or_timeout(MARIADB_PVIO *pvio, my_bool is_read, int timeout);
my_bool ma_pvio_connect(MARIADB_PVIO *pvio, MA_PVIO_CINFO *cinfo);
my_bool ma_pvio_is_alive(MARIADB_PVIO *pvio);
my_bool ma_pvio_get_handle(MARIADB_PVIO *pvio, void *handle);
my_bool ma_pvio_has_data(MARIADB_PVIO *pvio, ssize_t *length);

#endif /* _ma_pvio_h_ */
