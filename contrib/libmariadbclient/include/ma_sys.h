/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02111-1301, USA */

#ifndef _my_sys_h
#define _my_sys_h
#ifdef	__cplusplus
extern "C" {
#endif

#ifdef HAVE_AIOWAIT
#include <sys/asynch.h>			/* Used by record-cache */
typedef struct my_aio_result {
  aio_result_t result;
  int	       pending;
} my_aio_result;
#endif

#ifndef _mariadb_ctype_h
#include <mariadb_ctype.h>                    /* for MARIADB_CHARSET_INFO */
#endif

#include <stdarg.h>  

#define MYSYS_PROGRAM_USES_CURSES()  { ma_error_handler_hook = ma_message_curses;	mysys_uses_curses=1; }
#define MYSYS_PROGRAM_DONT_USE_CURSES()  { ma_error_handler_hook = ma_message_no_curses; mysys_uses_curses=0;}
#define MY_INIT(name);		{ ma_progname= name; ma_init(); }

#define MAXMAPS		(4)	/* Number of error message maps */
#define ERRMOD		(1000)	/* Max number of errors in a map */
#define ERRMSGSIZE	(SC_MAXWIDTH)	/* Max length of a error message */
#define NRERRBUFFS	(2)	/* Buffers for parameters */
#define MY_FILE_ERROR	((uint) ~0)

	/* General bitmaps for my_func's */
#define MY_FFNF		1	/* Fatal if file not found */
#define MY_FNABP	2	/* Fatal if not all bytes read/writen */
#define MY_NABP		4	/* Error if not all bytes read/writen */
#define MY_FAE		8	/* Fatal if any error */
#define MY_WME		16	/* Write message on error */
#define MY_WAIT_IF_FULL 32	/* Wait and try again if disk full error */
#define MY_RAID         64      /* Support for RAID (not the "Johnson&Johnson"-s one ;) */
#define MY_DONT_CHECK_FILESIZE 128	/* Option to init_io_cache() */
#define MY_LINK_WARNING 32	/* my_redel() gives warning if links */
#define MY_COPYTIME	64	/* my_redel() copys time */
#define MY_DELETE_OLD	256	/* my_create_with_symlink() */
#define MY_RESOLVE_LINK 128	/* my_realpath(); Only resolve links */
#define MY_HOLD_ORIGINAL_MODES 128  /* my_copy() holds to file modes */
#define MY_REDEL_MAKE_BACKUP 256
#define MY_SEEK_NOT_DONE 32	/* my_lock may have to do a seek */
#define MY_DONT_WAIT	64	/* my_lock() don't wait if can't lock */
#define MY_ZEROFILL	32	/* ma_malloc(), fill array with zero */
#define MY_ALLOW_ZERO_PTR 64	/* ma_realloc() ; zero ptr -> malloc */
#define MY_FREE_ON_ERROR 128	/* ma_realloc() ; Free old ptr on error */
#define MY_HOLD_ON_ERROR 256	/* ma_realloc() ; Return old ptr on error */
#define MY_THREADSAFE	128	/* pread/pwrite:  Don't allow interrupts */
#define MY_DONT_OVERWRITE_FILE 1024	/* my_copy; Don't overwrite file */

#define MY_CHECK_ERROR	1	/* Params to ma_end; Check open-close */
#define MY_GIVE_INFO	2	/* Give time info about process*/

#define ME_HIGHBYTE	8	/* Shift for colours */
#define ME_NOCUR	1	/* Don't use curses message */
#define ME_OLDWIN	2	/* Use old window */
#define ME_BELL		4	/* Ring bell then printing message */
#define ME_HOLDTANG	8	/* Don't delete last keys */
#define ME_WAITTOT	16	/* Wait for errtime secs of for a action */
#define ME_WAITTANG	32	/* Wait for a user action  */
#define ME_NOREFRESH	64	/* Dont refresh screen */
#define ME_NOINPUT	128	/* Dont use the input libary */
#define ME_COLOUR1	((1 << ME_HIGHBYTE))	/* Possibly error-colours */
#define ME_COLOUR2	((2 << ME_HIGHBYTE))
#define ME_COLOUR3	((3 << ME_HIGHBYTE))

	/* My seek flags */
#define MY_SEEK_SET	0
#define MY_SEEK_CUR	1
#define MY_SEEK_END	2

        /* My charsets_list flags */
#define MY_NO_SETS       0
#define MY_COMPILED_SETS 1      /* show compiled-in sets */
#define MY_CONFIG_SETS   2      /* sets that have a *.conf file */
#define MY_INDEX_SETS    4      /* all sets listed in the Index file */
#define MY_LOADED_SETS    8      /* the sets that are currently loaded */

	/* Some constants */
#define MY_WAIT_FOR_USER_TO_FIX_PANIC	60	/* in seconds */
#define MY_WAIT_GIVE_USER_A_MESSAGE	10	/* Every 10 times of prev */
#define MIN_COMPRESS_LENGTH		50	/* Don't compress small bl. */
#define KEYCACHE_BLOCK_SIZE		1024

	/* root_alloc flags */
#define MY_KEEP_PREALLOC	1

	/* defines when allocating data */

#define my_checkmalloc() (0)
#undef TERMINATE
#define TERMINATE(A) {}
#define QUICK_SAFEMALLOC
#define NORMAL_SAFEMALLOC
#define ma_malloc_ci(SZ,FLAG) ma_malloc( SZ, FLAG )
#define CALLER_INFO_PROTO   /* nothing */
#define CALLER_INFO         /* nothing */
#define ORIG_CALLER_INFO    /* nothing */

#ifdef HAVE_ALLOCA
#if defined(_AIX) && !defined(__GNUC__)
#pragma alloca
#endif /* _AIX */
#if defined(__GNUC__) && !defined(HAVE_ALLOCA_H)
#ifndef alloca
#define alloca __builtin_alloca
#endif
#endif /* GNUC */
#define my_alloca(SZ) alloca((size_t) (SZ))
#define my_afree(PTR) {}
#else
#define my_alloca(SZ) ma_malloc(SZ,MYF(0))
#define my_afree(PTR) ma_free(PTR)
#endif /* HAVE_ALLOCA */

#ifdef MSDOS
#ifdef __ZTC__
void * __CDECL halloc(long count,size_t length);
void   __CDECL hfree(void *ptr);
#endif
#if defined(USE_HALLOC)
#if defined(_VCM_) || defined(M_IC80386)
#undef USE_HALLOC
#endif
#endif
#ifdef USE_HALLOC
#define malloc(a) halloc((long) (a),1)
#define free(a) hfree(a)
#endif
#endif /* MSDOS */

#ifndef errno
#ifdef HAVE_ERRNO_AS_DEFINE
#include <errno.h>			/* errno is a define */
#else
extern int errno;			/* declare errno */
#endif
#endif
extern const char ** NEAR my_errmsg[];
extern char NEAR errbuff[NRERRBUFFS][ERRMSGSIZE];
/* tbr
extern int (*ma_error_handler_hook)(uint my_err, const char *str,myf MyFlags);
extern int (*fatal_ma_error_handler_hook)(uint my_err, const char *str,
				       myf MyFlags);
*/

/* charsets */
/* tbr
extern uint get_charset_number(const char *cs_name);
extern const char *get_charset_name(uint cs_number);
extern my_bool set_default_charset(uint cs, myf flags);
extern my_bool set_default_charset_by_name(const char *cs_name, myf flags);
extern void free_charsets(void);
extern char *list_charsets(myf want_flags); 
extern char *get_charsets_dir(char *buf);
*/
extern MARIADB_CHARSET_INFO *get_charset(uint cs_number, myf flags);
extern MARIADB_CHARSET_INFO *get_charset_by_name(const char *cs_name);
extern MARIADB_CHARSET_INFO *get_charset_by_nr(uint cs_number);

/* string functions */
char *ma_strmake(register char *dst, register const char *src, size_t length);

/* statistics */
#ifdef TBR
extern ulong	_my_cache_w_requests,_my_cache_write,_my_cache_r_requests,
		_my_cache_read;
extern ulong	 _my_blocks_used,_my_blocks_changed;
extern ulong	ma_file_opened,ma_stream_opened, ma_tmp_file_created;
extern my_bool	key_cache_inited;

					/* Point to current ma_message() */
extern void (*my_sigtstp_cleanup)(void),
					/* Executed before jump to shell */
	    (*my_sigtstp_restart)(void),
	    (*my_abort_hook)(int);
					/* Executed when comming from shell */
extern int NEAR ma_umask,		/* Default creation mask  */
	   NEAR ma_umask_dir,
	   NEAR my_recived_signals,	/* Signals we have got */
	   NEAR my_safe_to_handle_signal, /* Set when allowed to SIGTSTP */
	   NEAR ma_dont_interrupt;	/* call remember_intr when set */
extern my_bool NEAR mysys_uses_curses, ma_use_symdir;
extern size_t lCurMemory,lMaxMemory;	/* from safemalloc */

extern ulong	ma_default_record_cache_size;
extern my_bool NEAR ma_disable_locking,NEAR ma_disable_async_io,
               NEAR ma_disable_flush_key_blocks, NEAR ma_disable_symlinks;
extern char	wild_many,wild_one,wild_prefix;
extern const char *charsets_dir;
extern char *defaults_extra_file;
typedef struct wild_file_pack	/* Struct to hold info when selecting files */
{
  uint		wilds;		/* How many wildcards */
  uint		not_pos;	/* Start of not-theese-files */
  my_string	*wild;		/* Pointer to wildcards */
} WF_PACK;

struct my_rnd_struct {
  unsigned long seed1,seed2,max_value;
  double max_value_dbl;
};

#endif
typedef struct st_typelib {	/* Different types saved here */
  uint count;			/* How many types */
  const char *name;			/* Name of typelib */
  const char **type_names;
} TYPELIB;

enum cache_type {READ_CACHE,WRITE_CACHE,READ_FIFO,READ_NET,WRITE_NET};
enum flush_type { FLUSH_KEEP, FLUSH_RELEASE, FLUSH_IGNORE_CHANGED,
		  FLUSH_FORCE_WRITE};

typedef struct st_record_cache	/* Used when cacheing records */
{
  File file;
  int	rc_seek,error,inited;
  uint	rc_length,read_length,reclength;
  my_off_t rc_record_pos,end_of_file;
  unsigned char	*rc_buff,*rc_buff2,*rc_pos,*rc_end,*rc_request_pos;
#ifdef HAVE_AIOWAIT
  int	use_async_io;
  my_aio_result aio_result;
#endif
  enum cache_type type;
} RECORD_CACHE;

/*
enum file_type { UNOPEN = 0, FILE_BY_OPEN, FILE_BY_CREATE,
		   STREAM_BY_FOPEN, STREAM_BY_FDOPEN, FILE_BY_MKSTEMP };

extern struct ma_file_info
{
  my_string		name;
  enum file_type	type;
#if defined(THREAD) && !defined(HAVE_PREAD)  
  pthread_mutex_t	mutex;
#endif
} ma_file_info[MY_NFILE];
*/

typedef struct st_dynamic_array {
  char *buffer;
  uint elements,max_element;
  uint alloc_increment;
  uint size_of_element;
} DYNAMIC_ARRAY;

typedef struct st_dynamic_string {
  char *str;
  size_t length,max_length,alloc_increment;
} DYNAMIC_STRING;


typedef struct st_io_cache		/* Used when cacheing files */
{
  my_off_t pos_in_file,end_of_file;
  unsigned char	*rc_pos,*rc_end,*buffer,*rc_request_pos;
  int (*read_function)(struct st_io_cache *,unsigned char *,uint);
  char *file_name;			/* if used with 'open_cached_file' */
  char *dir,*prefix;
  File file;
  int	seek_not_done,error;
  uint	buffer_length,read_length;
  myf	myflags;			/* Flags used to my_read/my_write */
  enum cache_type type;
#ifdef HAVE_AIOWAIT
  uint inited;
  my_off_t aio_read_pos;
  my_aio_result aio_result;
#endif
} IO_CACHE;

typedef int (*qsort2_cmp)(const void *, const void *, const void *);

	/* defines for mf_iocache */

	/* Test if buffer is inited */
#define my_b_clear(info) (info)->buffer=0
#define my_b_inited(info) (info)->buffer
#define my_b_EOF INT_MIN

#define my_b_read(info,Buffer,Count) \
  ((info)->rc_pos + (Count) <= (info)->rc_end ?\
   (memcpy(Buffer,(info)->rc_pos,(size_t) (Count)), \
    ((info)->rc_pos+=(Count)),0) :\
   (*(info)->read_function)((info),Buffer,Count))

#define my_b_get(info) \
  ((info)->rc_pos != (info)->rc_end ?\
   ((info)->rc_pos++, (int) (uchar) (info)->rc_pos[-1]) :\
   _my_b_get(info))

#define my_b_write(info,Buffer,Count) \
  ((info)->rc_pos + (Count) <= (info)->rc_end ?\
   (memcpy((info)->rc_pos,Buffer,(size_t) (Count)), \
    ((info)->rc_pos+=(Count)),0) :\
   _my_b_write(info,Buffer,Count))

	/* my_b_write_byte dosn't have any err-check */
#define my_b_write_byte(info,chr) \
  (((info)->rc_pos < (info)->rc_end) ?\
   ((*(info)->rc_pos++)=(chr)) :\
   (_my_b_write(info,0,0) , ((*(info)->rc_pos++)=(chr))))

#define my_b_fill_cache(info) \
  (((info)->rc_end=(info)->rc_pos),(*(info)->read_function)(info,0,0))

#define my_b_tell(info) ((info)->pos_in_file + \
			 ((info)->rc_pos - (info)->rc_request_pos))

#define my_b_bytes_in_cache(info) ((uint) ((info)->rc_end - (info)->rc_pos))

typedef struct st_changeable_var {
  const char *name;			/* Name of variable */
  long *varptr;				/* Pointer to variable */
  long def_value,			/* Default value */
       min_value,			/* Min allowed value */
       max_value,			/* Max allowed value */
       sub_size,			/* Subtract this from given value */
       block_size;			/* Value should be a mult. of this */
} CHANGEABLE_VAR;


/* structs for ma_alloc_root */

#ifndef ST_MA_USED_MEM_DEFINED
#define ST_MA_USED_MEM_DEFINED
typedef struct st_ma_used_mem {   /* struct for once_alloc */
  struct st_ma_used_mem *next;    /* Next block in use */
  size_t left;                 /* memory left in block  */
  size_t size;                 /* Size of block */
} MA_USED_MEM;

typedef struct st_ma_mem_root {
  MA_USED_MEM *free;
  MA_USED_MEM *used;
  MA_USED_MEM *pre_alloc;
  size_t min_malloc;
  size_t block_size;
  unsigned int block_num;
  unsigned int first_block_usage;
  void (*error_handler)(void);
} MA_MEM_ROOT;
#endif

	/* Prototypes for mysys and my_func functions */

extern void * _mymalloc(size_t uSize,const char *sFile,
		      uint uLine, myf MyFlag);
extern void * _myrealloc(void * pPtr,size_t uSize,const char *sFile,
		       uint uLine, myf MyFlag);
extern void *ma_multi_malloc(myf MyFlags, ...);
extern void _myfree(void * pPtr,const char *sFile,uint uLine, myf MyFlag);
extern int _sanity(const char *sFile,unsigned int uLine);
#ifndef TERMINATE
extern void TERMINATE(FILE *file);
#endif
extern void ma_init_glob_errs(void);
extern FILE *my_fopen(const char *FileName,int Flags,myf MyFlags);
extern FILE *my_fdopen(File Filedes,const char *name, int Flags,myf MyFlags);
extern int my_fclose(FILE *fd,myf MyFlags);
extern int my_chsize(File fd,my_off_t newlength,myf MyFlags);
extern int ma_error _VARARGS((int nr,myf MyFlags, ...));
extern int ma_printf_error _VARARGS((uint my_err, const char *format,
				     myf MyFlags, ...)
				    __attribute__ ((format (printf, 2, 4))));
extern int ma_vsnprintf( char *str, size_t n,
                                const char *format, va_list ap );
extern int ma_snprintf(char* to, size_t n, const char* fmt, ...);
extern int ma_message(uint my_err, const char *str,myf MyFlags);
extern int _mariadb_stderr_out(unsigned int error, const char *errmsg, myf MyFlags);

extern void ma_init(void);
extern void ma_end(int infoflag);
extern int my_redel(const char *from, const char *to, int MyFlags);
extern int my_copystat(const char *from, const char *to, int MyFlags);
extern my_string my_filename(File fd);

#ifndef THREAD
extern void dont_break(void);
extern void allow_break(void);
#else
#define dont_break()
#define allow_break()
#endif

extern void my_remember_signal(int signal_number,sig_handler (*func)(int));
extern void caseup(my_string str,uint length);
extern void casedn(my_string str,uint length);
extern void caseup_str(my_string str);
extern void casedn_str(my_string str);
extern void case_sort(my_string str,uint length);
extern uint ma_dirname_part(my_string to,const char *name);
extern uint ma_dirname_length(const char *name);
#define base_name(A) (A+dirname_length(A))
extern int test_if_hard_path(const char *dir_name);
extern char *ma_convert_dirname(my_string name);
extern void to_unix_path(my_string name);
extern my_string ma_fn_ext(const char *name);
extern my_string fn_same(my_string toname,const char *name,int flag);
extern my_string ma_fn_format(my_string to,const char *name,const char *dsk,
			   const char *form,int flag);
extern size_s ma_strlength(const char *str);
extern void ma_pack_dirname(my_string to,const char *from);
extern uint unma_pack_dirname(my_string to,const char *from);
extern uint ma_cleanup_dirname(my_string to,const char *from);
extern uint ma_system_filename(my_string to,const char *from);
extern my_string ma_unpack_filename(my_string to,const char *from);
extern my_string ma_intern_filename(my_string to,const char *from);
extern my_string directory_file_name(my_string dst, const char *src);
extern int pack_filename(my_string to, const char *name, size_s max_length);
extern my_string my_path(my_string to,const char *progname,
			 const char *own_pathname_part);
extern my_string my_load_path(my_string to, const char *path,
			      const char *own_path_prefix);
extern int wild_compare(const char *str,const char *wildstr);
extern my_string my_strcasestr(const char *src,const char *suffix);
extern int my_strcasecmp(const char *s,const char *t);
extern int my_strsortcmp(const char *s,const char *t);
extern int my_casecmp(const char *s,const char *t,uint length);
extern int my_sortcmp(const char *s,const char *t,uint length);
extern int my_sortncmp(const char *s,uint s_len, const char *t,uint t_len);
#ifdef TBR
extern WF_PACK *wf_comp(my_string str);
extern int wf_test(struct wild_file_pack *wf_pack,const char *name);
extern void wf_end(struct wild_file_pack *buffer);
extern size_s strip_sp(my_string str);
extern void get_date(my_string to,int timeflag,time_t use_time);
extern void soundex(my_string out_pntr, my_string in_pntr,pbool remove_garbage);
extern int init_record_cache(RECORD_CACHE *info,uint cachesize,File file,
			     uint reclength,enum cache_type type,
			     pbool use_async_io);
extern int read_cache_record(RECORD_CACHE *info,unsigned char *to);
extern int end_record_cache(RECORD_CACHE *info);
extern int write_cache_record(RECORD_CACHE *info,my_off_t filepos,
			      const unsigned char *record,uint length);
extern int flush_write_cache(RECORD_CACHE *info);
extern long my_clock(void);
extern sig_handler sigtstp_handler(int signal_number);
extern void handle_recived_signals(void);
extern int init_key_cache(ulong use_mem,ulong leave_this_much_mem);
extern unsigned char *key_cache_read(File file,my_off_t filepos,unsigned char* buff,uint length,
			    uint block_length,int return_buffer);
extern int key_cache_write(File file,my_off_t filepos,unsigned char* buff,uint length,
			   uint block_length,int force_write);
extern int flush_key_blocks(int file, enum flush_type type);
extern void end_key_cache(void);
extern sig_handler my_set_alarm_variable(int signo);
extern void my_string_ptr_sort(void *base,uint items,size_s size);
extern void radixsort_for_str_ptr(uchar* base[], uint number_of_elements,
				  size_s size_of_element,uchar *buffer[]);
extern qsort_t qsort2(void *base_ptr, size_t total_elems, size_t size,
		      qsort2_cmp cmp, void *cmp_argument);
extern qsort2_cmp get_ptr_compare(uint);
extern int init_io_cache(IO_CACHE *info,File file,uint cachesize,
			 enum cache_type type,my_off_t seek_offset,
			 pbool use_async_io, myf cache_myflags);
extern my_bool reinit_io_cache(IO_CACHE *info,enum cache_type type,
			       my_off_t seek_offset,pbool use_async_io,
			       pbool clear_cache);
extern int _my_b_read(IO_CACHE *info,unsigned char *Buffer,uint Count);
extern int _my_b_net_read(IO_CACHE *info,unsigned char *Buffer,uint Count);
extern int _my_b_get(IO_CACHE *info);
extern int _my_b_async_read(IO_CACHE *info,unsigned char *Buffer,uint Count);
extern int _my_b_write(IO_CACHE *info,const unsigned char *Buffer,uint Count);
extern int my_block_write(IO_CACHE *info, const unsigned char *Buffer,
			  uint Count, my_off_t pos);
extern int flush_io_cache(IO_CACHE *info);
extern int end_io_cache(IO_CACHE *info);
extern uint my_b_fill(IO_CACHE *info);
extern void my_b_seek(IO_CACHE *info,my_off_t pos);
extern uint my_b_gets(IO_CACHE *info, char *to, uint max_length);
extern uint my_b_printf(IO_CACHE *info, const char* fmt, ...);
extern uint my_b_vprintf(IO_CACHE *info, const char* fmt, va_list ap);
extern my_bool open_cached_file(IO_CACHE *cache,const char *dir,
				 const char *prefix, uint cache_size,
				 myf cache_myflags);
extern my_bool real_open_cached_file(IO_CACHE *cache);
extern void close_cached_file(IO_CACHE *cache);
File create_temp_file(char *to, const char *dir, const char *pfx,
		      int mode, myf MyFlags);
#define ma_init_dynamic_array(A,B,C,D) init_dynamic_array(A,B,C,D CALLER_INFO)
#endif
extern my_bool ma_init_dynamic_array(DYNAMIC_ARRAY *array,uint element_size,
	  uint init_alloc,uint alloc_increment CALLER_INFO_PROTO);
#define ma_init_dynamic_array_ci(A,B,C,D) ma_init_dynamic_array(A,B,C,D ORIG_CALLER_INFO)
extern my_bool ma_insert_dynamic(DYNAMIC_ARRAY *array,void * element);
extern unsigned char *ma_alloc_dynamic(DYNAMIC_ARRAY *array);
extern unsigned char *ma_pop_dynamic(DYNAMIC_ARRAY*);
extern my_bool ma_set_dynamic(DYNAMIC_ARRAY *array,void * element,uint array_index);
extern void ma_get_dynamic(DYNAMIC_ARRAY *array,void * element,uint array_index);
extern void ma_delete_dynamic(DYNAMIC_ARRAY *array);
extern void ma_delete_dynamic_element(DYNAMIC_ARRAY *array, uint array_index);
extern void ma_freeze_size(DYNAMIC_ARRAY *array);
#define dynamic_array_ptr(array,array_index) ((array)->buffer+(array_index)*(array)->size_of_element)
#define dynamic_element(array,array_index,type) ((type)((array)->buffer) +(array_index))
#define push_dynamic(A,B) ma_insert_dynamic(A,B)

extern int ma_find_type(my_string x,TYPELIB *typelib,uint full_name);
extern void ma_make_type(my_string to,uint nr,TYPELIB *typelib);
extern const char *ma_get_type(TYPELIB *typelib,uint nr);
extern my_bool ma_init_dynamic_string(DYNAMIC_STRING *str, const char *init_str,
				   size_t init_alloc, size_t alloc_increment);
extern my_bool ma_dynstr_append(DYNAMIC_STRING *str, const char *append);
my_bool ma_dynstr_append_mem(DYNAMIC_STRING *str, const char *append,
			  size_t length);
extern my_bool ma_dynstr_set(DYNAMIC_STRING *str, const char *init_str);
extern my_bool ma_dynstr_realloc(DYNAMIC_STRING *str, size_t additional_size);
extern void ma_dynstr_free(DYNAMIC_STRING *str);
void set_all_changeable_vars(CHANGEABLE_VAR *vars);
my_bool set_changeable_var(my_string str,CHANGEABLE_VAR *vars);
my_bool set_changeable_varval(const char *var, ulong val,
			      CHANGEABLE_VAR *vars);
#ifdef HAVE_MLOCK
extern unsigned char *ma_malloc_lock(size_t length,myf flags);
extern void ma_free_lock(unsigned char *ptr,myf flags);
#else
#define ma_malloc_lock(A,B) ma_malloc((A),(B))
#define ma_free_lock(A,B) ma_free((A),(B))
#endif
#define ma_alloc_root_inited(A) ((A)->min_malloc != 0)
void ma_init_alloc_root(MA_MEM_ROOT *mem_root, size_t block_size, size_t pre_alloc_size);
void *ma_alloc_root(MA_MEM_ROOT *mem_root, size_t Size);
void ma_free_root(MA_MEM_ROOT *root, myf MyFLAGS);
char *ma_strdup_root(MA_MEM_ROOT *root,const char *str);
char *ma_memdup_root(MA_MEM_ROOT *root,const char *str, size_t len);
void ma_free_defaults(char **argv);
void ma_print_defaults(const char *conf_file, const char **groups);
my_bool _mariadb_compress(unsigned char *, size_t *, size_t *);
my_bool _mariadb_uncompress(unsigned char *, size_t *, size_t *);
unsigned char *_mariadb_compress_alloc(const unsigned char *packet, size_t *len, size_t *complen);
ulong checksum(const unsigned char *mem, uint count);

#if defined(_MSC_VER) && !defined(_WIN32)
extern void sleep(int sec);
#endif

#ifdef	__cplusplus
}
#endif
#endif /* _my_sys_h */
