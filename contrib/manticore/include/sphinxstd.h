//
// Copyright (c) 2017-2021, Manticore Software LTD (https://manticoresearch.com)
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

#ifndef _sphinxstd_
#define _sphinxstd_

#if _MSC_VER>=1400
#define _CRT_SECURE_NO_DEPRECATE 1
#define _CRT_NONSTDC_NO_DEPRECATE 1
#endif

#if _MSC_VER>=1600
#define HAVE_STDINT_H 1
#endif

#if (_MSC_VER>=1000) && !defined(__midl) && defined(_PREFAST_)
typedef int __declspec("SAL_nokernel") __declspec("SAL_nodriver") __prefast_flag_kernel_driver_mode;
#endif

#if defined(_MSC_VER) && (_MSC_VER<1400)
#define vsnprintf _vsnprintf
#endif

#ifndef __GNUC__
#define __attribute__(x)
#endif

#if HAVE_CONFIG_H
#include "config.h"
#endif

// supress C4577 ('noexcept' used with no exception handling mode specified)
#if _MSC_VER==1900
#pragma warning(disable:4577)
#endif

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cctype>
#include <cstdarg>
#include <climits>
#include <utility>
#include <memory>
#include <functional>
#include <atomic>

// for 64-bit types
#if HAVE_STDINT_H
#include <stdint.h>
#endif

#if HAVE_INTTYPES_H
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#endif

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifndef USE_WINDOWS
#ifdef _MSC_VER
#define USE_WINDOWS 1
#else
#define USE_WINDOWS 0
#endif // _MSC_VER
#endif

#if !USE_WINDOWS
#include <sys/mman.h>
#include <cerrno>
#include <pthread.h>
#include <semaphore.h>
#endif

#if USE_WINDOWS
	#include <io.h>
	#define sphSeek		_lseeki64
	typedef __int64		SphOffset_t;
#else
	#include <unistd.h>
	#define sphSeek		lseek
	typedef off_t		SphOffset_t;
#endif


/////////////////////////////////////////////////////////////////////////////
// COMPILE-TIME CHECKS
/////////////////////////////////////////////////////////////////////////////

#if defined (__GNUC__)
#define VARIABLE_IS_NOT_USED __attribute__((unused))
#define NO_RETURN  __attribute__ ((__noreturn__))
#else
#define  VARIABLE_IS_NOT_USED
#define NO_RETURN
#endif

#define STATIC_ASSERT(_cond,_name)		typedef char STATIC_ASSERT_FAILED_ ## _name [ (_cond) ? 1 : -1 ] VARIABLE_IS_NOT_USED
#define STATIC_SIZE_ASSERT(_type,_size)	STATIC_ASSERT ( sizeof(_type)==_size, _type ## _MUST_BE_ ## _size ## _BYTES )


#ifndef __analysis_assume
#define __analysis_assume(_arg)
#endif


/// some function arguments only need to have a name in debug builds
#ifndef NDEBUG
#define DEBUGARG(_arg) _arg
#else
#define DEBUGARG(_arg)
#endif

/////////////////////////////////////////////////////////////////////////////
// PORTABILITY
/////////////////////////////////////////////////////////////////////////////

#if _WIN32

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>

#include <intrin.h> // for bsr
#pragma intrinsic(_BitScanReverse)

#define strcasecmp			strcmpi
#define strncasecmp			_strnicmp
#define snprintf			_snprintf
#define strtoll				_strtoi64
#define strtoull			_strtoui64

#else

#if USE_ODBC
// UnixODBC compatible DWORD
#if defined(__alpha) || defined(__sparcv9) || defined(__LP64__) || (defined(__HOS_AIX__) && defined(_LP64)) || defined(__APPLE__)
typedef unsigned int		DWORD;
#else
typedef unsigned long		DWORD;
#endif
#else
// default DWORD
typedef unsigned int		DWORD;
#endif // USE_ODBC

typedef unsigned short		WORD;
typedef unsigned char		BYTE;

#endif // _WIN32

/////////////////////////////////////////////////////////////////////////////
// 64-BIT INTEGER TYPES AND MACROS
/////////////////////////////////////////////////////////////////////////////

#if defined(U64C) || defined(I64C)
#error "Internal 64-bit integer macros already defined."
#endif

#if !HAVE_STDINT_H

#if defined(_MSC_VER)
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#define U64C(v) v ## UI64
#define I64C(v) v ## I64
#define PRIu64 "I64d"
#define PRIi64 "I64d"
#else // !defined(_MSC_VER)
//typedef long long int64_t;
//typedef unsigned long long uint64_t;
#endif // !defined(_MSC_VER)

#endif // no stdint.h

// if platform-specific macros were not supplied, use common defaults
#ifndef U64C
#define U64C(v) v ## ULL
#endif

#ifndef I64C
#define I64C(v) v ## LL
#endif

#ifndef PRIu64
#define PRIu64 "llu"
#endif

#ifndef PRIi64
#define PRIi64 "lld"
#endif

#define UINT64_FMT "%" PRIu64
#define INT64_FMT "%" PRIi64

#ifndef UINT64_MAX
#define UINT64_MAX U64C(0xffffffffffffffff)
#endif

#ifndef INT64_MIN
#define INT64_MIN I64C(0x8000000000000000)
#endif

#ifndef INT64_MAX
#define INT64_MAX I64C(0x7fffffffffffffff)
#endif

STATIC_SIZE_ASSERT ( uint64_t, 8 );
STATIC_SIZE_ASSERT ( int64_t, 8 );

// conversion macros that suppress %lld format warnings vs printf
// problem is, on 64-bit Linux systems with gcc and stdint.h, int64_t is long int
// and despite sizeof(long int)==sizeof(long long int)==8, gcc bitches about that
// using PRIi64 instead of %lld is of course The Right Way, but ugly like fuck
// so lets wrap them args in INT64() instead
#define INT64(_v) ((long long int)(_v))
#define UINT64(_v) ((unsigned long long int)(_v))

/////////////////////////////////////////////////////////////////////////////
// MEMORY MANAGEMENT
/////////////////////////////////////////////////////////////////////////////

#define SPH_DEBUG_LEAKS			0
#define SPH_ALLOC_FILL			0
#define SPH_ALLOCS_PROFILER		0
#define SPH_DEBUG_BACKTRACES 0 // will add not only file/line, but also full backtrace

#if SPH_DEBUG_LEAKS || SPH_ALLOCS_PROFILER

/// debug new that tracks memory leaks
void *			operator new ( size_t iSize, const char * sFile, int iLine );

/// debug new that tracks memory leaks
void *			operator new [] ( size_t iSize, const char * sFile, int iLine );

/// debug allocate to use in custom allocator
void * debugallocate ( size_t );

/// debug deallocate to use in custom allocator
void debugdeallocate ( void * );

/// get current allocs count
int				sphAllocsCount ();

/// total allocated bytes
int64_t			sphAllocBytes ();

/// get last alloc id
int				sphAllocsLastID ();

/// dump all allocs since given id
void			sphAllocsDump ( int iFile, int iSinceID );

/// dump stats to stdout
void			sphAllocsStats ();

/// check all existing allocs; raises assertion failure in cases of errors
void			sphAllocsCheck ();

void			sphMemStatDump ( int iFD );

/// per thread cleanup of memory statistic's
void			sphMemStatThdCleanup ( void * pTLS );

void *			sphMemStatThdInit ();

void			sphMemStatMMapAdd ( int64_t iSize );
void			sphMemStatMMapDel ( int64_t iSize );

#undef new
#define new		new(__FILE__,__LINE__)
#define NEW_IS_OVERRIDED 1

#if USE_RE2
#define MYTHROW() throw()
#else
#define MYTHROW() noexcept
#endif

/// delete for my new
void			operator delete ( void * pPtr ) MYTHROW();

/// delete for my new
void			operator delete [] ( void * pPtr ) MYTHROW();

template<typename T>
class managed_allocator
{
public:
    typedef size_t size_type;
    typedef T * pointer;
    typedef const T * const_pointer;
	typedef T value_type;

    template<typename _Tp1>
    struct rebind
    {
        typedef managed_allocator <_Tp1> other;
    };

    pointer allocate ( size_type n, const void * = 0 )
    {
		return ( T * ) debugallocate ( n * sizeof ( T ) );
    }

    void deallocate ( pointer p, size_type )
    {
		debugdeallocate (p);
    }
};
#else
template<typename T> using managed_allocator = std::allocator<T>;
#endif // SPH_DEBUG_LEAKS || SPH_ALLOCS_PROFILER

extern const char * strerrorm ( int errnum ); // defined in sphinxint.h

/////////////////////////////////////////////////////////////////////////////
// THREAD ANNOTATIONS
/////////////////////////////////////////////////////////////////////////////

#if defined(__clang__)
#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__( x ) // no-op
#endif

#define CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

#define SCOPED_CAPABILITY \
    THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define GUARDED_BY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#define PT_GUARDED_BY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define ACQUIRED_BEFORE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define ACQUIRED_AFTER( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))

#define REQUIRES( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))

#define REQUIRES_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(requires_shared_capability(__VA_ARGS__))

#define ACQUIRE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))

#define ACQUIRE_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))

#define RELEASE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))

#define RELEASE_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))

#define TRY_ACQUIRE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))

#define TRY_ACQUIRE_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

#define EXCLUDES( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define ASSERT_CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(assert_capability(x))

#define ASSERT_SHARED_CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_capability(x))

#define RETURN_CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define NO_THREAD_SAFETY_ANALYSIS \
    THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

// Replaced by TRY_ACQUIRE
#define EXCLUSIVE_TRYLOCK_FUNCTION( ... ) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_trylock_function(__VA_ARGS__))

// Replaced by TRY_ACQUIRE_SHARED
#define SHARED_TRYLOCK_FUNCTION( ... ) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_trylock_function(__VA_ARGS__))

// Replaced by RELEASE and RELEASE_SHARED
#define UNLOCK_FUNCTION( ... ) \
	THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))

/////////////////////////////////////////////////////////////////////////////
// HELPERS
/////////////////////////////////////////////////////////////////////////////

// magic to determine widest from provided types and initialize whole unions
// for example,
/*
 *	union foo {
 *		BYTE	a;
 *		char	b;
 *		DWORD	c;
 *		WORDID	w;
 *		sphDocid_t d;
 *		void*	p;
 *		WIDEST<BYTE,char,DWORD,WORDID,sphDocid_t,void*>::T _init = 0;
 *	};
 */
template < typename T1, typename T2, bool= (sizeof ( T1 )<sizeof ( T2 )) >
struct WIDER
{
	using T=T2;
};

template < typename T1, typename T2 >
struct WIDER < T1, T2, false >
{
	using T=T1;
};

template < typename T1, typename... TYPES >
struct WIDEST
{
	using T=typename WIDER < T1, typename WIDEST< TYPES... >::T >::T;
};

template < typename T1, typename T2 >
struct WIDEST<T1, T2>
{
	using T=typename WIDER < T1, T2 >::T;
};



inline int sphBitCount ( DWORD n )
{
	// MIT HACKMEM count
	// works for 32-bit numbers only
	// fix last line for 64-bit numbers
	DWORD tmp;
	tmp = n - ((n >> 1) & 033333333333) - ((n >> 2) & 011111111111);
	return ( (tmp + (tmp >> 3) ) & 030707070707) % 63;
}

using SphDieCallback_t = bool (*) ( bool bDie, const char *, va_list );

/// use env variables, if available, instead of hard-coded macro
const char * GET_FULL_SHARE_DIR();

const char * GET_GALERA_SONAME ();

const char * GET_MYSQL_LIB();

const char * GET_PGSQL_LIB ();

const char * GET_UNIXODBC_LIB ();

const char * GET_EXPAT_LIB ();

const char * GET_ICU_DATA_DIR ();

/// crash with an error message, and do not have searchd watchdog attempt to resurrect
void			sphDie ( const char * sFmt, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) ) NO_RETURN;

/// crash with an error message, but have searchd watchdog attempt to resurrect
void			sphDieRestart ( const char * sMessage, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) ) NO_RETURN;

/// shutdown (not crash) on unrrecoverable error
void			sphFatal ( const char * sFmt, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) ) NO_RETURN;

/// log fatal error, not shutdown
void			sphFatalLog ( const char * sFmt, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) );

/// setup a callback function to call from sphDie() before exit
/// if callback returns false, sphDie() will not log to stdout
void			sphSetDieCallback ( SphDieCallback_t pfDieCallback );

/// how much bits do we need for given int
inline int sphLog2 ( uint64_t uValue )
{
#if USE_WINDOWS
	DWORD uRes;
	if ( BitScanReverse ( &uRes, (DWORD)( uValue>>32 ) ) )
		return 33+uRes;
	BitScanReverse ( &uRes, DWORD(uValue) );
	return 1+uRes;
#elif __GNUC__ || __clang__
	if ( !uValue )
		return 0;
	return 64 - __builtin_clzll(uValue);
#else
	int iBits = 0;
	while ( uValue )
	{
		uValue >>= 1;
		iBits++;
	}
	return iBits;
#endif
}

/// float vs dword conversion
inline DWORD sphF2DW ( float f )	{ union { float f; DWORD d; } u; u.f = f; return u.d; }

/// dword vs float conversion
inline float sphDW2F ( DWORD d )	{ union { float f; DWORD d; } u; u.d = d; return u.f; }

/// double to bigint conversion
inline uint64_t sphD2QW ( double f )	{ union { double f; uint64_t d; } u; u.f = f; return u.d; }

/// bigint to double conversion
inline double sphQW2D ( uint64_t d )	{ union { double f; uint64_t d; } u; u.d = d; return u.f; }

/// microsecond precision timestamp
/// current UNIX timestamp in seconds multiplied by 1000000, plus microseconds since the beginning of current second
int64_t		sphMicroTimer ();

/// return cpu time, in microseconds. CLOCK_THREAD_CPUTIME_ID, or CLOCK_PROCESS_CPUTIME_ID or fall to sphMicroTimer().
/// defined in searchd.cpp since depends from g_bCpuStats
int64_t		sphCpuTimer ();

/// returns sphCpuTimer() adjusted to current coro task (coro may jump from thread to thread, so sphCpuTimer() is irrelevant)
int64_t		sphTaskCpuTimer ();

/// double argument squared
inline double sqr ( double v ) { return v*v;}

/// float argument squared
inline float fsqr ( float v ) { return v*v; }

//////////////////////////////////////////////////////////////////////////
// RANDOM NUMBERS GENERATOR
//////////////////////////////////////////////////////////////////////////

/// seed RNG
void		sphSrand ( DWORD uSeed );

/// auto-seed RNG based on time and PID
void		sphAutoSrand ();

/// generate another random
DWORD		sphRand ();

/////////////////////////////////////////////////////////////////////////////
// DEBUGGING
/////////////////////////////////////////////////////////////////////////////

#if USE_WINDOWS
#ifndef NDEBUG

void sphAssert ( const char * sExpr, const char * sFile, int iLine );

#undef assert
#define assert(_expr) (void)( (_expr) || ( sphAssert ( #_expr, __FILE__, __LINE__ ), 0 ) )

#endif // !NDEBUG
#endif // USE_WINDOWS


// to avoid disappearing of _expr in release builds
#ifndef NDEBUG
#define Verify(_expr) assert(_expr)
#else
#define Verify(_expr) _expr
#endif

#ifndef NDEBUG
#define Debug( _expr ) _expr
#else
#define Debug(_expr)
#endif

/////////////////////////////////////////////////////////////////////////////
// GENERICS
/////////////////////////////////////////////////////////////////////////////

template <typename T> T Min ( T a, T b ) { return a<b ? a : b; }
template <typename T, typename U> typename WIDER<T,U>::T Min ( T a, U b )
{
	return a<b ? a : b;
}
template <typename T> T Max ( T a, T b ) { return a<b ? b : a; }
template <typename T, typename U> typename WIDER<T,U>::T Max ( T a, U b )
{
	return a<b ? b : a;
}
#define SafeDelete(_x)		{ if (_x) { delete (_x); (_x) = nullptr; } }
#define SafeDeleteArray(_x)	{ if (_x) { delete [] (_x); (_x) = nullptr; } }
#define SafeRelease(_x)		{ if (_x) { (_x)->Release(); (_x) = nullptr; } }
#define SafeAddRef( _x )        { if (_x) { (_x)->AddRef(); } }

/// swap
template < typename T > inline void Swap ( T & v1, T & v2 )
{
	T temp = std::move ( v1 );
	v1 = std::move ( v2 );
	v2 = std::move ( temp );
}

/// prevent copy
class ISphNoncopyable
{
public:
	ISphNoncopyable () = default;
	ISphNoncopyable ( const ISphNoncopyable & ) = delete;
	const ISphNoncopyable &		operator = ( const ISphNoncopyable & ) = delete;
};

/// prevent move
class ISphNonmovable
{
public:
	ISphNonmovable() = default;
	ISphNonmovable( ISphNonmovable&& ) noexcept = delete;
	ISphNonmovable& operator=( ISphNonmovable&& ) noexcept = delete;
};

// implement moving ctr and moving= using swap-and-release
#define MOVE_BYSWAP(class_c)								\
    class_c ( class_c&& rhs) noexcept {Swap(rhs);}			\
    class_c & operator= ( class_c && rhs ) noexcept			\
 		{ Swap(rhs); return *this;  }

// take all ctr definitions from parent
#define FWD_CTOR(type_c, base_c)                                                         \
    template <typename... V>                                                             \
    type_c(V&&... v)                                                                     \
        : base_c{std::forward<V>(v)...}                                                  \
    {                                                                                    \
    }

// take all ctr definitions from BASE parent
#define FWD_BASECTOR(type_c)  FWD_CTOR ( type_c, BASE )

//////////////////////////////////////////////////////////////////////////////

/// generic comparator
template < typename T >
struct SphLess_T
{
	static inline bool IsLess ( const T & a, const T & b )
	{
		return a < b;
	}
};


/// generic comparator
template < typename T >
struct SphGreater_T
{
	static inline bool IsLess ( const T & a, const T & b )
	{
		return b < a;
	}
};


/// generic comparator
template < typename T, typename C >
struct SphMemberLess_T
{
	const T C::* m_pMember;

	explicit SphMemberLess_T ( T C::* pMember )
		: m_pMember ( pMember )
	{}

	inline bool IsLess ( const C & a, const C & b ) const
	{
		return ( (&a)->*m_pMember ) < ( (&b)->*m_pMember );
	}
};

template < typename T, typename C >
inline SphMemberLess_T<T,C>
sphMemberLess ( T C::* pMember )
{
	return SphMemberLess_T<T,C> ( pMember );
}

/// generic comparator initialized by functor
template<typename COMP>
struct SphLesser
{
	COMP m_fnComp;
	explicit SphLesser (COMP&& fnComp)
		: m_fnComp ( std::forward<COMP>(fnComp)) {}

	template <typename T>
	bool IsLess ( T&& a, T&& b ) const
	{
		return m_fnComp ( std::forward<T>(a), std::forward<T>(b) );
	}
};

// make
template<typename FNCOMP>
SphLesser<FNCOMP> Lesser (FNCOMP&& fnComp)
{
	return SphLesser<FNCOMP>(std::forward<FNCOMP>(fnComp));
}

/// generic accessor
template < typename T >
struct SphAccessor_T
{
	using MEDIAN_TYPE = T;

	MEDIAN_TYPE & Key ( T * a ) const
	{
		return *a;
	}

	void CopyKey ( MEDIAN_TYPE * pMed, T * pVal ) const
	{
		*pMed = Key(pVal);
	}

	void Swap ( T * a, T * b ) const
	{
		::Swap ( *a, *b );
	}

	T * Add ( T * p, int i ) const
	{
		return p+i;
	}

	int Sub ( T * b, T * a ) const
	{
		return (int)(b-a);
	}
};


/// heap sort helper
template < typename T, typename U, typename V >
void sphSiftDown ( T * pData, int iStart, int iEnd, U&& COMP, V&& ACC )
{
	while (true)
	{
		int iChild = iStart*2+1;
		if ( iChild>iEnd )
			return;

		int iChild1 = iChild+1;
		if ( iChild1<=iEnd && COMP.IsLess ( ACC.Key ( ACC.Add ( pData, iChild ) ), ACC.Key ( ACC.Add ( pData, iChild1 ) ) ) )
			iChild = iChild1;

		if ( COMP.IsLess ( ACC.Key ( ACC.Add ( pData, iChild ) ), ACC.Key ( ACC.Add ( pData, iStart ) ) ) )
			return;
		ACC.Swap ( ACC.Add ( pData, iChild ), ACC.Add ( pData, iStart ) );
		iStart = iChild;
	}
}


/// heap sort
template < typename T, typename U, typename V >
void sphHeapSort ( T * pData, int iCount, U&& COMP, V&& ACC )
{
	if ( !pData || iCount<=1 )
		return;

	// build a max-heap, so that the largest element is root
	for ( int iStart=( iCount-2 )>>1; iStart>=0; --iStart )
		sphSiftDown ( pData, iStart, iCount-1, std::forward<U>(COMP), std::forward<V>(ACC) );

	// now keep popping root into the end of array
	for ( int iEnd=iCount-1; iEnd>0; )
	{
		ACC.Swap ( pData, ACC.Add ( pData, iEnd ) );
		sphSiftDown ( pData, 0, --iEnd, std::forward<U>(COMP), std::forward<V>(ACC) );
	}
}


/// generic sort
template < typename T, typename U, typename V >
void sphSort ( T * pData, int iCount, U&& COMP, V&& ACC )
{
	if ( iCount<2 )
		return;

	typedef T * P;
	// st0 and st1 are stacks with left and right bounds of array-part.
	// They allow us to avoid recursion in quicksort implementation.
	P st0[32], st1[32], a, b, i, j;
	typename std::remove_reference<V>::type::MEDIAN_TYPE x;
	int k;

	const int SMALL_THRESH = 32;
	int iDepthLimit = sphLog2 ( iCount );
	iDepthLimit = ( ( iDepthLimit<<2 ) + iDepthLimit ) >> 1; // x2.5

	k = 1;
	st0[0] = pData;
	st1[0] = ACC.Add ( pData, iCount-1 );
	while ( k )
	{
		k--;
		i = a = st0[k];
		j = b = st1[k];

		// if quicksort fails on this data; switch to heapsort
		if ( !k )
		{
			if ( !--iDepthLimit )
			{
				sphHeapSort ( a, ACC.Sub ( b, a )+1, std::forward<U>(COMP), ACC );
				return;
			}
		}

		// for tiny arrays, switch to insertion sort
		int iLen = ACC.Sub ( b, a );
		if ( iLen<=SMALL_THRESH )
		{
			for ( i=ACC.Add ( a, 1 ); i<=b; i=ACC.Add ( i, 1 ) )
			{
				for ( j=i; j>a; )
				{
					P j1 = ACC.Add ( j, -1 );
					if ( COMP.IsLess ( ACC.Key(j1), ACC.Key(j) ) )
						break;
					ACC.Swap ( j, j1 );
					j = j1;
				}
			}
			continue;
		}

		// ATTENTION! This copy can lead to memleaks if your CopyKey
		// copies something which is not freed by objects destructor.
		ACC.CopyKey ( &x, ACC.Add ( a, iLen/2 ) );
		while ( a<b )
		{
			while ( i<=j )
			{
				while ( COMP.IsLess ( ACC.Key(i), x ) )
					i = ACC.Add ( i, 1 );
				while ( COMP.IsLess ( x, ACC.Key(j) ) )
					j = ACC.Add ( j, -1 );
				if ( i<=j )
				{
					ACC.Swap ( i, j );
					i = ACC.Add ( i, 1 );
					j = ACC.Add ( j, -1 );
				}
			}

			// Not so obvious optimization. We put smaller array-parts
			// to the top of stack. That reduces peak stack size.
			if ( ACC.Sub ( j, a )>=ACC.Sub ( b, i ) )
			{
				if ( a<j ) { st0[k] = a; st1[k] = j; k++; }
				a = i;
			} else
			{
				if ( i<b ) { st0[k] = i; st1[k] = b; k++; }
				b = j;
			}
		}
	}
}


template < typename T, typename U >
void sphSort ( T * pData, int iCount, U&& COMP )
{
	sphSort ( pData, iCount, std::forward<U>(COMP), SphAccessor_T<T>() );
}


template < typename T >
void sphSort ( T * pData, int iCount )
{
	sphSort ( pData, iCount, SphLess_T<T>() );
}

//////////////////////////////////////////////////////////////////////////

/// member functor, wraps object member access
template < typename T, typename CLASS >
struct SphMemberFunctor_T
{
	const T CLASS::*	m_pMember;

	explicit			SphMemberFunctor_T ( T CLASS::* pMember )	: m_pMember ( pMember ) {}
	const T &			operator () ( const CLASS & arg ) const		{ return (&arg)->*m_pMember; }

	inline bool IsLess ( const CLASS & a, const CLASS & b ) const
	{
		return (&a)->*m_pMember < (&b)->*m_pMember;
	}

	inline bool IsEq ( const CLASS & a, T b )
	{
		return ( (&a)->*m_pMember )==b;
	}
};


/// handy member functor generator
/// this sugar allows you to write like this
/// dArr.Sort ( bind ( &CSphType::m_iMember ) );
/// dArr.BinarySearch ( bind ( &CSphType::m_iMember ), iValue );
template < typename T, typename CLASS >
inline SphMemberFunctor_T < T, CLASS >
bind ( T CLASS::* ptr )
{
	return SphMemberFunctor_T < T, CLASS > ( ptr );
}


/// identity functor
template < typename T >
struct SphIdentityFunctor_T
{
	const T &			operator () ( const T & arg ) const			{ return arg; }
};

/// equality functor
template < typename T >
struct SphEqualityFunctor_T
{
	bool IsEq ( const T & a, const T & b )
	{
		return a==b;
	}
};


//////////////////////////////////////////////////////////////////////////

/// generic binary search
template < typename T, typename U, typename PRED >
T * sphBinarySearch ( T * pStart, T * pEnd, PRED && tPred, U tRef )
{
	if ( !pStart || pEnd<pStart )
		return NULL;

	if ( tPred(*pStart)==tRef )
		return pStart;

	if ( tPred(*pEnd)==tRef )
		return pEnd;

	while ( pEnd-pStart>1 )
	{
		if ( tRef<tPred(*pStart) || tPred(*pEnd)<tRef )
			break;
		assert ( tPred(*pStart)<tRef );
		assert ( tRef<tPred(*pEnd) );

		T * pMid = pStart + (pEnd-pStart)/2;
		if ( tRef==tPred(*pMid) )
			return pMid;

		if ( tRef<tPred(*pMid) )
			pEnd = pMid;
		else
			pStart = pMid;
	}
	return NULL;
}


// returns first (leftmost) occurrence of the value
// returns -1 if not found
template < typename T, typename U, typename PRED >
int sphBinarySearchFirst ( T * pValues, int iStart, int iEnd, PRED && tPred, U tRef )
{
	assert ( iStart<=iEnd );

	while ( iEnd-iStart>1 )
	{
		if ( tRef<tPred(pValues[iStart]) || tRef>tPred(pValues[iEnd]) )
			return -1;

		int iMid = iStart + (iEnd-iStart)/2;
		if ( tPred(pValues[iMid])>=tRef )
			iEnd = iMid;
		else
			iStart = iMid;
	}

	return iEnd;
}


/// generic binary search
template < typename T >
T * sphBinarySearch ( T * pStart, T * pEnd, T & tRef )
{
	return sphBinarySearch ( pStart, pEnd, SphIdentityFunctor_T<T>(), tRef );
}


// find the first entry that is greater than tRef
template < typename T, typename U, typename PRED >
T * sphBinarySearchFirst ( T * pStart, T * pEnd, PRED && tPred, U tRef )
{
	if ( !pStart || pEnd<pStart )
		return NULL;

	while ( pStart!=pEnd )
	{
		T * pMid = pStart + (pEnd-pStart)/2;
		if ( tRef>tPred(*pMid) )
			pStart = pMid+1;
		else
			pEnd = pMid;
	}

	return pStart;
}


template < typename T, typename T_COUNTER, typename COMP >
T_COUNTER sphUniq ( T * pData, T_COUNTER iCount, COMP && tComp )
{
	if ( !iCount )
		return 0;

	T_COUNTER iSrc = 1, iDst = 1;
	while ( iSrc<iCount )
	{
		if ( tComp.IsEq ( pData[iDst-1], pData[iSrc] ) )
			iSrc++;
		else
			pData[iDst++] = pData[iSrc++];
	}
	return iDst;
}

/// generic uniq
template < typename T, typename T_COUNTER >
T_COUNTER sphUniq ( T * pData, T_COUNTER iCount )
{
	return sphUniq ( pData, iCount, SphEqualityFunctor_T<T>() );
}

/// generic bytes of chars array
using ByteBlob_t = std::pair<const BYTE *, int>;

inline bool IsNull ( const ByteBlob_t & dBlob ) { return !dBlob.second; };
inline bool IsFilled ( const ByteBlob_t & dBlob ) { return dBlob.first && dBlob.second>0; }
inline bool IsValid ( const ByteBlob_t & dBlob ) { return IsNull ( dBlob ) || IsFilled ( dBlob ); };

/// buffer traits - provides generic ops over a typed blob (vector).
/// just provide common operators; doesn't manage buffer anyway
template < typename T > class VecTraits_T
{

public:
	VecTraits_T() = default;

	// this ctr allows to regard any typed blob as VecTraits, and use it's benefits.
	VecTraits_T( T* pData, int64_t iCount )
		: m_pData ( pData )
		, m_iCount ( iCount )
	{}

	template <typename TT>
	VecTraits_T ( TT * pData, int64_t iCount )
		: m_pData ( pData )
		, m_iCount ( iCount * sizeof ( TT ) / sizeof ( T ))
	{}

	template<typename TT, typename INT>
	VecTraits_T ( const std::pair<TT *, INT> & dData )
		: m_pData ( (T*) dData.first ),
		m_iCount ( dData.second * sizeof ( TT ) / sizeof ( T ) )
	{}

	VecTraits_T Slice ( int64_t iBegin=0, int64_t iCount=-1 ) const
	{
		// calculate starting bound
		if ( iBegin<0 )
			iBegin = 0;
		else if ( iBegin>m_iCount )
			iBegin = m_iCount;

		iCount = ( iCount<0 ) ? ( m_iCount - iBegin ) : Min ( iCount, m_iCount - iBegin );
		return VecTraits_T ( m_pData + iBegin, iCount );
	}

	/// accessor by forward index
	T &operator[] ( int64_t iIndex ) const
	{
		assert ( iIndex>=0 && iIndex<m_iCount );
		return m_pData[iIndex];
	}

	T & At ( int64_t iIndex ) const
	{
		return this->operator [] ( iIndex );
	}

	/// get first entry ptr
	T * Begin () const
	{
		return m_iCount ? m_pData : nullptr;
	}

	/// pointer to the item after the last
	T * End () const
	{
		return  m_pData + m_iCount;
	}

	/// make happy C++11 ranged for loops
	T * begin () const
	{
		return Begin ();
	}

	T * end () const
	{
		return m_iCount ? m_pData + m_iCount : nullptr;
	}

	/// get first entry
	T &First () const
	{
		return ( *this )[0];
	}

	/// get last entry
	T &Last () const
	{
		return ( *this )[m_iCount - 1];
	}

	/// return idx of the item pointed by pBuf, or -1
	inline int Idx ( const T* pBuf ) const
	{
		if ( !pBuf )
			return -1;

		if ( pBuf < m_pData || pBuf >= m_pData + m_iCount )
			return -1;

		return pBuf - m_pData;
	}

	/// make possible to pass VecTraits_T<T*> into funcs which need VecTraits_T<const T*>
	/// fixme! M.b. add check and fire error if T is not a pointer?
	operator VecTraits_T<const typename std::remove_pointer<T>::type *> & () const
	{
		return *( VecTraits_T<const typename std::remove_pointer<T>::type *>* ) ( this );
	}

	template<typename TT>
	operator VecTraits_T<TT> & () const
	{
		STATIC_ASSERT ( sizeof ( T )==sizeof ( TT ), SIZE_OF_DERIVED_NOT_SAME_AS_ORIGIN );
		return *( VecTraits_T<TT> * ) ( this );
	}

	template<typename TT, typename INT>
	operator std::pair<TT *, INT> () const
	{
		return { (TT*)m_pData, INT(m_iCount * sizeof ( T ) / sizeof ( TT ) ) };
	}

	/// check if i'm empty
	bool IsEmpty () const
	{
		return ( m_pData==nullptr || m_iCount==0 );
	}

	/// query current length, in elements
	int64_t GetLength64 () const
	{
		return m_iCount;
	}

	int GetLength () const
	{
		return (int)m_iCount;
	}

	/// get length in bytes
	size_t GetLengthBytes () const
	{
		return sizeof ( T ) * ( size_t ) m_iCount;
	}

	/// get length in bytes
	int64_t GetLengthBytes64 () const
	{
		return m_iCount * sizeof ( T );
	}

	/// default sort
	void Sort ( int iStart = 0, int iEnd = -1 )
	{
		Sort ( SphLess_T<T> (), iStart, iEnd );
	}

	/// default reverse sort
	void RSort ( int iStart = 0, int iEnd = -1 )
	{
		Sort ( SphGreater_T<T> (), iStart, iEnd );
	}

	/// generic sort
	template < typename F >
	void Sort ( F&& COMP, int iStart = 0, int iEnd = -1 ) NO_THREAD_SAFETY_ANALYSIS
	{
		if ( m_iCount<2 )
			return;
		if ( iStart<0 )
			iStart += m_iCount;
		if ( iEnd<0 )
			iEnd += m_iCount;
		assert ( iStart<=iEnd );

		sphSort ( m_pData + iStart, iEnd - iStart + 1, std::forward<F>(COMP) );
	}

	/// generic binary search
	/// assumes that the array is sorted in ascending order
	template < typename U, typename PRED >
	T * BinarySearch ( const PRED &tPred, U tRef ) const NO_THREAD_SAFETY_ANALYSIS
	{
		return sphBinarySearch ( m_pData, m_pData + m_iCount - 1, tPred, tRef );
	}

	/// generic binary search
	/// assumes that the array is sorted in ascending order
	T * BinarySearch ( T tRef ) const
	{
		return sphBinarySearch ( m_pData, m_pData + m_iCount - 1, tRef );
	}

	template <typename FILTER >
	inline int GetFirst( FILTER&& cond ) const NO_THREAD_SAFETY_ANALYSIS
	{
		for ( int i = 0; i<m_iCount; ++i )
			if ( cond ( m_pData[i] ) )
				return i;
		return -1;
	}

	/// generic 'ARRAY_ALL'
	template <typename FILTER>
	inline bool all_of ( FILTER && cond ) const NO_THREAD_SAFETY_ANALYSIS
	{
		for ( int i = 0; i<m_iCount; ++i )
			if ( !cond ( m_pData[i] ) )
				return false;
		return true;
	}

	/// generic linear search - 'ARRAY_ANY' replace
	/// see 'Contains()' below for examlpe of usage.
	template <typename FILTER>
	inline bool any_of ( FILTER && cond ) const NO_THREAD_SAFETY_ANALYSIS
	{
		for ( int i = 0; i<m_iCount; ++i )
			if ( cond ( m_pData[i] ) )
				return true;

		return false;
	}

	template <typename FILTER>
	inline bool none_of ( FILTER && cond ) const NO_THREAD_SAFETY_ANALYSIS
	{
		return !any_of ( cond );
	}

	/// Apply an action to every member
	/// Apply ( [] (T& item) {...} );
	template < typename ACTION >
	void Apply( ACTION&& Verb ) const NO_THREAD_SAFETY_ANALYSIS
	{
		for ( int i = 0; i<m_iCount; ++i )
			Verb ( m_pData[i] );
	}

	template < typename ACTION >
	void for_each ( ACTION && tAction ) const
	{
		Apply(tAction);
	}

	/// generic linear search
	bool Contains ( T tRef ) const NO_THREAD_SAFETY_ANALYSIS
	{
		return any_of ( [&] ( const T &v ) { return tRef==v; } );
	}

	/// generic linear search
	template < typename FUNCTOR, typename U >
	bool Contains ( FUNCTOR&& COMP, U tValue ) NO_THREAD_SAFETY_ANALYSIS
	{
		return any_of ( [&] ( const T &v ) { return COMP.IsEq ( v, tValue ); } );
	}

	/// fill with given value
	void Fill ( const T &rhs )
	{
		for ( int i = 0; i<m_iCount; ++i )
			m_pData[i] = rhs;
	}

protected:
	T * m_pData = nullptr;
	int64_t m_iCount = 0;
};

namespace sph {

//////////////////////////////////////////////////////////////////////////
/// Storage backends for vector
/// Each backend provides Allocate and Deallocate

// workaround missing "is_trivially_copyable" in g++ < 5.0
#if defined (__GNUG__) && (__GNUC__ < 5) && !defined (__clang__)
#define IS_TRIVIALLY_COPYABLE(T) __has_trivial_copy(T)
#define IS_TRIVIALLY_DEFAULT_CONSTRUCTIBLE( T ) std::has_trivial_default_constructor<T>::value
#else
#define IS_TRIVIALLY_COPYABLE( T ) std::is_trivially_copyable<T>::value
#define IS_TRIVIALLY_DEFAULT_CONSTRUCTIBLE( T ) std::is_trivially_default_constructible<T>::value
#endif

/// Default backend - uses plain old new/delete
template < typename T >
class DefaultStorage_T
{
protected:
	inline static T * Allocate ( int64_t iLimit )
	{
		return new T[iLimit];
	}

	inline static void Deallocate ( T * pData )
	{
		delete[] pData;
	}

	static const bool is_constructed = true;
	static const bool is_owned = false;
};

/// Static backend: small blobs stored localy,
/// bigger came to plain old new/delete
template < typename T, int STATICSIZE = 4096 >
class LazyStorage_T
{
public:
	// don't allow moving (it has no sence with embedded buffer)
	inline LazyStorage_T ( LazyStorage_T &&rhs ) noexcept = delete;
	inline LazyStorage_T &operator= ( LazyStorage_T &&rhs ) noexcept = delete;

	LazyStorage_T() = default;
	static const int iSTATICSIZE = STATICSIZE;
protected:
	inline T * Allocate ( int64_t iLimit )
	{
		if ( iLimit<=STATICSIZE )
			return m_dData;
		return new T[iLimit];
	}

	inline void Deallocate ( T * pData ) const
	{
		if ( pData!=m_dData )
			delete[] pData;
	}

	static const bool is_constructed = true;
	static const bool is_owned = true;

private:
	T m_dData[iSTATICSIZE];
};

/// optional backend - allocates space but *not* calls ctrs and dtrs
/// bigger came to plain old new/delete
template < typename T >
class RawStorage_T
{
	using StorageType = typename std::aligned_storage<sizeof ( T ), alignof ( T )>::type;
protected:
	inline static T * Allocate ( int64_t iLimit )
	{
		return ( T * )new StorageType[iLimit];
	}

	inline static void Deallocate ( T * pData )
	{
		delete[] reinterpret_cast<StorageType*> (pData);
	}

	//static const bool is_constructed = IS_TRIVIALLY_COPYABLE( T );
	static const bool is_constructed = IS_TRIVIALLY_DEFAULT_CONSTRUCTIBLE ( T );
	static const bool is_owned = false;
};

//////////////////////////////////////////////////////////////////////////
/// Copy backends for vector
/// Each backend provides Copy, Move and CopyOrSwap



/// Copy/move vec of a data item-by-item
template < typename T, bool = IS_TRIVIALLY_COPYABLE(T) >
class DataMover_T
{
public:
	static inline void Copy ( T * pNew, T * pData, int64_t iLength )
	{
		for ( int i = 0; i<iLength; ++i )
			pNew[i] = pData[i];
	}

	static inline void Move ( T * pNew, T * pData, int64_t iLength )
	{
		for ( int i = 0; i<iLength; ++i )
			pNew[i] = std::move ( pData[i] );
	}

	static inline void Zero ( T * pData, int64_t iLength )
	{
		for ( int i = 0; i<iLength; ++i )
			pData[i] = 0;
	}
};

template < typename T > /// Copy/move blob of trivial data using memmove
class DataMover_T<T, true>
{
public:
	static inline void Copy ( T * pNew, const T * pData, int64_t iLength )
	{
		if ( iLength ) // m.b. work without this check, but sanitize for paranoids.
			memmove ( ( void * ) pNew, ( const void * ) pData, iLength * sizeof ( T ) );
	}

	static inline void Move ( T * pNew, const T * pData, int64_t iLength )
	{ Copy ( pNew, pData, iLength ); }

	// append raw blob: defined ONLY in POD specialization.
	static inline void CopyVoid ( T * pNew, const void * pData, int64_t iLength )
	{ Copy ( pNew, ( T * ) pData, iLength ); }

	static inline void Zero ( T * pData, int64_t iLength )
	{ memset ((void *) pData, 0, iLength * sizeof ( T )); }
};

/// default vector mover
template < typename T >
class DefaultCopy_T : public DataMover_T<T>
{
public:
	static inline void CopyOrSwap ( T &pLeft, const T &pRight )
	{
		pLeft = pRight;
	}
};


/// swap-vector policy (for non-copyable classes)
/// use Swap() instead of assignment on resize
template < typename T >
class SwapCopy_T
{
public:
	static inline void Copy ( T * pNew, T * pData, int64_t iLength )
	{
		for ( int i = 0; i<iLength; ++i )
			Swap ( pNew[i], pData[i] );
	}

	static inline void Move ( T * pNew, T * pData, int64_t iLength )
	{
		for ( int i = 0; i<iLength; ++i )
			Swap ( pNew[i], pData[i] );
	}

	static inline void CopyOrSwap ( T &dLeft, T &dRight )
	{
		Swap ( dLeft, dRight );
	}
};

//////////////////////////////////////////////////////////////////////////
/// Resize backends for vector
/// Each backend provides Relimit

/// Default relimit: grow 2x
class DefaultRelimit
{
public:
	static const int MAGIC_INITIAL_LIMIT = 8;
	static inline int64_t Relimit ( int64_t iLimit, int64_t iNewLimit )
	{
		if ( !iLimit )
			iLimit = MAGIC_INITIAL_LIMIT;
		while ( iLimit<iNewLimit )
		{
			iLimit *= 2;
			assert ( iLimit>0 );
		}
		return iLimit;
	}
};

/// tight-vector policy
/// grow only 1.2x on resize (not 2x) starting from a certain threshold
class TightRelimit : public DefaultRelimit
{
public:
	static const int SLOW_GROW_TRESHOLD = 1024;
	static inline int64_t Relimit ( int64_t iLimit, int64_t iNewLimit )
	{
		if ( !iLimit )
			iLimit = MAGIC_INITIAL_LIMIT;
		while ( iLimit<iNewLimit && iLimit<SLOW_GROW_TRESHOLD )
		{
			iLimit *= 2;
			assert ( iLimit>0 );
		}
		while ( iLimit<iNewLimit )
		{
			iLimit = ( int ) ( iLimit * 1.2f );
			assert ( iLimit>0 );
		}
		return iLimit;
	}
};


/// generic vector
/// uses storage, mover and relimit backends
/// (don't even ask why it's not std::vector)
template < typename T, class POLICY=DefaultCopy_T<T>, class LIMIT=DefaultRelimit, class STORE=DefaultStorage_T<T> >
class Vector_T : public VecTraits_T<T>, protected STORE, protected LIMIT
{
protected:
	using BASE = VecTraits_T<T>;
	using BASE::m_pData;
	using BASE::m_iCount;
	using STORE::Allocate;
	using STORE::Deallocate;

public:
	using BASE::Begin;
	using BASE::Sort;
	using BASE::GetLength; // these are for IDE helpers to work
	using BASE::GetLength64;
	using BASE::GetLengthBytes;
	using BASE::Slice;
	using LIMIT::Relimit;


	/// ctor
	Vector_T () = default;

	/// ctor with initial size
	explicit Vector_T ( int iCount )
	{
		Resize ( iCount );
	}

	/// copy ctor
	Vector_T ( const Vector_T<T> & rhs )
	{
		m_iCount = rhs.m_iCount;
		m_iLimit = rhs.m_iLimit;
		if ( m_iLimit )
			m_pData = STORE::Allocate(m_iLimit);
		__analysis_assume ( m_iCount<=m_iLimit );
		POLICY::Copy ( m_pData, rhs.m_pData, m_iCount );
	}

	/// move ctr
	Vector_T ( Vector_T<T> &&rhs ) noexcept
		: Vector_T()
	{
		SwapData(rhs);
	}

	/// dtor
	~Vector_T ()
	{
		destroy_at ( 0, m_iCount );
		STORE::Deallocate ( m_pData );
	}

	/// add entry
	T & Add ()
	{
		if ( m_iCount>=m_iLimit )
			Reserve ( 1 + m_iCount );
		construct_at ( m_iCount, 1 );
		return m_pData[m_iCount++];
	}

	/// add entry
	template<typename S=STORE>
	typename std::enable_if<S::is_constructed>::type Add ( T tValue )
	{
		assert ( ( &tValue<m_pData || &tValue>=( m_pData + m_iCount ) ) && "inserting own value (like last()) by ref!" );
		if ( m_iCount>=m_iLimit )
			Reserve ( 1 + m_iCount );
		m_pData[m_iCount++] = std::move ( tValue );
	}

	template<typename S=STORE>
	typename std::enable_if<!S::is_constructed>::type Add ( T tValue )
	{
		assert (( &tValue<m_pData || &tValue>=( m_pData+m_iCount )) && "inserting own value (like last()) by ref!" );
		if ( m_iCount>=m_iLimit )
			Reserve ( 1+m_iCount );
		new ( m_pData+m_iCount++ ) T ( std::move ( tValue ));
	}

	template<typename S=STORE, class... Args>
	typename std::enable_if<!S::is_constructed>::type
	Emplace_back ( Args && ... args )
	{
		assert ( m_iCount<=m_iLimit );
		new ( m_pData+m_iCount++ ) T ( std::forward<Args> ( args )... );
	}

	/// add N more entries, and return a pointer to that buffer
	T * AddN ( int iCount )
	{
		if ( m_iCount + iCount>m_iLimit )
			Reserve ( m_iCount + iCount );
		construct_at ( m_iCount, iCount );
		m_iCount += iCount;
		return m_pData + m_iCount - iCount;
	}

	/// add unique entry (ie. do not add if equal to last one)
	void AddUnique ( const T & tValue )
	{
		assert ( ( &tValue<m_pData || &tValue>=( m_pData + m_iCount ) ) && "inserting own value (like last()) by ref!" );
		if ( m_iCount>=m_iLimit )
			Reserve ( 1 + m_iCount );

		if ( m_iCount==0 || m_pData[m_iCount - 1]!=tValue )
			m_pData[m_iCount++] = tValue;
	}

	/// remove several elements by index
	void Remove ( int iIndex, int iCount=1 )
	{
		if ( iCount<=0 )
			return;

		assert ( iIndex>=0 && iIndex<m_iCount );
		assert ( iIndex+iCount<=m_iCount );

		m_iCount -= iCount;
		if ( m_iCount>iIndex )
			POLICY::Move ( m_pData + iIndex, m_pData + iIndex + iCount, m_iCount - iIndex );
		destroy_at ( m_iCount, iCount );
	}

	/// remove element by index, swapping it with the tail
	void RemoveFast ( int iIndex )
	{
		assert ( iIndex>=0 && iIndex<m_iCount );
		if ( iIndex!=--m_iCount )
			Swap ( m_pData[iIndex], m_pData[m_iCount] ); // fixme! What about POLICY::CopyOrSwap here?
		destroy_at ( m_iCount, 1 );
	}

	/// remove element by value (warning, linear O(n) search)
	bool RemoveValue ( T tValue )
	{
		for ( int i = 0; i<m_iCount; ++i )
			if ( m_pData[i]==tValue )
			{
				Remove ( i );
				return true;
			}
		return false;
	}

	/// remove element by value, asuming vec is sorted/uniq
	bool RemoveValueFromSorted ( T tValue )
	{
		T* pValue = VecTraits_T<T>::BinarySearch (tValue);
		if ( !pValue )
			return false;

		Remove ( pValue - Begin() );
		return true;
	}

	/// pop last value by ref (for constructed storage)
	template<typename S=STORE> typename std::enable_if<S::is_constructed, T&>::type
	Pop ()
	{
		assert ( m_iCount>0 );
		return m_pData[--m_iCount];
	}

	/// pop last value
	template<typename S=STORE> typename std::enable_if<!S::is_constructed, T>::type
	Pop ()
	{
		assert ( m_iCount>0 );
		auto res = m_pData[--m_iCount];
		destroy_at(m_iCount);
		return res;
	}

public:

	/// grow enough to hold iNewLimit-iDiscard entries, if needed.
	/// returns updated index of elem iDiscard.
	int DiscardAndReserve ( int64_t iDiscard, int64_t iNewLimit )
	{
		assert ( iNewLimit>=0 );
		assert ( iDiscard>=0 );

		// check that we really need to be called
		if ( iNewLimit<=m_iLimit )
			return iDiscard;

		if ( iDiscard>0 )
		{
			// align limit and size
			iNewLimit -= iDiscard;
			m_iCount = ( iDiscard<m_iCount ) ? ( m_iCount-iDiscard ) : 0;

			// check, if we still need to be called with aligned limit
			if ( iNewLimit<=m_iLimit )
			{
				if ( m_iCount ) // has something to move back
					POLICY::Move ( m_pData, m_pData+iDiscard, m_iCount );
				return 0;
			}
		}

		// calc new limit
		m_iLimit = LIMIT::Relimit ( m_iLimit, iNewLimit );

		// realloc
		T * pNew = nullptr;
		if ( m_iLimit )
		{
			pNew = STORE::Allocate ( m_iLimit );
			__analysis_assume ( m_iCount-iDiscard<=m_iLimit );
			if ( m_iCount ) // has something to copy from an old storage
				POLICY::Move ( pNew, m_pData+iDiscard, m_iCount );

			if ( pNew==m_pData )
				return 0;
		}
		Swap ( pNew, m_pData );
		STORE::Deallocate ( pNew );
		return 0;
	}

	/// grow enough to hold that much entries, if needed, but do *not* change the length
	void Reserve ( int64_t iNewLimit )
	{
		DiscardAndReserve ( 0, iNewLimit );
	}

	/// for non-copyable types - work like Reset() + Reserve()
	/// destroys previous dataset, allocate new one and set size to 0.
	template<typename S=STORE> typename std::enable_if<!S::is_constructed>::type
	Reserve_static ( int64_t iNewLimit )
	{
		// check that we really need to be called
		destroy_at ( 0, m_iCount );
		m_iCount = 0;

		if ( iNewLimit==m_iLimit )
			return;

		m_iLimit = iNewLimit;

		// realloc
		T * pNew = nullptr;
		pNew = STORE::Allocate ( m_iLimit );
		if ( pNew==m_pData )
			return;

		__analysis_assume ( m_iCount<=m_iLimit );
		Swap ( pNew, m_pData );
		STORE::Deallocate ( pNew );
	}

	/// ensure we have space for iGap more items (reserve more if necessary)
	inline void ReserveGap ( int iGap )
	{
		Reserve ( m_iCount + iGap );
	}

	/// resize
	template<typename S=STORE>
	typename std::enable_if<S::is_constructed>::type Resize ( int64_t iNewLength )
	{
		assert ( iNewLength>=0 );
		if ( iNewLength > m_iCount )
			Reserve ( iNewLength );
		m_iCount = iNewLength;
	}

	/// for non-constructed imply destroy when shrinking, of construct when widening
	template<typename S=STORE>
	typename std::enable_if<!S::is_constructed>::type Resize ( int64_t iNewLength )
	{
		assert ( iNewLength>=0 );
		if ( iNewLength < m_iCount )
			destroy_at ( iNewLength, m_iCount-iNewLength );
		else
		{
			Reserve ( iNewLength );
			construct_at ( m_iCount, iNewLength-m_iCount );
		}
		m_iCount = iNewLength;
	}

	// doesn't need default c-tr
	void Shrink ( int64_t iNewLength )
	{
		assert ( iNewLength<=m_iCount );
		destroy_at ( iNewLength, m_iCount-iNewLength );
		m_iCount = iNewLength;
	}

	/// reset
	void Reset ()
	{
		Shrink ( 0 );
		STORE::Deallocate ( m_pData );
		m_pData = nullptr;
		m_iLimit = 0;
	}

	/// Set whole vec to 0. For trivially copyable memset will be used
	template<typename S=STORE> typename std::enable_if<S::is_constructed>::type
	ZeroVec ()
	{
		POLICY::Zero ( m_pData, m_iLimit );
	}

	/// set the tail [m_iCount..m_iLimit) to zero
	void ZeroTail ()
	{
		if ( !m_pData )
			return;

		POLICY::Zero ( &m_pData[m_iCount], m_iLimit-m_iCount );
	}

	/// query current reserved size, in elements
	inline int GetLimit () const
	{
		return m_iLimit;
	}

	/// query currently allocated RAM, in bytes
	/// (could be > GetLengthBytes() since uses limit, not size)
	inline int64_t AllocatedBytes() const
	{
		return (int) m_iLimit*sizeof(T);
	}

public:
	/// filter unique
	void Uniq ()
	{
		if ( !m_iCount )
			return;

		Sort ();
		int64_t iLeft = sphUniq ( m_pData, m_iCount );
		Shrink ( iLeft );
	}

	/// copy + move
	// if provided lvalue, it will be copied into rhs via copy ctr, then swapped to *this
	// if provided rvalue, it will just pass to SwapData immediately.
	Vector_T &operator= ( Vector_T<T> rhs ) noexcept
	{
		SwapData ( rhs );
		return *this;
	}

	/// memmove N elements from raw pointer to the end
	/// works ONLY if T is POD type (i.e. may be simple memmoved)
	/// otherwize compile error will appear (if so, use typed version below).
	void Append ( const void * pData, int iN )
	{
		if ( iN<=0 )
			return;

		auto * pDst = AddN ( iN );
		POLICY::CopyVoid ( pDst, pData, iN );
	}

	/// append another vec to the end
	/// will use memmove (POD case), or one-by-one copying.
	template<typename S=STORE>
	typename std::enable_if<S::is_constructed>::type Append ( const VecTraits_T<T> &rhs )
	{
		if ( rhs.IsEmpty () )
			return;

		auto * pDst = AddN ( rhs.GetLength() );
		POLICY::Copy ( pDst, rhs.begin(), rhs.GetLength() );
	}

	/// append another vec to the end for non-constructed
	/// will construct in-place with copy c-tr
	template<typename S=STORE>
	typename std::enable_if<!S::is_constructed>::type Append ( const VecTraits_T<T> &rhs )
	{
		if ( rhs.IsEmpty () )
			return;

		auto iRhsLen = rhs.GetLength64();
		if ( m_iCount+iRhsLen>m_iLimit )
			Reserve ( m_iCount+iRhsLen );
		for ( int i=0; i<iRhsLen; ++i)
			new ( m_pData+m_iCount+i ) T ( rhs[i] );

		m_iCount += iRhsLen;
	}

	/// swap
	template<typename L=LIMIT, typename S=STORE> typename std::enable_if<!S::is_owned>::type
	SwapData ( Vector_T<T, POLICY, L, STORE> &rhs ) noexcept
	{
		Swap ( m_iCount, rhs.m_iCount );
		Swap ( m_iLimit, rhs.m_iLimit );
		Swap ( m_pData, rhs.m_pData );
	}

	/// leak
	template<typename S=STORE> typename std::enable_if<!S::is_owned, T*>::type
	LeakData ()
	{
		T * pData = m_pData;
		m_pData = nullptr;
		Reset();
		return pData;
	}

	/// adopt external buffer
	/// note that caller must himself then nullify origin pData to avoid double-deletion
	template<typename S=STORE>
	typename std::enable_if<!S::is_owned>::type
	AdoptData ( T * pData, int64_t iLen, int64_t iLimit )
	{
		assert ( iLen>=0 );
		assert ( iLimit>=0 );
		assert ( pData || iLimit==0 );
		assert ( iLen<=iLimit );
		Reset();
		m_pData = pData;
		m_iLimit = iLimit;
		m_iCount = iLen;
	}

	/// insert into a middle (will fail to compile for swap vector)
	void Insert ( int64_t iIndex, const T & tValue )
	{
		assert ( iIndex>=0 && iIndex<=this->m_iCount );

		if ( this->m_iCount>=this->m_iLimit )
			Reserve ( this->m_iCount+1 );

		for ( auto i = this->m_iCount-1; i>=iIndex; --i )
			POLICY::CopyOrSwap ( this->m_pData [ i+1 ], this->m_pData[i] );

		POLICY::CopyOrSwap ( this->m_pData[iIndex], tValue );
		++this->m_iCount;
	}

	/// insert into a middle by policy-defined copier
	void Insert ( int64_t iIndex, T &tValue )
	{
		assert ( iIndex>=0 && iIndex<=m_iCount );

		if ( this->m_iCount>=m_iLimit )
			Reserve ( this->m_iCount + 1 );

		for ( auto i = this->m_iCount - 1; i>=iIndex; --i )
			POLICY::CopyOrSwap ( this->m_pData[i + 1], this->m_pData[i] );

		POLICY::CopyOrSwap ( this->m_pData[iIndex], tValue );
		++this->m_iCount;
	}

protected:
	int64_t		m_iLimit = 0;		///< entries allocated

	template<typename S=STORE>
	typename std::enable_if<S::is_constructed>::type destroy_at ( int64_t, int64_t ) {}

	template<typename S=STORE>
	typename std::enable_if<S::is_constructed>::type construct_at ( int64_t, int64_t ) {}

	template<typename S=STORE>
	typename std::enable_if<!S::is_constructed>::type destroy_at ( int64_t iIndex, int64_t iCount )
	{
		for ( int64_t i = 0; i<iCount; ++i )
			m_pData[iIndex+i].~T ();
	}

	template<typename S=STORE>
	typename std::enable_if<!S::is_constructed>::type construct_at ( int64_t iIndex, int64_t iCount )
	{
		assert ( m_pData );
		for ( int64_t i = 0; i<iCount; ++i )
			new ( m_pData+iIndex+i ) T();
	}
};

} // namespace sph

#define ARRAY_FOREACH(_index,_array) \
	for ( int _index=0; _index<_array.GetLength(); ++_index )

#define ARRAY_FOREACH_COND(_index,_array,_cond) \
	for ( int _index=0; _index<_array.GetLength() && (_cond); ++_index )

#define ARRAY_CONSTFOREACH(_index,_array) \
	for ( int _index=0, _bound=_array.GetLength(); _index<_bound; ++_index )

#define ARRAY_CONSTFOREACH_COND(_index,_array,_cond) \
	for ( int _index=0, _bound=_array.GetLength(); _index<_bound && (_cond); ++_index )

//////////////////////////////////////////////////////////////////////////

/// old well-known vector
template < typename T, typename R=sph::DefaultRelimit >
using CSphVector = sph::Vector_T < T, sph::DefaultCopy_T<T>, R >;

template < typename T, typename R=sph::DefaultRelimit, int STATICSIZE=4096/sizeof(T) >
using LazyVector_T = sph::Vector_T<T, sph::DefaultCopy_T<T>, R, sph::LazyStorage_T<T, STATICSIZE> >;

/// swap-vector
template < typename T >
using CSphSwapVector = sph::Vector_T < T, sph::SwapCopy_T<T> >;

/// tight-vector
template < typename T >
using CSphTightVector =  CSphVector < T, sph::TightRelimit >;

/// raw vector for non-default-constructibles
template<typename T>
using RawVector_T = sph::Vector_T<T, sph::SwapCopy_T<T>, sph::DefaultRelimit, sph::RawStorage_T<T>>;

//////////////////////////////////////////////////////////////////////////

/// dynamically allocated fixed-size vector
template<typename T, class POLICY=sph::DefaultCopy_T <T>, class STORE=sph::DefaultStorage_T <T>>
class CSphFixedVector : public ISphNoncopyable, public VecTraits_T<T>, protected STORE
{
protected:
	using VecTraits_T<T>::m_pData;
	using VecTraits_T<T>::m_iCount;

public:
	explicit CSphFixedVector ( int64_t iSize )
	{
		m_iCount = iSize;
		assert ( iSize>=0 );
		m_pData = ( iSize>0 ) ? STORE::Allocate ( iSize ) : nullptr;
	}

	~CSphFixedVector ()
	{
		STORE::Deallocate ( m_pData );
	}

	CSphFixedVector ( CSphFixedVector&& rhs ) noexcept
	{
		SwapData(rhs);
	}

	CSphFixedVector & operator= ( CSphFixedVector rhs ) noexcept
	{
		SwapData(rhs);
		return *this;
	}

	void Reset ( int64_t iSize )
	{
		assert ( iSize>=0 );
		if ( iSize==m_iCount )
			return;

		STORE::Deallocate ( m_pData );
		m_pData = ( iSize>0 ) ? STORE::Allocate ( iSize ) : nullptr;
		m_iCount = iSize;
	}

	void CopyFrom ( const VecTraits_T<T>& dOrigin )
	{
		Reset ( dOrigin.GetLength() );
		POLICY::Copy ( m_pData, dOrigin.begin(), dOrigin.GetLength() );
	}

	template<typename S=STORE> typename std::enable_if<!S::is_owned, T*>::type
	LeakData ()
	{
		T * pData = m_pData;
		m_pData = nullptr;
		Reset ( 0 );
		return pData;
	}

	/// swap
	template<typename S=STORE> typename std::enable_if<!S::is_owned>::type
	SwapData ( CSphFixedVector<T> & rhs ) noexcept
	{
		Swap ( m_pData, rhs.m_pData );
		Swap ( m_iCount, rhs.m_iCount );
	}

	template<typename S=STORE>
	typename std::enable_if<S::is_constructed && !S::is_owned>::type
	Set ( T * pData, int64_t iSize )
	{
		m_pData = pData;
		m_iCount = iSize;
	}

	/// Set whole vec to 0. For trivially copyable memset will be used
	void ZeroVec ()
	{
		POLICY::Zero ( m_pData, m_iCount );
	}
};

//////////////////////////////////////////////////////////////////////////

/// simple dynamic hash
/// implementation: fixed-size bucket + chaining
/// keeps the order, so Iterate() return the entries in the order they was inserted
/// WARNING: slow copy
template < typename T, typename KEY, typename HASHFUNC, int LENGTH >
class CSphOrderedHash
{
public:
	using KeyValue_t = std::pair<KEY, T>;

protected:
	struct HashEntry_t : public KeyValue_t // key, data, owned by the hash
	{
		HashEntry_t *	m_pNextByHash = nullptr;	///< next entry in hash list
		HashEntry_t *	m_pPrevByOrder = nullptr;	///< prev entry in the insertion order
		HashEntry_t *	m_pNextByOrder = nullptr;	///< next entry in the insertion order
	};


protected:
	HashEntry_t *	m_dHash [ LENGTH ];			///< all the hash entries
	HashEntry_t *	m_pFirstByOrder = nullptr;	///< first entry in the insertion order
	HashEntry_t *	m_pLastByOrder = nullptr;	///< last entry in the insertion order
	int				m_iLength = 0;				///< entries count

protected:

	inline unsigned int HashPos ( const KEY & tKey ) const
	{
		return ( ( unsigned int ) HASHFUNC::Hash ( tKey ) ) % LENGTH;
	}

	/// find entry by key
	HashEntry_t * FindByKey ( const KEY & tKey ) const
	{
		HashEntry_t * pEntry = m_dHash[HashPos ( tKey )];

		while ( pEntry )
		{
			if ( pEntry->first==tKey )
				return pEntry;
			pEntry = pEntry->m_pNextByHash;
		}
		return nullptr;
	}

	HashEntry_t * AddImpl ( const KEY &tKey )
	{
		// check if this key is already hashed
		HashEntry_t ** ppEntry = &m_dHash[HashPos ( tKey )];
		HashEntry_t * pEntry = *ppEntry;
		while ( pEntry )
		{
			if ( pEntry->first==tKey )
				return nullptr;

			ppEntry = &pEntry->m_pNextByHash;
			pEntry = pEntry->m_pNextByHash;
		}

		// it's not; let's add the entry
		assert ( !pEntry );
		assert ( !*ppEntry );

		pEntry = new HashEntry_t;
		pEntry->first = tKey;

		*ppEntry = pEntry;

		if ( !m_pFirstByOrder )
			m_pFirstByOrder = pEntry;

		if ( m_pLastByOrder )
		{
			assert ( !m_pLastByOrder->m_pNextByOrder );
			assert ( !pEntry->m_pNextByOrder );
			m_pLastByOrder->m_pNextByOrder = pEntry;
			pEntry->m_pPrevByOrder = m_pLastByOrder;
		}
		m_pLastByOrder = pEntry;

		++m_iLength;
		return pEntry;
	}

public:
	/// ctor
	CSphOrderedHash ()
	{
		for ( auto &pHash : m_dHash )
			pHash = nullptr;
	}

	/// dtor
	~CSphOrderedHash ()
	{
		Reset ();
	}

	/// reset
	void Reset ()
	{
		assert ( ( m_pFirstByOrder && m_iLength ) || ( !m_pFirstByOrder && !m_iLength ) );
		HashEntry_t * pKill = m_pFirstByOrder;
		while ( pKill )
		{
			HashEntry_t * pNext = pKill->m_pNextByOrder;
			SafeDelete ( pKill );
			pKill = pNext;
		}

		for ( auto &pHash : m_dHash )
			pHash = nullptr;

		m_pFirstByOrder = nullptr;
		m_pLastByOrder = nullptr;
		m_pIterator = nullptr;
		m_iLength = 0;
	}

	/// add new entry
	/// returns true on success
	/// returns false if this key is already hashed
	bool Add ( T&& tValue, const KEY & tKey )
	{
		// check if this key is already hashed
		HashEntry_t * pEntry = AddImpl ( tKey );
		if ( !pEntry )
			return false;
		pEntry->second = std::move ( tValue );
		return true;
	}

	bool Add ( const T & tValue, const KEY & tKey )
	{
		// check if this key is already hashed
		HashEntry_t * pEntry = AddImpl ( tKey );
		if ( !pEntry )
			return false;
		pEntry->second = tValue;
		return true;
	}

	/// add new entry
	/// returns ref to just intersed or previously existed value
	T & AddUnique ( const KEY & tKey )
	{
		// check if this key is already hashed
		HashEntry_t ** ppEntry = &m_dHash[HashPos ( tKey )];
		HashEntry_t * pEntry = *ppEntry;

		while ( pEntry )
		{
			if ( pEntry->first==tKey )
				return pEntry->second;

			ppEntry = &pEntry->m_pNextByHash;
			pEntry = *ppEntry;
		}

		// it's not; let's add the entry
		assert ( !pEntry );

		pEntry = new HashEntry_t;
		pEntry->first = tKey;

		*ppEntry = pEntry;

		if ( !m_pFirstByOrder )
			m_pFirstByOrder = pEntry;

		if ( m_pLastByOrder )
		{
			assert ( !m_pLastByOrder->m_pNextByOrder );
			assert ( !pEntry->m_pNextByOrder );
			m_pLastByOrder->m_pNextByOrder = pEntry;
			pEntry->m_pPrevByOrder = m_pLastByOrder;
		}
		m_pLastByOrder = pEntry;

		++m_iLength;
		return pEntry->second;
	}

	/// delete an entry
	bool Delete ( const KEY & tKey )
	{
		auto uHash = HashPos ( tKey );
		HashEntry_t * pEntry = m_dHash [ uHash ];

		HashEntry_t * pPrevEntry = nullptr;
		HashEntry_t * pToDelete = nullptr;
		while ( pEntry )
		{
			if ( pEntry->first==tKey )
			{
				pToDelete = pEntry;
				if ( pPrevEntry )
					pPrevEntry->m_pNextByHash = pEntry->m_pNextByHash;
				else
					m_dHash [ uHash ] = pEntry->m_pNextByHash;

				break;
			}

			pPrevEntry = pEntry;
			pEntry = pEntry->m_pNextByHash;
		}

		if ( !pToDelete )
			return false;

		if ( pToDelete->m_pPrevByOrder )
			pToDelete->m_pPrevByOrder->m_pNextByOrder = pToDelete->m_pNextByOrder;
		else
			m_pFirstByOrder = pToDelete->m_pNextByOrder;

		if ( pToDelete->m_pNextByOrder )
			pToDelete->m_pNextByOrder->m_pPrevByOrder = pToDelete->m_pPrevByOrder;
		else
			m_pLastByOrder = pToDelete->m_pPrevByOrder;

		// step the iterator one item back - to gracefully hold deletion in iteration cycle
		if ( pToDelete==m_pIterator )
			m_pIterator = pToDelete->m_pPrevByOrder;

		SafeDelete ( pToDelete );
		--m_iLength;

		return true;
	}

	/// check if key exists
	bool Exists ( const KEY & tKey ) const
	{
		return FindByKey ( tKey )!=nullptr;
	}

	/// get value pointer by key
	T * operator () ( const KEY & tKey ) const
	{
		HashEntry_t * pEntry = FindByKey ( tKey );
		return pEntry ? &pEntry->second : nullptr;
	}

	/// get value reference by key, asserting that the key exists in hash
	T & operator [] ( const KEY & tKey ) const
	{
		HashEntry_t * pEntry = FindByKey ( tKey );
		assert ( pEntry && "hash missing value in operator []" );

		return pEntry->second;
	}

	/// copying ctor
	CSphOrderedHash ( const CSphOrderedHash& rhs )
	    : CSphOrderedHash ()
	{
		for ( rhs.IterateStart (); rhs.IterateNext (); )
			Add ( rhs.IterateGet (), rhs.IterateGetKey ());
	}

	/// moving ctor
	CSphOrderedHash ( CSphOrderedHash&& rhs ) noexcept
		: CSphOrderedHash ()
	{
		Swap(rhs);
	}

	void Swap ( CSphOrderedHash& rhs ) noexcept
	{
		HashEntry_t* dFoo[LENGTH];
		memcpy ( dFoo, m_dHash, LENGTH * sizeof ( HashEntry_t* ));
		memcpy ( m_dHash, rhs.m_dHash, LENGTH * sizeof ( HashEntry_t* ));
		memcpy ( rhs.m_dHash, dFoo, LENGTH * sizeof ( HashEntry_t* ));
		::Swap ( m_pFirstByOrder, rhs.m_pFirstByOrder );
		::Swap ( m_pLastByOrder, rhs.m_pLastByOrder );
		::Swap ( m_iLength, rhs.m_iLength );
	}

	/// copying & moving
	CSphOrderedHash& operator= ( CSphOrderedHash rhs )
	{
		Swap ( rhs );
		return *this;
	}

	/// length query
	int GetLength () const
	{
		return m_iLength;
	}

public:
	/// start iterating
	void IterateStart () const
	{
		m_pIterator = nullptr;
	}

	/// go to next existing entry
	bool IterateNext () const
	{
		m_pIterator = m_pIterator ? m_pIterator->m_pNextByOrder : m_pFirstByOrder;
		return m_pIterator!=nullptr;
	}

	/// get entry value
	T & IterateGet () const
	{
		assert ( m_pIterator );
		return m_pIterator->second;
	}

	/// get entry key
	const KEY & IterateGetKey () const
	{
		assert ( m_pIterator );
		return m_pIterator->first;
	}

	/// go to next existing entry in terms of external independed iterator
	bool IterateNext ( void ** ppCookie ) const
	{
		auto ** ppIterator = reinterpret_cast < HashEntry_t** > ( ppCookie );
		*ppIterator = ( *ppIterator ) ? ( *ppIterator )->m_pNextByOrder : m_pFirstByOrder;
		return ( *ppIterator )!=nullptr;
	}

	/// get entry value in terms of external independed iterator
	static T & IterateGet ( void ** ppCookie )
	{
		assert ( ppCookie );
		auto ** ppIterator = reinterpret_cast < HashEntry_t** > ( ppCookie );
		assert ( *ppIterator );
		return ( *ppIterator )->second;
	}

	/// get entry key in terms of external independed iterator
	static const KEY & IterateGetKey ( void ** ppCookie )
	{
		assert ( ppCookie );
		auto ** ppIterator = reinterpret_cast < HashEntry_t** > ( ppCookie );
		assert ( *ppIterator );
		return ( *ppIterator )->first;
	}

public:

	class Iterator_c
	{
		HashEntry_t* m_pIterator = nullptr;
	public:
		explicit Iterator_c ( HashEntry_t * pIterator=nullptr)
			: m_pIterator ( pIterator ) {}

		KeyValue_t& operator*() { return *m_pIterator; };

		Iterator_c & operator++ ()
		{
			m_pIterator = m_pIterator->m_pNextByOrder;
			return *this;
		}

		bool operator!= ( const Iterator_c & rhs ) const
		{
			return m_pIterator!=rhs.m_pIterator;
		}
	};

	// c++11 style iteration
	Iterator_c begin () const
	{
		return Iterator_c(m_pFirstByOrder);
	}

	Iterator_c end() const
	{
		return Iterator_c(nullptr);
	}


private:
	/// current iterator
	mutable HashEntry_t *	m_pIterator = nullptr;
};

/// very popular and so, moved here
/// use integer values as hash values (like document IDs, for example)
struct IdentityHash_fn
{
	template <typename INT>
	static inline INT Hash ( INT iValue )	{ return iValue; }
};

/////////////////////////////////////////////////////////////////////////////

inline bool StrEq ( const char * l, const char * r )
{
	if ( !l || !r )
		return ( ( !r && !l ) || ( !r && l && !*l ) || ( !l && r && !*r ) );
	return strcmp ( l, r )==0;
}

inline bool StrEqN ( const char * l, const char * r )
{
	if ( !l || !r )
		return ( ( !r && !l ) || ( !r && l && !*l ) || ( !l && r && !*r ) );
	return strcasecmp ( l, r )==0;
}

/// immutable C string proxy
struct CSphString
{
protected:
	char *				m_sValue = nullptr;
	// Empty ("") string optimization.
	// added
	//static char EMPTY[];

private:
	/// safety gap after the string end; for instance, UTF-8 Russian stemmer
	/// which treats strings as 16-bit word sequences needs this in some cases.
	/// note that this zero-filled gap does NOT include trailing C-string zero,
	/// and does NOT affect strlen() as well.
	static const int	SAFETY_GAP = 4;

	inline void SafeFree ()
	{ if ( m_sValue ) SafeDeleteArray ( m_sValue ); }
	//{ if ( m_sValue!=EMPTY ) SafeDeleteArray ( m_sValue ); }

public:
	CSphString () = default;

	// take a note this is not an explicit constructor
	// so a lot of silent constructing and deleting of strings is possible
	// Example:
	// SmallStringHash_T<int> hHash;
	// ...
	// hHash.Exists ( "asdf" ); // implicit CSphString construction and deletion here
	CSphString ( const CSphString & rhs )
	{
		if (!rhs.m_sValue)
			return;
		else if ( rhs.m_sValue[0]=='\0' )
		{
			//m_sValue = EMPTY;
			m_sValue = nullptr;
		} else
		{
			auto iLen = 1 + (int)strlen ( rhs.m_sValue ) + 1;
			m_sValue = new char[iLen + SAFETY_GAP];

			memcpy ( m_sValue, rhs.m_sValue, iLen ); // NOLINT
			memset ( m_sValue + iLen, 0, SAFETY_GAP );
		}
	}

	CSphString ( CSphString&& rhs ) noexcept
	{
		Swap(rhs);
	}

	~CSphString ()
	{
		SafeFree();
	}

	const char * cstr () const
	{
		return m_sValue;
	}

	const char * scstr() const
	{
		return m_sValue ? m_sValue : nullptr; //EMPTY;
	}

	inline bool operator == ( const char * t ) const
	{
		return StrEq ( t, m_sValue );
	}

	inline bool operator == ( const CSphString & t ) const
	{
		return operator==( t.cstr() );
	}

	inline bool operator != ( const CSphString & t ) const
	{
		return !operator==( t );
	}

	bool operator != ( const char * t ) const
	{
		return !operator==( t );
	}

	// compare ignoring case
	inline bool EqN ( const char * t ) const
	{
		return StrEqN ( t, m_sValue );
	}

	inline bool EqN ( const CSphString &t ) const
	{
		return EqN ( t.cstr () );
	}

	CSphString ( const char * sString ) // NOLINT
	{
		if ( sString )
		{
			if ( sString[0]=='\0' )
			{
				m_sValue = nullptr; //EMPTY;
			} else
			{
				auto iLen = (int) strlen(sString);
				m_sValue = new char [ iLen+SAFETY_GAP+1 ];
				memcpy ( m_sValue, sString, iLen ); // NOLINT
				memset ( m_sValue+iLen, 0, SAFETY_GAP+1 );
			}
		}
	}

	CSphString ( const char * sValue, int iLen )
	{
		SetBinary ( sValue, iLen );
	}

	// pass by value - replaces both copy and move assignments.
	CSphString & operator = ( CSphString rhs )
	{
		Swap (rhs);
		return *this;
	}

	CSphString SubString ( int iStart, int iCount ) const
	{
		#ifndef NDEBUG
		auto iLen = (int) strlen(m_sValue);
		iCount = Min( iLen - iStart, iCount );
		#endif
		assert ( iStart>=0 && iStart<iLen );
		assert ( iCount>0 );
		assert ( (iStart+iCount)<=iLen );

		CSphString sRes;
		sRes.m_sValue = new char [ 1+SAFETY_GAP+iCount ];
		strncpy ( sRes.m_sValue, m_sValue+iStart, iCount );
		memset ( sRes.m_sValue+iCount, 0, 1+SAFETY_GAP );
		return sRes;
	}

	// tries to reuse memory buffer, but calls Length() every time
	// hope this won't kill performance on a huge strings
	void SetBinary ( const char * sValue, int iLen )
	{
		if ( Length ()<( iLen + SAFETY_GAP + 1 ) )
		{
			SafeFree ();
			if ( !sValue )
				m_sValue = nullptr; //EMPTY;
			else
			{
				m_sValue = new char [ 1+SAFETY_GAP+iLen ];
				memcpy ( m_sValue, sValue, iLen );
				memset ( m_sValue+iLen, 0, 1+SAFETY_GAP );
			}
			return;
		}

		if ( sValue && iLen )
		{
			memcpy ( m_sValue, sValue, iLen );
			memset ( m_sValue + iLen, 0, 1 + SAFETY_GAP );
		} else
		{
			SafeFree ();
			m_sValue = nullptr; //EMPTY;
		}
	}

	void Reserve ( int iLen )
	{
		SafeFree ();
		m_sValue = new char [ 1+SAFETY_GAP+iLen ];
		memset ( m_sValue, 0, 1+SAFETY_GAP+iLen );
	}

	const CSphString & SetSprintf ( const char * sTemplate, ... ) __attribute__ ( ( format ( printf, 2, 3 ) ) )
	{
		char sBuf[1024];
		va_list ap;

		va_start ( ap, sTemplate );
		vsnprintf ( sBuf, sizeof(sBuf), sTemplate, ap );
		va_end ( ap );

		(*this) = sBuf;
		return (*this);
	}

	/// format value using provided va_list
	const CSphString & SetSprintfVa ( const char * sTemplate, va_list ap )
	{
		char sBuf[1024];
		vsnprintf ( sBuf, sizeof(sBuf), sTemplate, ap );

		(*this) = sBuf;
		return (*this);
	}
	/// \return true if internal char* ptr is null, of value is empty.
	bool IsEmpty () const
	{
		if ( !m_sValue )
			return true;
		return ( (*m_sValue)=='\0' );
	}

	CSphString & ToLower ()
	{
		if ( m_sValue )
			for ( char * s=m_sValue; *s; s++ )
				*s = (char) tolower ( *s );
		return *this;
	}

	CSphString & ToUpper ()
	{
		if ( m_sValue )
			for ( char * s=m_sValue; *s; s++ )
				*s = (char) toupper ( *s );
		return *this;
	}

	void Swap ( CSphString & rhs )
	{
		::Swap ( m_sValue, rhs.m_sValue );
	}

	/// \return true if the string begins with sPrefix
	bool Begins ( const char * sPrefix ) const
	{
		if ( !m_sValue || !sPrefix )
			return false;
		return strncmp ( m_sValue, sPrefix, strlen(sPrefix) )==0;
	}

	/// \return true if the string ends with sSuffix
	bool Ends ( const char * sSuffix ) const
	{
		if ( !m_sValue || !sSuffix )
			return false;

		auto iVal = (int) strlen ( m_sValue );
		auto iSuffix = (int) strlen ( sSuffix );
		if ( iVal<iSuffix )
			return false;
		return strncmp ( m_sValue+iVal-iSuffix, sSuffix, iSuffix )==0;
	}

	/// trim leading and trailing spaces
	CSphString & Trim()
	{
		if ( m_sValue )
		{
			const char * sStart = m_sValue;
			const char * sEnd = m_sValue + strlen(m_sValue) - 1;
			while ( sStart<=sEnd && isspace ( (unsigned char)*sStart ) ) sStart++;
			while ( sStart<=sEnd && isspace ( (unsigned char)*sEnd ) ) sEnd--;
			memmove ( m_sValue, sStart, sEnd-sStart+1 );
			m_sValue [ sEnd-sStart+1 ] = '\0';
		}

		return *this;
	}

	int Length () const
	{
		return m_sValue ? (int)strlen(m_sValue) : 0;
	}

	/// \return internal string and releases it from being destroyed in d-tr
	char * Leak ()
	{
		if ( m_sValue==nullptr ) //EMPTY )
		{
			m_sValue = nullptr;
			auto * pBuf = new char[1];
			pBuf[0] = '\0';
			return pBuf;
		}
		char * pBuf = m_sValue;
		m_sValue = nullptr;
		return pBuf;
	}

	/// \return internal string and releases it from being destroyed in d-tr
	void LeakToVec ( CSphVector<BYTE> &dVec )
	{
		if ( m_sValue== nullptr ) //EMPTY )
		{
			m_sValue = nullptr;
			auto * pBuf = new char[1];
			pBuf[0] = '\0';
			dVec.AdoptData ((BYTE*)pBuf,0,1);
			return;
		}
		int iLen = Length();
		dVec.AdoptData ( ( BYTE * ) m_sValue, iLen, iLen + 1 + SAFETY_GAP );
		m_sValue = nullptr;
	}

	/// take string from outside and 'adopt' it as own child.
	void Adopt ( char ** sValue )
	{
		SafeFree ();
		m_sValue = *sValue;
		*sValue = nullptr;
	}

	void Adopt ( char * && sValue )
	{
		SafeFree ();
		m_sValue = sValue;
		sValue = nullptr;
	}

	/// compares using strcmp
	bool operator < ( const CSphString & b ) const
	{
		if ( !m_sValue && !b.m_sValue )
			return false;
		if ( !m_sValue || !b.m_sValue )
			return !m_sValue;
		return strcmp ( m_sValue, b.m_sValue ) < 0;
	}

	void Unquote()
	{
		int l = Length();
		if ( l && m_sValue[0]=='\'' && m_sValue[l-1]=='\'' )
		{
			memmove ( m_sValue, m_sValue+1, l-2 );
			m_sValue[l-2] = '\0';
		}
	}

	static int GetGap () { return SAFETY_GAP; }

	explicit operator ByteBlob_t () const
	{
		return { (const BYTE*) m_sValue, Length() };
	}
};

/// string swapper
inline void Swap ( CSphString & v1, CSphString & v2 )
{
	v1.Swap ( v2 );
}

// commonly used vector of strings
using StrVec_t = CSphVector<CSphString>;

// vector of byte vectors
using BlobVec_t = CSphVector<CSphVector<BYTE> >;

/////////////////////////////////////////////////////////////////////////////

/// immutable string/int/float variant list proxy
/// used in config parsing
struct CSphVariant
{
protected:
	CSphString		m_sValue;
	int				m_iValue = 0;
	int64_t			m_i64Value = 0;
	float			m_fValue = 0.0f;

public:
	CSphVariant *	m_pNext = nullptr;
	// tags are used for handling multiple same keys
	bool			m_bTag = false; // 'true' means override - no multi-valued; 'false' means multi-valued - chain them
	int				m_iTag = 0; // stores order like in config file

public:
	/// default ctor
	CSphVariant () = default;


	/// ctor from C string
	explicit CSphVariant ( const char * sString, int iTag=0 )
		: m_sValue ( sString )
		, m_iValue ( sString ? atoi ( sString ) : 0 )
		, m_i64Value ( sString ? (int64_t)strtoull ( sString, nullptr, 10 ) : 0 )
		, m_fValue ( sString ? (float)atof ( sString ) : 0.0f )
		, m_iTag ( iTag )
	{
	}

	/// copy ctor
	CSphVariant ( const CSphVariant& rhs )
	{
		if ( rhs.m_pNext )
			m_pNext = new CSphVariant ( *rhs.m_pNext );

		m_sValue = rhs.m_sValue;
		m_iValue = rhs.m_iValue;
		m_i64Value = rhs.m_i64Value;
		m_fValue = rhs.m_fValue;
		m_bTag = rhs.m_bTag;
		m_iTag = rhs.m_iTag;
	}

	/// move ctor
	CSphVariant ( CSphVariant&& rhs ) noexcept
		: m_pNext ( nullptr ) // otherwise trash in uninitialized m_pNext causes crash in dtr
	{
		Swap ( rhs );
	}


	/// default dtor
	/// WARNING: automatically frees linked items!
	~CSphVariant ()
	{
		SafeDelete ( m_pNext );
	}

	const char * cstr() const { return m_sValue.cstr(); }

	const CSphString & strval () const { return m_sValue; }
	int intval () const	{ return m_iValue; }
	int64_t int64val () const { return m_i64Value; }
	float floatval () const	{ return m_fValue; }

	/// default copy operator
	CSphVariant& operator= ( CSphVariant rhs )
	{
		Swap ( rhs );
		return *this;
	}

	void Swap ( CSphVariant& rhs ) noexcept
	{
		::Swap ( m_pNext, rhs.m_pNext );
		::Swap ( m_sValue, rhs.m_sValue );
		::Swap ( m_iValue, rhs.m_iValue );
		::Swap ( m_i64Value, rhs.m_i64Value );
		::Swap ( m_fValue, rhs.m_fValue );
		::Swap ( m_bTag, rhs.m_bTag );
		::Swap ( m_iTag, rhs.m_iTag );
	}

	bool operator== ( const char * s ) const { return m_sValue==s; }
	bool operator!= ( const char * s ) const { return m_sValue!=s; }
};

/// text delimiter
/// returns "" first time, then defined delimiter starting from 2-nd call
/// NOTE that using >1 call in one chain like out << comma << "foo" << comma << "bar" is NOT defined,
/// since order of calling 2 commas here is undefined (so, you may take "foo, bar", but may ", foobar" also).
/// Use out << comma << "foo"; out << comma << "bar"; in the case
using Str_t = std::pair<const char*, int>;
const Str_t dEmptyStr = { "", 0 };
inline bool IsEmpty ( const Str_t & dBlob ) { return dBlob.second==0; }
inline bool IsFilled ( const Str_t & dBlob ) { return dBlob.first && dBlob.second>0; }
inline Str_t FromSz ( const char * szString ) { return { szString, (int) strlen ( szString ) }; }
inline Str_t FromStr ( const CSphString& sString ) { return { sString.cstr(), (int) sString.Length() }; }

class Comma_c
{
protected:
	Str_t m_sComma = dEmptyStr;
	bool m_bStarted = false;

public:
	// standalone - cast to 'Str_t' when necessary
	explicit Comma_c ( const char * sDelim=nullptr )
	{
		m_sComma = sDelim ? Str_t { sDelim, (int) strlen( sDelim ) } : dEmptyStr;
	}

	explicit Comma_c( Str_t sDelim ): m_sComma( std::move( sDelim )) {}

	Comma_c ( const Comma_c& rhs ) = default;
	Comma_c ( Comma_c&& rhs) noexcept = default;
	Comma_c& operator= ( Comma_c rhs)
	{
		Swap(rhs);
		return *this;
	}

	void Swap ( Comma_c& rhs ) noexcept
	{
		m_sComma.swap(rhs.m_sComma);
		::Swap ( m_bStarted, rhs.m_bStarted );
	}

	inline bool Started() const { return m_bStarted; };

	operator Str_t()
	{
		if ( m_bStarted )
			return m_sComma;
		m_bStarted = true;
		return dEmptyStr;
	}
};

using StrBlock_t = std::tuple<Str_t, Str_t, Str_t>;

// common pattern
const StrBlock_t dEmptyBl { dEmptyStr, dEmptyStr, dEmptyStr }; // empty
const StrBlock_t dJsonObj { {",",1}, {"{",1}, {"}",1} }; // json object
const StrBlock_t dJsonArr { {",",1}, {"[",1}, {"]",1} }; // json array
const StrBlock_t dBracketsComma { {",",1}, {"(",1}, {")",1} }; // collection in brackets, comma separated

const StrBlock_t dJsonObjW { {",\n",2}, {"{\n",2}, {"\n}",2} }; // json object with formatting
const StrBlock_t dJsonArrW { {",\n",2}, {"[\n",2}, {"\n]",2} }; // json array with formatting

/// string builder
/// somewhat quicker than a series of SetSprintf()s
/// lets you build strings bigger than 1024 bytes, too
class StringBuilder_c : public ISphNoncopyable
{
	class LazyComma_c;

public:
		// creates and m.b. start block
						StringBuilder_c ( const char * sDel = nullptr, const char * sPref = nullptr, const char * sTerm = nullptr );
						StringBuilder_c ( StringBuilder_c&& rhs ) noexcept;
						~StringBuilder_c ();

	void				Swap ( StringBuilder_c& rhs ) noexcept;

	// reset to initial state
	void				Clear();

	// get current build value
	const char *		cstr() const { return m_szBuffer ? m_szBuffer : ""; }
	explicit operator	CSphString() const { return CSphString (cstr()); }

	// move out (de-own) value
	BYTE *				Leak();
	void				MoveTo ( CSphString &sTarget ); // leak to string

	// get state
	bool				IsEmpty () const { return !m_szBuffer || m_szBuffer[0]=='\0'; }
	inline int			GetLength () const { return m_iUsed; }

	// different kind of fullfillments
	StringBuilder_c &	AppendChunk ( const Str_t& sChunk, char cQuote = '\0' );
	StringBuilder_c &	AppendString ( const CSphString & sText, char cQuote = '\0' );

	StringBuilder_c &	operator = ( StringBuilder_c rhs ) noexcept;
	StringBuilder_c &	operator += ( const char * sText );
	StringBuilder_c &	operator += ( const Str_t& sChunk );
	StringBuilder_c &	operator << ( const VecTraits_T<char> &sText );
	StringBuilder_c &	operator << ( const char * sText ) { return *this += sText; }
	StringBuilder_c &	operator << ( const CSphString &sText ) { return *this += sText.cstr (); }
	StringBuilder_c &	operator << ( const CSphVariant &sText )	{ return *this += sText.cstr (); }
	StringBuilder_c &	operator << ( Comma_c& dComma ) { return *this += dComma; }

	StringBuilder_c &	operator << ( int iVal );
	StringBuilder_c &	operator << ( long iVal );
	StringBuilder_c &	operator << ( long long iVal );

	StringBuilder_c &	operator << ( unsigned int uVal );
	StringBuilder_c &	operator << ( unsigned long uVal );
	StringBuilder_c &	operator << ( unsigned long long uVal );

	StringBuilder_c &	operator << ( float fVal );
	StringBuilder_c &	operator << ( double fVal );
	StringBuilder_c &	operator << ( void* pVal );

	// support for sph::Sprintf - emulate POD 'char*'
	inline StringBuilder_c &	operator ++() { GrowEnough ( 1 ); ++m_iUsed; return *this; }
	inline void					operator += (int i) { GrowEnough ( i ); m_iUsed += i; }

	// append 1 char despite any blocks.
	inline void			RawC ( char cChar ) { GrowEnough ( 1 ); *end () = cChar; ++m_iUsed; }
	void				AppendRawChunk ( Str_t sText ); // append without any commas
	StringBuilder_c &	SkipNextComma();
	StringBuilder_c &	AppendName ( const char * sName); // append

	// these use standard sprintf() inside
	StringBuilder_c &	vAppendf ( const char * sTemplate, va_list ap );
	StringBuilder_c &	Appendf ( const char * sTemplate, ... ) __attribute__ ( ( format ( printf, 2, 3 ) ) );

	// these use or own implementation sph::Sprintf which provides also some sugar
	StringBuilder_c &	vSprintf ( const char * sTemplate, va_list ap );
	StringBuilder_c &	Sprintf ( const char * sTemplate, ... );

	// comma manipulations
	// start new comma block; return index of it (for future possible reference in FinishBlocks())
	int					StartBlock ( const char * sDel = ", ", const char * sPref = nullptr, const char * sTerm = nullptr );
	int 				StartBlock( const StrBlock_t& dBlock );
	int					MuteBlock ();

	// finish and close last opened comma block.
	// bAllowEmpty - close empty block output nothing(default), or prefix/suffix pair (if any).
	void				FinishBlock ( bool bAllowEmpty = true );

	// finish and close all blocks including pLevels (by default - all blocks)
	void				FinishBlocks ( int iLevels = 0, bool bAllowEmpty = true );

	inline char *		begin() const { return m_szBuffer; }
	inline char *		end () const { return m_szBuffer + m_iUsed; }

	// shrink, if necessary, to be able to fit at least iLen more chars
	inline void GrowEnough ( int iLen )
	{
		if ( m_iUsed + iLen<m_iSize )
			return;

		Grow ( iLen );
	}

	void NtoA ( DWORD uVal );
	void NtoA ( int64_t iVal );
	void FtoA ( float fVal );

protected:
	static const BYTE GROW_STEP = 64; // how much to grow if no space left

	char *			m_szBuffer = nullptr;
	int				m_iSize = 0;
	int				m_iUsed = 0;
	CSphVector<LazyComma_c> m_dDelimiters;

	void			Grow ( int iLen ); // unconditionally shrink enough to place at least iLen more bytes

	inline void InitAddPrefix()
	{
		if ( !m_szBuffer )
			InitBuffer();

		assert ( m_iUsed==0 || m_iUsed<m_iSize );

		auto sPrefix = Delim();
		if ( sPrefix.second ) // prepend delimiter first...
		{
			GrowEnough ( sPrefix.second );
			memcpy ( m_szBuffer + m_iUsed, sPrefix.first, sPrefix.second );
			m_iUsed += sPrefix.second;
		}
	}

	const Str_t & Delim ()
	{
		if ( m_dDelimiters.IsEmpty ())
			return dEmptyStr;
		int iLast = m_dDelimiters.GetLength()-1;
		std::function<void()> fnApply = [this, &iLast, &fnApply]()
		{
			--iLast;
			if ( iLast>=0 )
				AppendRawChunk( m_dDelimiters[iLast].RawComma( fnApply ));
		};
		return m_dDelimiters.Last().RawComma( fnApply );
	}

private:
	void			NewBuffer ();
	void			InitBuffer ();

	// RAII comma for frequently used pattern of pushing into StringBuilder many values separated by ',', ';', etc.
	// When in scope, inject prefix before very first item, or delimiter before each next.
	class LazyComma_c : public Comma_c
	{
		bool m_bSkipNext = false;

	public:
		Str_t m_sPrefix = dEmptyStr;
		Str_t m_sSuffix = dEmptyStr;

		// c-tr for managed - linked StringBuilder will inject RawComma() on each call, terminator at end
		LazyComma_c ( const char * sDelim, const char * sPrefix, const char * sTerm );
		explicit LazyComma_c( const StrBlock_t& dBlock );
		LazyComma_c () = default;
		LazyComma_c ( const LazyComma_c & ) = default;
		LazyComma_c ( LazyComma_c && ) noexcept = default;
		LazyComma_c& operator= (LazyComma_c rhs)
		{
			Swap(rhs);
			return *this;
		}

		void Swap ( LazyComma_c & rhs ) noexcept
		{
			Comma_c::Swap ( rhs );
			m_sPrefix.swap ( rhs.m_sPrefix );
			m_sSuffix.swap ( rhs.m_sSuffix );
			::Swap ( m_bSkipNext, rhs.m_bSkipNext );
		}

		const Str_t & RawComma ( const std::function<void ()> & fnAddNext );

		void SkipNext ()
		{
			m_bSkipNext = true;
		}
	};
};

struct BaseQuotation_t
{
	// represents char for quote
	static const char cQuote = '\'';

	// returns true to chars need to escape
	inline static bool IsEscapeChar ( char c ) {return false;}

	// called if char need to escape to map into another
	inline static char GetEscapedChar ( char c ) { return c; }

	// replaces \t, \n, \r into spaces
	inline static char FixupSpace ( char c )
	{
		alignas ( 16 ) static const char dSpacesLookupTable[] = {
			0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,' ', ' ', 0x0b, 0x0c, ' ', 0x0e, 0x0f
		};
		return ( c & 0xF0 ) ? c : dSpacesLookupTable[(BYTE) c];
	}

	// if simultaneous escaping and fixup spaces - may use the fact that if char is escaping,
	// it will pass to GetEscapedChar, and will NOT be passed to FixupSpaces, so may optimize for speed
	inline static char FixupSpaceWithEscaping ( char c ) { return FixupSpace (c); }
};


namespace EscBld
{	// what kind of changes will do AppendEscaped of escaped string builder:
	enum eAct : BYTE
	{
		eNone		= 0, // [comma,] append raw text without changes
		eFixupSpace	= 1, // [comma,] change \t, \n, \r into spaces
		eEscape		= 2, // [comma,] all escaping according to provided interface
		eAll		= 3, // [comma,] escape and change spaces
		eSkipComma	= 4, // force to NOT prefix comma (if any active)
		eNoLimit	= 8, // internal - set if iLen is not set (i.e. -1). To convert conditions into switch cases
	};
}

template < typename T >
class EscapedStringBuilder_T : public StringBuilder_c
{
	inline bool AppendEmpty ( const char * sText )
	{
		if ( sText && *sText )
			return false;

		GrowEnough ( 1 );
		auto * pCur = end ();
		*pCur = '\0';
		return true;
	}

	inline bool AppendEmptyEscaped ( const char * sText )
	{
		if ( sText && *sText )
			return false;

		GrowEnough ( 3 );
		auto * pCur = end ();
		pCur[0] = T::cQuote;
		pCur[1] = T::cQuote;
		pCur[2] = '\0';
		m_iUsed += 2;
		return true;
	}

public:

	// dedicated EscBld::eEscape | EscBld::eSkipComma
	void AppendEscapedSkippingComma ( const char * sText )
	{
		if ( AppendEmptyEscaped ( sText ) )
			return;

		GrowEnough ( 3 ); // 2 quotes and terminator
		const char * pSrc = sText;
		auto * pCur = end ();
		auto pEnd = m_szBuffer + m_iSize;

		*pCur++ = T::cQuote;
		for ( ; *pSrc; ++pSrc, ++pCur )
		{
			char s = *pSrc;
			if ( T::IsEscapeChar ( s ) )
			{
				*pCur++ = '\\';
				*pCur = T::GetEscapedChar ( s );
			} else
				*pCur = s;

			if ( pCur>( pEnd-3 ) ) // need 1 ending quote + terminator
			{
				m_iUsed = pCur-m_szBuffer;
				GrowEnough ( 32 );
				pEnd = m_szBuffer+m_iSize;
				pCur = m_szBuffer+m_iUsed;
			}
		}
		*pCur++ = T::cQuote;
		*pCur = '\0';
		m_iUsed = pCur - m_szBuffer;
	}

	// dedicated EscBld::eEscape with comma
	void AppendEscapedWithComma ( const char * sText )
	{
		auto & sComma = Delim ();
		if ( sComma.second )
		{
			GrowEnough ( sComma.second );
			memcpy ( end (), sComma.first, sComma.second );
			m_iUsed += sComma.second;
		}

		if ( AppendEmptyEscaped ( sText ) )
			return;

		GrowEnough ( 3 ); // 2 quotes and terminator
		const char * pSrc = sText;
		auto * pCur = end ();
		auto pEnd = m_szBuffer + m_iSize;

		*pCur++ = T::cQuote;
		for ( ; *pSrc; ++pSrc, ++pCur )
		{
			char s = *pSrc;
			if ( T::IsEscapeChar ( s ) )
			{
				*pCur++ = '\\';
				*pCur = T::GetEscapedChar ( s );
			} else
				*pCur = s;

			if ( pCur>( pEnd-3 ) ) // need 1 ending quote + terminator
			{
				m_iUsed = pCur-m_szBuffer;
				GrowEnough ( 32 );
				pEnd = m_szBuffer+m_iSize;
				pCur = m_szBuffer+m_iUsed;
			}
		}
		*pCur++ = T::cQuote;
		*pCur = '\0';
		m_iUsed = pCur - m_szBuffer;
	}

	// dedicated EscBld::eEscape with comma with external len
	void AppendEscapedWithComma ( const char * sText, int iLen )
	{
		auto & sComma = Delim ();
		if ( sComma.second )
		{
			GrowEnough ( sComma.second );
			memcpy ( end (), sComma.first, sComma.second );
			m_iUsed += sComma.second;
		}

		if ( AppendEmptyEscaped ( sText ) )
			return;

		GrowEnough ( 3 ); // 2 quotes and terminator
		const char * pSrc = sText;
		auto * pCur = end ();
		auto pEnd = m_szBuffer + m_iSize;

		*pCur++ = T::cQuote;
		for ( ; iLen && *pSrc; ++pSrc, ++pCur, --iLen )
		{
			char s = *pSrc;
			if ( T::IsEscapeChar ( s ) )
			{
				*pCur++ = '\\';
				*pCur = T::GetEscapedChar ( s );
			} else
				*pCur = s;

			if ( pCur>( pEnd-3 ) ) // need 1 ending quote + terminator
			{
				m_iUsed = pCur-m_szBuffer;
				GrowEnough ( 32 );
				pEnd = m_szBuffer+m_iSize;
				pCur = m_szBuffer+m_iUsed;
			}
		}
		*pCur++ = T::cQuote;
		*pCur = '\0';
		m_iUsed = pCur - m_szBuffer;
	}

	// dedicated EscBld::eFixupSpace
	void FixupSpacesAndAppend ( const char * sText )
	{
		if ( AppendEmpty ( sText ) )
			return;

		auto& sComma = Delim ();
		if ( sComma.second )
		{
			GrowEnough ( sComma.second );
			memcpy ( end (), sComma.first, sComma.second );
			m_iUsed += sComma.second;
		}

		GrowEnough ( 1 ); // terminator
		const char * pSrc = sText;
		auto * pCur = end ();
		auto pEnd = m_szBuffer + m_iSize;

		for ( ; *pSrc; ++pSrc, ++pCur )
		{
			*pCur = T::FixupSpace ( *pSrc );

			if ( pCur>( pEnd-2 ) ) // need terminator
			{
				m_iUsed = pCur-m_szBuffer;
				GrowEnough ( 32 );
				pEnd = m_szBuffer+m_iSize;
				pCur = m_szBuffer+m_iUsed;
			}
		}
		*pCur = '\0';
		m_iUsed = pCur - m_szBuffer;
	}

	// dedicated EscBld::eAll (=EscBld::eFixupSpace | EscBld::eEscape )
	void FixupSpacedAndAppendEscaped ( const char * sText )
	{
		auto & sComma = Delim ();
		if ( sComma.second )
		{
			GrowEnough ( sComma.second );
			memcpy ( end (), sComma.first, sComma.second );
			m_iUsed += sComma.second;
		}

		if ( AppendEmptyEscaped ( sText ) )
			return;

		GrowEnough ( 3 ); // 2 quotes and terminator
		const char * pSrc = sText;
		auto * pCur = end ();
		auto pEnd = m_szBuffer+m_iSize;

		*pCur++ = T::cQuote;
		for ( ; *pSrc; ++pSrc, ++pCur )
		{
			char s = *pSrc;
			if ( T::IsEscapeChar ( s ) )
			{
				*pCur++ = '\\';
				*pCur = T::GetEscapedChar ( s );
			} else
				*pCur = T::FixupSpaceWithEscaping ( s );

			if ( pCur>( pEnd-3 ) ) // need 1 ending quote + terminator
			{
				m_iUsed = pCur-m_szBuffer;
				GrowEnough ( 32 );
				pEnd = m_szBuffer+m_iSize;
				pCur = m_szBuffer+m_iUsed;
			}
		}
		*pCur++ = T::cQuote;
		*pCur = '\0';
		m_iUsed = pCur-m_szBuffer;
	}

	// dedicated EscBld::eAll (=EscBld::eFixupSpace | EscBld::eEscape ) with external len
	void FixupSpacedAndAppendEscaped ( const char * sText, int iLen )
	{
		assert ( iLen>=0 );

		auto & sComma = Delim ();
		if ( sComma.second )
		{
			GrowEnough ( sComma.second );
			memcpy ( end (), sComma.first, sComma.second );
			m_iUsed += sComma.second;
		}

		if ( AppendEmptyEscaped ( sText ) )
			return;

		GrowEnough ( 3 ); // 2 quotes and terminator
		const char * pSrc = sText;
		auto * pCur = end ();
		auto pEnd = m_szBuffer+m_iSize;

		*pCur++ = T::cQuote;
		for ( ; iLen && *pSrc; ++pSrc, ++pCur, --iLen )
		{
			char s = *pSrc;
			if ( T::IsEscapeChar ( s ) )
			{
				*pCur++ = '\\';
				*pCur = T::GetEscapedChar ( s );
			} else
				*pCur = T::FixupSpaceWithEscaping ( s );

			if ( pCur>( pEnd-3 ) ) // need 1 ending quote + terminator
			{
				m_iUsed = pCur-m_szBuffer;
				GrowEnough ( 32 );
				pEnd = m_szBuffer+m_iSize;
				pCur = m_szBuffer+m_iUsed;
			}
		}
		*pCur++ = T::cQuote;
		*pCur = '\0';
		m_iUsed = pCur-m_szBuffer;
	}

	// generic implementation. Used this way in tests. For best performance consider to use specialized versions
	// (see selector switch inside) directly.
	void AppendEscaped ( const char * sText, BYTE eWhat=EscBld::eAll, int iLen=-1 )
	{
		if ( iLen==-1 )
			eWhat |= EscBld::eNoLimit;
		else
			eWhat &= ~EscBld::eNoLimit;

		// shortcuts to dedicated separate cases
		switch ( eWhat )
		{
		case ( EscBld::eEscape | EscBld::eSkipComma | EscBld::eNoLimit ):
			AppendEscapedSkippingComma ( sText );
			return;
		case ( EscBld::eEscape | EscBld::eNoLimit ):
			AppendEscapedWithComma ( sText );
			return;
		case ( EscBld::eEscape ):
			AppendEscapedWithComma ( sText, iLen );
			return;
		case ( EscBld::eFixupSpace | EscBld::eNoLimit ):
			FixupSpacesAndAppend ( sText );
			return;
		case ( EscBld::eAll | EscBld::eNoLimit ):
			FixupSpacedAndAppendEscaped ( sText );
			return;
		case ( EscBld::eAll ):
			FixupSpacedAndAppendEscaped ( sText, iLen );
			return;
		}

		if ( ( eWhat & EscBld::eEscape )==0 && AppendEmpty ( sText ) )
			return;

		// process comma
		if ( eWhat & EscBld::eSkipComma ) // assert no eEscape here, since it is hold separately already.
			eWhat -= EscBld::eSkipComma;
		else
		{
			auto sComma = Delim();
			if ( sComma.second )
			{
				GrowEnough ( sComma.second );
				memcpy ( end (), sComma.first, sComma.second );
				m_iUsed+=sComma.second;
			}
		}

		if ( ( eWhat & EscBld::eEscape ) && AppendEmptyEscaped ( sText ) )
			return;

		const char * pSrc = sText;
		int iFinalLen = 0;
		if ( eWhat & EscBld::eEscape )
		{
			if ( eWhat & EscBld::eNoLimit )
			{
				eWhat &= ~EscBld::eNoLimit;
				for ( ; *pSrc; ++pSrc )
					if ( T::IsEscapeChar (*pSrc) )
						++iFinalLen;
			} else
			{
				for ( auto iL=0; *pSrc && iL<iLen; ++pSrc, ++iL )
					if ( T::IsEscapeChar ( *pSrc ) )
						++iFinalLen;
			}
			iLen = (int) (pSrc - sText);
			iFinalLen += iLen+2; // 2 quotes: 1 prefix, 2 postfix.
		} else if ( eWhat & EscBld::eNoLimit )
		{
			eWhat &= ~EscBld::eNoLimit;
			iFinalLen = iLen = (int) strlen (sText);
		}
		else
			iFinalLen = iLen;

		GrowEnough ( iFinalLen+1 ); // + zero terminator

		auto * pCur = end();
		switch (eWhat)
		{
		case EscBld::eNone:
			memcpy ( pCur, sText, iFinalLen );
			pCur += iFinalLen;
			break;
		case EscBld::eFixupSpace:  // EscBld::eNoLimit hold especially
			for ( ; iLen; --iLen )
			{
				*pCur++ = T::FixupSpace( *sText++ );
			}
			break;
		case EscBld::eEscape:
			*pCur++ = T::cQuote;
			for ( ; iLen; --iLen )
			{
				char s = *sText++;
				if ( T::IsEscapeChar ( s ) )
				{
					*pCur++ = '\\';
					*pCur++ = T::GetEscapedChar ( s );
				} else
					*pCur++ = s;
			}
			*pCur++ = T::cQuote;
			break;
		case EscBld::eAll:
		default:
			*pCur++ = T::cQuote;
			for ( ; iLen; --iLen )
			{
				char s = *sText++;
				if ( T::IsEscapeChar ( s ) )
				{
					*pCur++ = '\\';
					*pCur++ = T::GetEscapedChar ( s );
				} else
					*pCur++ = T::FixupSpaceWithEscaping ( s );
			}
			*pCur++ = T::cQuote;
		}
		*pCur = '\0';
		m_iUsed += iFinalLen;
	}

	EscapedStringBuilder_T &SkipNextComma ()
	{
		StringBuilder_c::SkipNextComma ();
		return *this;
	}

	EscapedStringBuilder_T &AppendName ( const char * sName )
	{
		StringBuilder_c::AppendName(sName);
		return *this;
	}
};

class ScopedComma_c : public ISphNoncopyable
{
public:
	ScopedComma_c() = default;

	ScopedComma_c ( StringBuilder_c & tOwner, const char * sDel, const char * sPref = nullptr, const char * sTerm = nullptr, bool bAllowEmpty=true )
		: m_pOwner ( &tOwner )
		, m_bAllowEmpty ( bAllowEmpty )
	{
		m_iLevel = tOwner.StartBlock ( sDel, sPref, sTerm );
	}

	ScopedComma_c ( StringBuilder_c & tOwner, const StrBlock_t & dBlock )
		: m_pOwner ( &tOwner )
	{
		m_iLevel = tOwner.StartBlock(dBlock);
	}

	ScopedComma_c ( ScopedComma_c && rhs ) noexcept
	{
		Swap (rhs);
	}

	ScopedComma_c & operator= ( ScopedComma_c && rhs ) noexcept
	{
		Swap (rhs);
		return *this;
	}

	~ScopedComma_c()
	{
		if ( m_pOwner )
			m_pOwner->FinishBlocks ( m_iLevel, m_bAllowEmpty );
	}

	void Swap ( ScopedComma_c & rhs ) noexcept
	{
		::Swap ( m_pOwner, rhs.m_pOwner );
		::Swap ( m_iLevel, rhs.m_iLevel );
	}

	void Init ( StringBuilder_c & tOwner, const char * sDel, const char * sPref = nullptr, const char * sTerm = nullptr )
	{
		assert ( !m_pOwner );
		if ( m_pOwner )
			return;
		m_pOwner = &tOwner;
		m_iLevel = tOwner.StartBlock ( sDel, sPref, sTerm );
	}

	StringBuilder_c & Sink() const
	{
		assert ( m_pOwner );
		return *m_pOwner;
	}

private:
	StringBuilder_c *	m_pOwner = nullptr;
	int					m_iLevel = 0;
	bool				m_bAllowEmpty = true;
};

//////////////////////////////////////////////////////////////////////////

/// name+int pair
using CSphNamedInt = std::pair<CSphString,int>;

inline StringBuilder_c& operator<< ( StringBuilder_c& tOut, const CSphNamedInt& tValue )
{
	tOut.Sprintf ( "%s=%d", tValue.first.cstr(), tValue.second );
	return tOut;
}


/////////////////////////////////////////////////////////////////////////////

/// string hash function
struct CSphStrHashFunc
{
	static int Hash ( const CSphString & sKey );
};

/// small hash with string keys
template < typename T, int LENGTH = 256 >
using SmallStringHash_T = CSphOrderedHash < T, CSphString, CSphStrHashFunc, LENGTH >;


namespace sph {

// used to simple add/delete strings and check if a string was added by [] op
class StringSet : private SmallStringHash_T<bool>
{
	using BASE = SmallStringHash_T<bool>;
public:
	inline void Add ( const CSphString& sKey )
	{
		BASE::Add ( true, sKey );
	}

	inline void Delete ( const CSphString& sKey )
	{
		BASE::Delete ( sKey );
	}

	inline bool operator[] ( const CSphString& sKey ) const
	{
		if ( BASE::Exists ( sKey ) )
			return BASE::operator[] ( sKey );
		return false;
	}
	using BASE::Reset;
	using BASE::GetLength;

	using BASE::begin;
	using BASE::end;
	using Iterator_c = BASE::Iterator_c;
};
}

//////////////////////////////////////////////////////////////////////////

/// pointer with automatic safe deletion when going out of scope
template < typename T >
class CSphScopedPtr : public ISphNoncopyable
{
public:
	explicit		CSphScopedPtr ( T * pPtr )	{ m_pPtr = pPtr; }
					~CSphScopedPtr ()			{ SafeDelete ( m_pPtr ); }
	T *				operator -> () const		{ return m_pPtr; }
	T *				Ptr () const				{ return m_pPtr; }
	explicit operator bool () const				{ return m_pPtr!=nullptr; }

	CSphScopedPtr& operator= ( T* pPtr )
	{
		CSphScopedPtr<T> pTmp ( pPtr );
		Swap ( pTmp );
		return *this;
	}

	CSphScopedPtr& operator= ( CSphScopedPtr pPtr )
	{
		Swap ( pPtr );
		return *this;
	}
	T *				LeakPtr ()					{ T * pPtr = m_pPtr; m_pPtr = NULL; return pPtr; }
	void			Reset ()					{ SafeDelete ( m_pPtr ); }
	inline void 	Swap (CSphScopedPtr & rhs) noexcept { ::Swap(m_pPtr,rhs.m_pPtr);}


protected:
	T *				m_pPtr;
};

//////////////////////////////////////////////////////////////////////////

/// automatic pointer wrapper for refcounted objects
/// construction from or assignment of a raw pointer takes over (!) the ownership
template < typename T >
class CSphRefcountedPtr
{
public:
	explicit		CSphRefcountedPtr () = default;		///< default NULL wrapper construction (for vectors)
	explicit		CSphRefcountedPtr ( T * pPtr ) : m_pPtr ( pPtr ) {}	///< construction from raw pointer, takes over ownership!

	CSphRefcountedPtr ( const CSphRefcountedPtr& rhs )
		: m_pPtr ( rhs.m_pPtr )
	{
		SafeAddRef ( m_pPtr );
	}

	CSphRefcountedPtr ( CSphRefcountedPtr&& rhs ) noexcept
	{
		Swap(rhs);
	}

	CSphRefcountedPtr& operator= ( CSphRefcountedPtr rhs )
	{
		Swap(rhs);
		return *this;
	}

	void Swap ( CSphRefcountedPtr& rhs ) noexcept
	{
		::Swap(m_pPtr, rhs.m_pPtr);
	}

	~CSphRefcountedPtr ()				{ SafeRelease ( m_pPtr ); }

	T *	operator -> () const			{ return m_pPtr; }
		explicit operator bool() const	{ return m_pPtr!=nullptr; }
		operator T * () const			{ return m_pPtr; }

	// drop the ownership and reset pointer
	inline T * Leak ()
	{
		T * pRes = m_pPtr;
		m_pPtr = nullptr;
		return pRes;
	}

	T * Ptr() const { return m_pPtr; }

public:
	/// assignment of a raw pointer, takes over ownership!
	CSphRefcountedPtr<T> & operator = ( T * pPtr )
	{
		SafeRelease ( m_pPtr );
		m_pPtr = pPtr;
		return *this;
	}

protected:
	T *				m_pPtr = nullptr;
};

//////////////////////////////////////////////////////////////////////////

void sphWarn ( const char *, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) );
void SafeClose ( int & iFD );

//////////////////////////////////////////////////////////////////////////
/// system-agnostic wrappers for mmap
namespace sph {
#if SPH_ALLOCS_PROFILER
inline void MemStatMMapAdd ( int64_t iSize ) { sphMemStatMMapAdd(iSize); }
inline void MemStatMMapDel ( int64_t iSize ) { sphMemStatMMapDel(iSize); }
#else
inline void MemStatMMapAdd ( int64_t ) {}
inline void MemStatMMapDel ( int64_t ) {}
#endif
};

enum class Mode_e
{
	NONE,
	READ,
	WRITE,
	RW,
};

enum class Share_e
{
	ANON_PRIVATE,
	ANON_SHARED,
	SHARED,
};

enum class Advise_e
{
	NOFORK,
	NODUMP,
};

void * mmalloc ( size_t uSize, Mode_e = Mode_e::RW, Share_e = Share_e::ANON_PRIVATE );
bool mmapvalid ( const void* pMem );
int mmfree ( void* pMem, size_t uSize );
void mmadvise ( void* pMem, size_t uSize, Advise_e = Advise_e::NODUMP );
bool mmlock( void * pMem, size_t uSize );
bool mmunlock( void * pMem, size_t uSize );

//////////////////////////////////////////////////////////////////////////

/// buffer trait that neither own buffer nor clean-up it on destroy
template < typename T >
class CSphBufferTrait : public ISphNoncopyable, public VecTraits_T<T>
{
protected:
	using VecTraits_T<T>::m_pData;
	using VecTraits_T<T>::m_iCount;
public:
	using VecTraits_T<T>::GetLengthBytes;
	/// ctor
	CSphBufferTrait () = default;

	/// dtor
	virtual ~CSphBufferTrait ()
	{
		assert ( !m_bMemLocked && !m_pData );
	}

	virtual void Reset () = 0;


	/// get write address
	T * GetWritePtr () const
	{
		return m_pData;
	}

	void Set ( T * pData, int64_t iCount )
	{
		m_pData = pData;
		m_iCount = iCount;
	}

	bool MemLock ( CSphString & sWarning )
	{
		m_bMemLocked = mmlock ( m_pData, GetLengthBytes() );
		if ( !m_bMemLocked )
			sWarning.SetSprintf ( "mlock() failed: %s", strerrorm(errno) );

		return m_bMemLocked;
	}

protected:

	bool		m_bMemLocked = false;

	void MemUnlock ()
	{
		if ( !m_bMemLocked )
			return;

		m_bMemLocked = false;
		bool bOk = mmunlock ( m_pData, GetLengthBytes() );
		if ( !bOk )
			sphWarn ( "munlock() failed: %s", strerrorm(errno) );
	}
};


//////////////////////////////////////////////////////////////////////////

/// in-memory buffer shared between processes
template < typename T, bool SHARED=false >
class CSphLargeBuffer : public CSphBufferTrait < T >
{
public:
	/// ctor
	CSphLargeBuffer () {}

	/// dtor
	virtual ~CSphLargeBuffer ()
	{
		this->Reset();
	}

public:
	/// allocate storage
	bool Alloc ( int64_t iEntries, CSphString & sError )
	{
		assert ( !this->GetWritePtr() );

		int64_t uCheck = sizeof(T);
		uCheck *= iEntries;

		int64_t iLength = (size_t)uCheck;
		if ( uCheck!=iLength )
		{
			sError.SetSprintf ( "impossible to mmap() over 4 GB on 32-bit system" );
			return false;
		}

		auto * pData = (T *) mmalloc ( iLength, Mode_e::RW, SHARED ? Share_e::ANON_SHARED : Share_e::ANON_PRIVATE );
		if ( !mmapvalid ( pData ) )
		{
			if ( iLength>(int64_t)0x7fffffffUL )
				sError.SetSprintf ( "mmap() failed: %s (length=" INT64_FMT " is over 2GB, impossible on some 32-bit systems)",
					strerrorm(errno), iLength );
			else
				sError.SetSprintf ( "mmap() failed: %s (length=" INT64_FMT ")", strerrorm(errno), iLength );
			return false;
		}
		mmadvise ( pData, iLength, Advise_e::NODUMP );
		if ( !SHARED )
			mmadvise ( pData, iLength, Advise_e::NOFORK );

		sph::MemStatMMapAdd ( iLength );

		assert ( pData );
		this->Set ( pData, iEntries );
		return true;
	}


	/// deallocate storage
	virtual void Reset ()
	{
		this->MemUnlock();

		if ( !this->GetWritePtr() )
			return;

		int iRes = mmfree ( this->GetWritePtr(), this->GetLengthBytes() );
		if ( iRes )
			sphWarn ( "munmap() failed: %s", strerrorm(errno) );

		sph::MemStatMMapDel ( this->GetLengthBytes() );
		this->Set ( NULL, 0 );
	}
};

//////////////////////////////////////////////////////////////////////////

extern int g_iMaxCoroStackSize;

/// my thread handle and thread func magic
#if USE_WINDOWS
typedef HANDLE SphThread_t;
typedef DWORD SphThreadKey_t;
#else
typedef pthread_t SphThread_t;
typedef pthread_key_t SphThreadKey_t;
#endif

/// init of memory statistic's data
void sphMemStatInit ();

/// cleanup of memory statistic's data
void sphMemStatDone ();

//bool sphThreadCreate ( SphThread_t * pThread, void (*fnThread)(void*), void * pArg, bool bDetached=false, const char * sName=nullptr );
// function was removed. Use Threads::Create instead

/// get the pointer to my job's stack (m.b. different from thread stack in coro)
const void * sphMyStack ();

/// get size of the stack (either thread, either coro - depends from context)
int sphMyStackSize();

/// get size of used stack (threads or coro - depends from context)
int64_t sphGetStackUsed();

/// a singleton. Since C++11 it is thread-safe, and so, looks really simple
template<typename T, typename T_tag = T>
T & Single_T ()
{
	static T t;
	return t;
}

template<typename T, typename T_tag = T>
const T & SingleC_T ()
{
	return Single_T<T, T_tag> ();
}

#if !USE_WINDOWS
/// what kind of threading lib do we have? The number of frames in the stack depends from it
bool sphIsLtLib();
#endif

/// capability for tracing threads
using ThreadRole CAPABILITY ( "role" ) = bool;

inline void AcquireRole ( ThreadRole R ) ACQUIRE(R) NO_THREAD_SAFETY_ANALYSIS
{}

inline void ReleaseRole ( ThreadRole R ) RELEASE(R) NO_THREAD_SAFETY_ANALYSIS
{}

class SCOPED_CAPABILITY ScopedRole_c
{
	ThreadRole &m_tRoleRef;
public:
	/// acquire on creation
	inline explicit ScopedRole_c ( ThreadRole &tRole ) ACQUIRE( tRole )
		: m_tRoleRef ( tRole )
	{
		AcquireRole ( tRole );
	}

	/// release on going out of scope
	~ScopedRole_c () RELEASE()
	{
		ReleaseRole ( m_tRoleRef );
	}
};

#if USE_WINDOWS
	using TMutex = HANDLE;
#else
	using TMutex = pthread_mutex_t;
#endif

/// mutex implementation
class CAPABILITY ( "mutex" ) CSphMutex : public ISphNoncopyable
{

public:
	CSphMutex ();
	~CSphMutex ();

	bool Lock () ACQUIRE();
	bool Unlock () RELEASE();
	bool TimedLock ( int iMsec ) TRY_ACQUIRE (true);

	// Just for clang negative capabilities.
	const CSphMutex &operator! () const { return *this; }

	TMutex & mutex () RETURN_CAPABILITY ( this )
	{
		return m_tMutex;
	}

protected:
	TMutex m_tMutex;
};

// event implementation
class EventWrapper_c : public ISphNoncopyable
{
public:
	EventWrapper_c ();
	~EventWrapper_c();

	inline bool Initialized() const
	{
		return m_bInitialized;
	}

protected:
	bool m_bInitialized = false;

#if USE_WINDOWS
	HANDLE m_hEvent = 0;
#else
	pthread_cond_t m_tCond;
	pthread_mutex_t m_tMutex;
#endif
};

template <bool bONESHOT=true>
class AutoEvent_T: public EventWrapper_c
{
public:
	// increase of set (oneshot) event's count and issue an event.
	void SetEvent ();

	// decrease or reset (oneshot) event's count. If count empty, go to sleep until new events
	// returns true if event happened, false if timeout reached or event is not initialized
	bool WaitEvent ( int iMsec = -1); // -1 means 'infinite'

private:
	volatile int m_iSent = 0;
};

using CSphAutoEvent = AutoEvent_T<false>;
using OneshotEvent_c = AutoEvent_T<>;

/// scoped mutex lock
///  may adopt, lock and unlock explicitly
template<typename Mutex>
class CAPABILITY("mutex") SCOPED_CAPABILITY CSphScopedLock : public ISphNoncopyable
{
public:

	// Tag type used to distinguish constructors.
	enum ADOPT_LOCK_E { adopt_lock };

	/// adopt already held lock
	CSphScopedLock ( Mutex & tMutex, ADOPT_LOCK_E ) REQUIRES ( tMutex) ACQUIRE ( tMutex )
			: m_tMutexRef ( tMutex )
			, m_bLocked (true )
	{
	}

	/// constructor acquires the lock
	explicit CSphScopedLock ( Mutex & tMutex ) ACQUIRE ( tMutex )
			: m_tMutexRef ( tMutex ), m_bLocked ( true )
	{
		m_tMutexRef.Lock();
		m_bLocked = true;
	}

	/// unlock on going out of scope
	~CSphScopedLock () RELEASE()
	{
		if ( m_bLocked )
			m_tMutexRef.Unlock ();
	}

	/// Explicitly acquire the lock.
	/// to be used ONLY from the same thread! (call from another is obviously wrong)
	void Lock () ACQUIRE ()
	{
		if ( !m_bLocked )
		{
			m_tMutexRef.Lock ();
			m_bLocked = true;
		}
	}

	/// Explicitly release the lock.
	void Unlock () RELEASE ()
	{
		if ( m_bLocked )
		{
			m_tMutexRef.Unlock ();
			m_bLocked = false;
		}
	}

	bool Locked () const
	{
		return m_bLocked;
	}

	TMutex & mutex () RETURN_CAPABILITY ( m_tMutexRef )
	{
		return m_tMutexRef.mutex();
	}

private:
	Mutex & m_tMutexRef;
	bool m_bLocked; // whether the mutex is currently locked or unlocked
};

using ScopedMutex_t = CSphScopedLock<CSphMutex>;

/// rwlock implementation
class CAPABILITY ( "mutex" ) CSphRwlock : public ISphNoncopyable
{
public:
	CSphRwlock ();
	~CSphRwlock () {
#if !USE_WINDOWS
		SafeDelete ( m_pLock );
		SafeDelete ( m_pWritePreferHelper );
#endif
	}

	bool Init ( bool bPreferWriter=false );
	bool Done ();

	bool ReadLock () ACQUIRE_SHARED();
	bool WriteLock () ACQUIRE();
	bool Unlock () UNLOCK_FUNCTION();

	// Just for clang negative capabilities.
	const CSphRwlock &operator! () const { return *this; }

private:
	bool				m_bInitialized = false;
#if USE_WINDOWS
	HANDLE				m_hWriteMutex = 0;
	HANDLE				m_hReadEvent = 0;
	LONG				m_iReaders = 0;
#else
	pthread_rwlock_t	* m_pLock;
	CSphMutex			* m_pWritePreferHelper = nullptr;
#endif
};

// rwlock with auto init/done
class RwLock_t : public CSphRwlock
{
public:
	RwLock_t()
	{
		Verify ( Init());
	}
	~RwLock_t()
	{
		Verify ( Done());
	}

	explicit RwLock_t ( bool bPreferWriter )
	{
		Verify ( Init ( bPreferWriter ) );
	}
};


/// scoped shared (read) lock
template<class LOCKED=CSphRwlock>
class SCOPED_CAPABILITY CSphScopedRLock_T : ISphNoncopyable
{
public:
	/// lock on creation
	explicit CSphScopedRLock_T ( LOCKED & tLock ) ACQUIRE_SHARED ( tLock )
		: m_tLock ( tLock )
	{
		m_tLock.ReadLock();
	}

	/// unlock on going out of scope
	~CSphScopedRLock_T () RELEASE ()
	{
		m_tLock.Unlock();
	}

protected:
	LOCKED & m_tLock;
};

/// scoped exclusive (write) lock
template<class LOCKED=CSphRwlock>
class SCOPED_CAPABILITY CSphScopedWLock_T : ISphNoncopyable
{
public:
	/// lock on creation
	explicit CSphScopedWLock_T ( LOCKED & tLock ) ACQUIRE ( tLock ) EXCLUDES ( tLock )
		: m_tLock ( tLock )
	{
		m_tLock.WriteLock();
	}

	/// unlock on going out of scope
	~CSphScopedWLock_T () RELEASE ()
	{
		m_tLock.Unlock();
	}

protected:
	LOCKED & m_tLock;
};

/// scoped lock owner - unlock in dtr
template <class LOCKED=CSphRwlock>
class SCOPED_CAPABILITY ScopedUnlock_T : ISphNoncopyable
{
public:
	/// lock on creation
	explicit ScopedUnlock_T ( LOCKED &tLock ) ACQUIRE ( tLock )
		: m_pLock ( &tLock )
	{}

	ScopedUnlock_T ( ScopedUnlock_T && tLock ) noexcept
		: m_pLock ( tLock.m_pLock )
	{
		tLock.m_pLock = nullptr;
	}

	ScopedUnlock_T &operator= ( ScopedUnlock_T &&rhs ) noexcept
		RELEASE()
	{
		if ( this==&rhs )
			return *this;
		if ( m_pLock )
			m_pLock->Unlock();
		m_pLock = rhs.m_pLock;
		rhs.m_pLock = nullptr;
		return *this;
	}

	/// unlock on going out of scope
	~ScopedUnlock_T () RELEASE ()
	{
		if ( m_pLock )
			m_pLock->Unlock ();
	}

protected:
	LOCKED * m_pLock;
};

using CSphScopedRLock = CSphScopedRLock_T<>;
using CSphScopedWLock = CSphScopedWLock_T<>;
// shortcuts (original names sometimes looks too long)
using ScRL_t = CSphScopedRLock;
using ScWL_t = CSphScopedWLock;

// perform any (function-defined) action on exit from a scope.
template < typename ACTION >
class AtScopeExit_T
{
	ACTION m_dAction;
public:
	explicit AtScopeExit_T ( ACTION &&tAction )
		: m_dAction { std::forward<ACTION> ( tAction ) }
	{}

	AtScopeExit_T ( AtScopeExit_T &&rhs ) noexcept
		: m_dAction { std::move ( rhs.m_dAction ) }
	{}

	~AtScopeExit_T ()
	{
		m_dAction ();
	}
};

// create action to be performed on-exit-from-scope.
// usage example:
// someObject * pObj; // need to be freed going out of scope
// auto dObjDeleter = AtScopeExit ( [&pObj] { SafeDelete (pObj); } )
// ...
template < typename ACTION >
AtScopeExit_T<ACTION> AtScopeExit ( ACTION &&action )
{
	return AtScopeExit_T<ACTION>{ std::forward<ACTION> ( action ) };
}


//////////////////////////////////////////////////////////////////////////

/// generic dynamic bitvector
/// with a preallocated part for small-size cases, and a dynamic route for big-size ones
class CSphBitvec
{
protected:
	DWORD *		m_pData = nullptr;
	DWORD		m_uStatic[4] {0};
	int			m_iElements = 0;

public:
	CSphBitvec () = default;

	explicit CSphBitvec ( int iElements )
	{
		Init ( iElements );
	}

	~CSphBitvec ()
	{
		if ( m_pData!=m_uStatic )
			SafeDeleteArray ( m_pData );
	}

	/// copy ctor
	CSphBitvec ( const CSphBitvec & rhs )
	{
		m_pData = nullptr;
		m_iElements = 0;
		*this = rhs;
	}

	/// copy
	CSphBitvec & operator = ( const CSphBitvec & rhs )
	{
		if ( m_pData!=m_uStatic )
			SafeDeleteArray ( m_pData );

		Init ( rhs.m_iElements );
		memcpy ( m_pData, rhs.m_pData, sizeof(m_uStatic[0]) * GetSize() );

		return *this;
	}

	void Init ( int iElements )
	{
		assert ( iElements>=0 );
		m_iElements = iElements;
		if ( iElements > int(sizeof(m_uStatic)*8) )
		{
			int iSize = GetSize();
			m_pData = new DWORD [ iSize ];
		} else
		{
			m_pData = m_uStatic;
		}
		Clear();
	}

	void Clear ()
	{
		int iSize = GetSize();
		memset ( m_pData, 0, sizeof(DWORD)*iSize );
	}

	void Set ()
	{
		int iSize = GetSize();
		memset ( m_pData, 0xff, sizeof(DWORD)*iSize );
	}


	bool BitGet ( int iIndex ) const
	{
		assert ( m_pData );
		assert ( iIndex>=0 );
		assert ( iIndex<m_iElements );
		return ( m_pData [ iIndex>>5 ] & ( 1UL<<( iIndex&31 ) ) )!=0; // NOLINT
	}

	void BitSet ( int iIndex )
	{
		assert ( iIndex>=0 );
		assert ( iIndex<m_iElements );
		m_pData [ iIndex>>5 ] |= ( 1UL<<( iIndex&31 ) ); // NOLINT
	}

	void BitClear ( int iIndex )
	{
		assert ( iIndex>=0 );
		assert ( iIndex<m_iElements );
		m_pData [ iIndex>>5 ] &= ~( 1UL<<( iIndex&31 ) ); // NOLINT
	}

	const DWORD * Begin () const
	{
		return m_pData;
	}

	DWORD * Begin ()
	{
		return m_pData;
	}

	int GetSize() const
	{
		return (m_iElements+31)/32;
	}

	bool IsEmpty() const
	{
		if (!m_pData)
			return true;

		return GetSize ()==0;
	}

	int GetBits() const
	{
		return m_iElements;
	}

	int BitCount () const
	{
		int iBitSet = 0;
		for ( int i=0; i<GetSize(); i++ )
			iBitSet += sphBitCount ( m_pData[i] );

		return iBitSet;
	}
};

//////////////////////////////////////////////////////////////////////////

#if USE_WINDOWS
#define DISABLE_CONST_COND_CHECK \
	__pragma ( warning ( push ) ) \
	__pragma ( warning ( disable:4127 ) )
#define ENABLE_CONST_COND_CHECK \
	__pragma ( warning ( pop ) )
#else
#define DISABLE_CONST_COND_CHECK
#define ENABLE_CONST_COND_CHECK
#endif

#define if_const(_arg) \
	DISABLE_CONST_COND_CHECK \
	if ( _arg ) \
	ENABLE_CONST_COND_CHECK

/// MT-aware refcounted base (uses atomics that sometimes m.b. slow because of inter-cpu sync)
struct ISphRefcountedMT : public ISphNoncopyable
{
protected:
	virtual ~ISphRefcountedMT ()
	{}

public:
	inline void AddRef () const
	{
		m_iRefCount.fetch_add ( 1, std::memory_order_acquire );
	}

	inline void Release () const
	{
		if ( m_iRefCount.fetch_sub ( 1, std::memory_order_release )==1 )
		{
			assert ( m_iRefCount.load ( std::memory_order_acquire )==0 );
			delete this;
		}
	}

	inline long GetRefcount() const
	{
		return m_iRefCount.load ( std::memory_order_acquire );
	}

	inline bool IsLast() const
	{
		return 1==m_iRefCount.load ( std::memory_order_acquire );
	}

private:
	mutable std::atomic<long> m_iRefCount { 1 };
};

using RefCountedRefPtr_t = CSphRefcountedPtr<ISphRefcountedMT>;

template <class T>
struct VecRefPtrs_t : public ISphNoncopyable, public CSphVector<T>
{
	using CSphVector<T>::SwapData;

	VecRefPtrs_t () = default;
	VecRefPtrs_t ( VecRefPtrs_t<T>&& rhs ) noexcept
	{
		SwapData (rhs);
	}

	VecRefPtrs_t& operator = ( VecRefPtrs_t<T>&& rhs ) noexcept
	{
		SwapData ( rhs );
		return *this;
	}

	~VecRefPtrs_t ()
	{
		CSphVector<T>::Apply ( [] ( T &ptr ) { SafeRelease ( ptr ); } );
	}
};

enum class ETYPE { SINGLE, ARRAY };
template<typename PTR, ETYPE tp>
struct Deleter_T {
	inline static void Delete ( void * pArg ) { if (pArg) delete (PTR) pArg; }
};

template<typename PTR>
struct Deleter_T<PTR,ETYPE::ARRAY>
{
	inline static void Delete ( void * pArg ) { if (pArg) delete [] (PTR) pArg; }
};

// stateless (i.e. may use pointer to fn)
template<typename PTR, typename DELETER>
struct StaticDeleter_t
{
	inline static void Delete ( void * pArg ) { if ( pArg ) DELETER () ( PTR (pArg )); }
};

// statefull (i.e. contains state, implies using of lambda with captures)
template<typename PTR, typename DELETER>
class CustomDeleter_T
{
	DELETER m_dDeleter;
public:

	CustomDeleter_T () = default;

	CustomDeleter_T ( DELETER&& dDeleter )
		: m_dDeleter { std::forward<DELETER> ( dDeleter ) }
	{}

	inline void Delete ( void * pArg ) {
		if ( m_dDeleter )
			m_dDeleter ( (PTR) pArg );
	}
};
/// shared pointer for any object, managed by refcount
template < typename PTR, typename DELETER, typename REFCOUNTED = ISphRefcountedMT >
class SharedPtr_T
{
	template <typename RefCountedT>
	struct SharedState_T : public RefCountedT
	{
		PTR m_pPtr = nullptr;
		DELETER m_fnDelete;

		SharedState_T() = default;

		template<typename DEL>
		explicit SharedState_T ( DEL&& fnDelete )
			: m_fnDelete ( std::forward<DEL>(fnDelete) )
		{}

		~SharedState_T()
		{
			m_fnDelete.Delete(m_pPtr);
			m_pPtr = nullptr;
		}
	};

	using SharedState_t = SharedState_T<REFCOUNTED>;
	using StatePtr = CSphRefcountedPtr<SharedState_t>;

	StatePtr m_tState;

public:
	///< default ctr (for vectors)
	explicit SharedPtr_T () = default;

	/// construction from raw pointer, creates new shared state!
	explicit SharedPtr_T ( PTR pPtr ) : m_tState ( new SharedState_t() )
	{
		m_tState->m_pPtr = pPtr;
	}

	template <typename DEL>
	SharedPtr_T ( PTR pPtr, DEL&& fn )
		: m_tState ( new SharedState_t (std::forward<DEL>(fn)) )
	{
		m_tState->m_pPtr = pPtr;
	}

	SharedPtr_T ( const SharedPtr_T& rhs )
		: m_tState ( rhs.m_tState )
	{}

	SharedPtr_T ( SharedPtr_T&& rhs ) noexcept
	{
		Swap(rhs);
	}

	SharedPtr_T& operator= ( SharedPtr_T rhs )
	{
		Swap(rhs);
		return *this;
	}

	void Swap ( SharedPtr_T& rhs ) noexcept
	{
		::Swap( m_tState, rhs.m_tState);
	}

	PTR	operator -> () const			{ return m_tState->m_pPtr; }
		explicit operator bool() const	{ return m_tState && m_tState->m_pPtr!=nullptr; }
		operator PTR () const			{ return m_tState?m_tState->m_pPtr:nullptr; }

public:
	/// assignment of a raw pointer
	SharedPtr_T & operator = ( PTR pPtr )
	{
		m_tState = new SharedState_t;
		m_tState->m_pPtr = pPtr;
		return *this;
	}
};

template <typename T, typename REFCOUNTED = ISphRefcountedMT>
using SharedPtr_t = SharedPtr_T<T, Deleter_T<T, ETYPE::SINGLE>, REFCOUNTED>;

template<typename T, typename REFCOUNTED = ISphRefcountedMT>
using SharedPtrArr_t = SharedPtr_T<T, Deleter_T<T, ETYPE::ARRAY>, REFCOUNTED>;

template<typename T, typename DELETER=std::function<void(T)>, typename REFCOUNTED = ISphRefcountedMT>
using SharedPtrCustom_t = SharedPtr_T<T, CustomDeleter_T<T, DELETER>, REFCOUNTED>;

int sphCpuThreadsCount ();

//////////////////////////////////////////////////////////////////////////
struct HashFunc_Int64_t
{
	static DWORD GetHash ( int64_t k )
	{
		return ( DWORD(k) * 0x607cbb77UL ) ^ ( k>>32 );
	}
};


/// simple open-addressing hash
template < typename VALUE, typename KEY, typename HASHFUNC=HashFunc_Int64_t >
class OpenHash_T
{
public:
	using MYTYPE = OpenHash_T<VALUE,KEY,HASHFUNC>;

	/// initialize hash of a given initial size
	explicit OpenHash_T ( int64_t iSize=256 )
	{
		Reset ( iSize );
	}

	~OpenHash_T()
	{
		SafeDeleteArray ( m_pHash );
	}

	/// reset to a given size
	void Reset ( int64_t iSize )
	{
		assert ( iSize<=UINT_MAX ); 		// sanity check
		SafeDeleteArray ( m_pHash );
		if ( iSize<=0 )
		{
			m_iSize = m_iUsed = m_iMaxUsed = 0;
			return;
		}

		iSize = ( 1ULL<<sphLog2 ( iSize-1 ) );
		assert ( iSize<=UINT_MAX ); 		// sanity check
		m_pHash = new Entry_t[iSize];
		m_iSize = iSize;
		m_iUsed = 0;
		m_iMaxUsed = GetMaxLoad ( iSize );
	}

	void Clear()
	{
		for ( int i=0; i<m_iSize; i++ )
			m_pHash[i] = Entry_t();

		m_iUsed = 0;
	}

	/// acquire value by key (ie. get existing hashed value, or add a new default value)
	VALUE & Acquire ( KEY k )
	{
		DWORD uHash = HASHFUNC::GetHash(k);
		int64_t iIndex = uHash & ( m_iSize-1 );

		int64_t iDead = -1;
		while (true)
		{
			// found matching key? great, return the value
			Entry_t * p = m_pHash + iIndex;
			if ( p->m_uState==Entry_e::USED && p->m_Key==k )
				return p->m_Value;

			// no matching keys? add it
			if ( p->m_uState==Entry_e::EMPTY )
			{
				// not enough space? grow the hash and force rescan
				if ( m_iUsed>=m_iMaxUsed )
				{
					Grow();
					iIndex = uHash & ( m_iSize-1 );
					iDead = -1;
					continue;
				}

				// did we walk past a dead entry while probing? if so, lets reuse it
				if ( iDead>=0 )
					p = m_pHash + iDead;

				// store the newly added key
				p->m_Key = k;
				p->m_uState = Entry_e::USED;
				m_iUsed++;
				return p->m_Value;
			}

			// is this a dead entry? store its index for (possible) reuse
			if ( p->m_uState==Entry_e::DELETED )
				iDead = iIndex;

			// no match so far, keep probing
			iIndex = ( iIndex+1 ) & ( m_iSize-1 );
		}
	}

	/// find an existing value by key
	VALUE * Find ( KEY k ) const
	{
		Entry_t * e = FindEntry(k);
		return e ? &e->m_Value : nullptr;
	}

	/// add or fail (if key already exists)
	bool Add ( KEY k, const VALUE & v )
	{
		int64_t u = m_iUsed;
		VALUE & x = Acquire(k);
		if ( u==m_iUsed )
			return false; // found an existing value by k, can not add v
		x = v;

		return true;
	}

	/// find existing value, or add a new value
	VALUE & FindOrAdd ( KEY k, const VALUE & v )
	{
		int64_t u = m_iUsed;
		VALUE & x = Acquire(k);
		if ( u!=m_iUsed )
			x = v; // did not find an existing value by k, so add v

		return x;
	}

	/// delete by key
	bool Delete ( KEY k )
	{
		Entry_t * e = FindEntry(k);
		if ( e )
			e->m_uState = Entry_e::DELETED;

		return e!=nullptr;
	}

	/// get number of inserted key-value pairs
	int64_t GetLength() const
	{
		return m_iUsed;
	}

	int64_t GetLengthBytes () const
	{
		return m_iSize * sizeof ( Entry_t );
	}

	/// iterate the hash by entry index, starting from 0
	/// finds the next alive key-value pair starting from the given index
	/// returns that pair and updates the index on success
	/// returns NULL when the hash is over
	VALUE * Iterate ( int64_t * pIndex, KEY * pKey ) const
	{
		if ( !pIndex || *pIndex<0 )
			return nullptr;

		for ( int64_t i = *pIndex; i < m_iSize; ++i )
			if ( m_pHash[i].m_uState==Entry_e::USED )
			{
				*pIndex = i+1;
				if ( pKey )
					*pKey = m_pHash[i].m_Key;

				return &m_pHash[i].m_Value;
			}

		return nullptr;
	}

	// same as above, but without messing of return value/return param
	std::pair<KEY,VALUE*> Iterate ( int64_t * pIndex ) const
	{
		if ( !pIndex || *pIndex<0 )
			return {0, nullptr};

		for ( int64_t i = *pIndex; i<m_iSize; ++i )
			if ( m_pHash[i].m_uState==Entry_e::USED ) {
				*pIndex = i+1;
				return {m_pHash[i].m_Key, &m_pHash[i].m_Value};
			}

		return {0,nullptr};
	}

	void Swap ( MYTYPE& rhs ) noexcept
	{
		::Swap ( m_iSize, rhs.m_iSize );
		::Swap ( m_iUsed, rhs.m_iUsed );
		::Swap ( m_iMaxUsed, rhs.m_iMaxUsed );
		::Swap ( m_pHash, rhs.m_pHash );
	}

protected:
	enum class Entry_e
	{
		EMPTY,
		USED,
		DELETED
	};

#pragma pack(push,4)
	struct Entry_t
	{
		KEY		m_Key;
		VALUE	m_Value;
		Entry_e	m_uState { Entry_e::EMPTY };
		Entry_t ();
	};
#pragma pack(pop)

	int64_t		m_iSize {0};					// total hash size
	int64_t		m_iUsed {0};					// how many entries are actually used
	int64_t		m_iMaxUsed {0};					// resize threshold

	Entry_t *	m_pHash {nullptr};	///< hash entries

									/// get max load, ie. max number of actually used entries at given size
	int64_t GetMaxLoad ( int64_t iSize ) const
	{
		return (int64_t)( iSize*LOAD_FACTOR );
	}

	/// we are overloaded, lets grow 2x and rehash
	void Grow()
	{
		int64_t iNewSize = 2*Max(m_iSize,8);
		assert ( iNewSize<=UINT_MAX ); 		// sanity check

		Entry_t * pNew = new Entry_t[iNewSize];

		for ( int64_t i=0; i<m_iSize; i++ )
			if ( m_pHash[i].m_uState==Entry_e::USED )
			{
				int64_t j = HASHFUNC::GetHash ( m_pHash[i].m_Key ) & ( iNewSize-1 );
				while ( pNew[j].m_uState==Entry_e::USED )
					j = ( j+1 ) & ( iNewSize-1 );

				pNew[j] = m_pHash[i];
			}

		SafeDeleteArray ( m_pHash );
		m_pHash = pNew;
		m_iSize = iNewSize;
		m_iMaxUsed = GetMaxLoad ( m_iSize );
	}

	/// find (and do not touch!) entry by key
	inline Entry_t * FindEntry ( KEY k ) const
	{
		int64_t iIndex = HASHFUNC::GetHash(k) & ( m_iSize-1 );

		while ( m_pHash[iIndex].m_uState!=Entry_e::EMPTY )
		{
			Entry_t & tEntry = m_pHash[iIndex];
			if ( tEntry.m_Key==k && tEntry.m_uState!=Entry_e::DELETED )
				return &tEntry;

			iIndex = ( iIndex+1 ) & ( m_iSize-1 );
		}

		return nullptr;
	}

private:
	static constexpr float LOAD_FACTOR = 0.95f;
};

template<typename V, typename K, typename H> OpenHash_T<V,K,H>::Entry_t::Entry_t() = default;
template<> inline OpenHash_T<int,int64_t>::Entry_t::Entry_t() : m_Key{0}, m_Value{0} {}
template<> inline OpenHash_T<DWORD,int64_t>::Entry_t::Entry_t() : m_Key{0}, m_Value{0} {}
template<> inline OpenHash_T<float,int64_t>::Entry_t::Entry_t() : m_Key{0}, m_Value{0.0f} {}
template<> inline OpenHash_T<int64_t,int64_t>::Entry_t::Entry_t() : m_Key{0}, m_Value{0} {}
template<> inline OpenHash_T<uint64_t,int64_t>::Entry_t::Entry_t() : m_Key{0}, m_Value{0} {}

/////////////////////////////////////////////////////////////////////////////

/// generic stateless priority queue
template < typename T, typename COMP >
class CSphQueue
{
protected:
	T * m_pData = nullptr;
	int m_iUsed = 0;
	int m_iSize;

public:
	/// ctor
	explicit CSphQueue ( int iSize )
		: m_iSize ( iSize )
	{
		Reset ( iSize );
	}

	/// dtor
	~CSphQueue ()
	{
		SafeDeleteArray ( m_pData );
	}

	void Reset ( int iSize )
	{
		SafeDeleteArray ( m_pData );
		assert ( iSize>=0 );
		m_iSize = iSize;
		if ( iSize )
			m_pData = new T[iSize];
		assert ( !iSize || m_pData );
	}

	/// add entry to the queue
	bool Push ( const T &tEntry )
	{
		assert ( m_pData );
		if ( m_iUsed==m_iSize )
		{
			// if it's worse that current min, reject it, else pop off current min
			if ( COMP::IsLess ( tEntry, m_pData[0] ) )
				return false;
			else
				Pop ();
		}

		// do add
		m_pData[m_iUsed] = tEntry;
		int iEntry = m_iUsed++;

		// shift up if needed, so that worst (lesser) ones float to the top
		while ( iEntry )
		{
			int iParent = ( iEntry - 1 ) >> 1;
			if ( !COMP::IsLess ( m_pData[iEntry], m_pData[iParent] ) )
				break;

			// entry is less than parent, should float to the top
			Swap ( m_pData[iEntry], m_pData[iParent] );
			iEntry = iParent;
		}

		return true;
	}

	/// remove root (ie. top priority) entry
	void Pop ()
	{
		assert ( m_iUsed && m_pData );
		if ( !( --m_iUsed ) ) // empty queue? just return
			return;

		// make the last entry my new root
		m_pData[0] = m_pData[m_iUsed];

		// shift down if needed
		int iEntry = 0;
		while ( true )
		{
			// select child
			int iChild = ( iEntry << 1 ) + 1;
			if ( iChild>=m_iUsed )
				break;

			// select smallest child
			if ( iChild + 1<m_iUsed )
				if ( COMP::IsLess ( m_pData[iChild + 1], m_pData[iChild] ) )
					++iChild;

			// if smallest child is less than entry, do float it to the top
			if ( COMP::IsLess ( m_pData[iChild], m_pData[iEntry] ) )
			{
				Swap ( m_pData[iChild], m_pData[iEntry] );
				iEntry = iChild;
				continue;
			}

			break;
		}
	}

	/// get entries count
	inline int GetLength () const
	{
		return m_iUsed;
	}

	/// get current root
	inline const T &Root () const
	{
		assert ( m_iUsed && m_pData );
		return m_pData[0];
	}
};


// simple circular buffer
template < typename T >
class CircularBuffer_T
{
public:
	explicit CircularBuffer_T ( int iInitialSize=256, float fGrowFactor=1.5f )
		: m_dValues ( iInitialSize )
		, m_fGrowFactor ( fGrowFactor )
	{}

	CircularBuffer_T ( CircularBuffer_T&& rhs ) noexcept
		: CircularBuffer_T ( 0, 1.5f )
	{
		Swap ( rhs );
	}

	void Swap ( CircularBuffer_T& rhs ) noexcept
	{
		m_dValues.SwapData ( rhs.m_dValues );
		::Swap ( m_fGrowFactor, rhs.m_fGrowFactor );
		::Swap ( m_iHead, rhs.m_iHead );
		::Swap ( m_iTail, rhs.m_iTail );
		::Swap ( m_iUsed, rhs.m_iUsed );
	}

	CircularBuffer_T & operator= ( CircularBuffer_T rhs )
	{
		Swap ( rhs );
		return *this;
	}


	void Push ( const T & tValue )
	{
		if ( m_iUsed==m_dValues.GetLength() )
			Resize ( int(m_iUsed*m_fGrowFactor) );

		m_dValues[m_iTail] = tValue;
		m_iTail = ( m_iTail+1 ) % m_dValues.GetLength();
		m_iUsed++;
	}

	T & Push()
	{
		if ( m_iUsed==m_dValues.GetLength() )
			Resize ( int ( m_iUsed*m_fGrowFactor ) );

		int iOldTail = m_iTail;
		m_iTail = (m_iTail + 1) % m_dValues.GetLength ();
		m_iUsed++;

		return m_dValues[iOldTail];
	}


	T & Pop()
	{
		assert ( !IsEmpty() );
		int iOldHead = m_iHead;
		m_iHead = ( m_iHead+1 ) % m_dValues.GetLength();
		m_iUsed--;

		return m_dValues[iOldHead];
	}

	const T & Last() const
	{
		assert (!IsEmpty());
		return operator[](GetLength()-1);
	}

	T & Last()
	{
		assert (!IsEmpty());
		int iIndex = GetLength()-1;
		return m_dValues[(iIndex+m_iHead) % m_dValues.GetLength()];
	}

	const T & operator [] ( int iIndex ) const
	{
		assert ( iIndex < m_iUsed );
		return m_dValues[(iIndex+m_iHead) % m_dValues.GetLength()];
	}

	bool IsEmpty() const
	{
		return m_iUsed==0;
	}

	int GetLength() const
	{
		return m_iUsed;
	}

private:
	CSphFixedVector<T>	m_dValues;
	float				m_fGrowFactor;
	int					m_iHead = 0;
	int					m_iTail = 0;
	int					m_iUsed = 0;

	void Resize ( int iNewLength )
	{
		CSphFixedVector<T> dNew ( iNewLength );
		for ( int i = 0; i < GetLength(); i++ )
			dNew[i] = m_dValues[(i+m_iHead) % m_dValues.GetLength()];

		m_dValues.SwapData(dNew);

		m_iHead = 0;
		m_iTail = m_iUsed;
	}
};


//////////////////////////////////////////////////////////////////////////
class TDigest_i
{
public:
	virtual				~TDigest_i() {}

	virtual void		Add ( double fValue, int64_t iWeight = 1 ) = 0;
	virtual double		Percentile ( int iPercent ) const = 0;
};

TDigest_i * sphCreateTDigest();

//////////////////////////////////////////////////////////////////////////
/// simple linked list
//////////////////////////////////////////////////////////////////////////
struct ListNode_t
{
	ListNode_t * m_pPrev = nullptr;
	ListNode_t * m_pNext = nullptr;
};


/// Simple linked list.
class List_t
{
public:
	List_t ()
	{
		m_tStub.m_pPrev = &m_tStub;
		m_tStub.m_pNext = &m_tStub;
		m_iCount = 0;
	}

	/// Append the node to the tail
	void Add ( ListNode_t * pNode )
	{
		if ( !pNode )
			return;
		assert ( !pNode->m_pNext && !pNode->m_pPrev );
		pNode->m_pNext = m_tStub.m_pNext;
		pNode->m_pPrev = &m_tStub;
		m_tStub.m_pNext->m_pPrev = pNode;
		m_tStub.m_pNext = pNode;

		++m_iCount;
	}

	void HardReset()
	{
		m_tStub.m_pPrev = &m_tStub;
		m_tStub.m_pNext = &m_tStub;
		m_iCount = 0;
	}

	void Remove ( ListNode_t * pNode )
	{
		if ( !pNode )
			return;
		assert ( pNode->m_pNext && pNode->m_pPrev );
		pNode->m_pNext->m_pPrev = pNode->m_pPrev;
		pNode->m_pPrev->m_pNext = pNode->m_pNext;
		pNode->m_pNext = nullptr;
		pNode->m_pPrev = nullptr;

		--m_iCount;
	}

	inline int GetLength () const
	{
		return m_iCount;
	}

	inline const ListNode_t * Begin () const
	{
		return m_tStub.m_pNext;
	}

	inline const ListNode_t * End () const
	{
		return &m_tStub;
	}

	class Iterator_c
	{
		ListNode_t * m_pIterator = nullptr;
		ListNode_t * m_pNext = nullptr; // backup since original m.b. corrupted by dtr/free
	public:
		explicit Iterator_c ( ListNode_t * pIterator = nullptr ) : m_pIterator ( pIterator )
		{
			if ( m_pIterator )
				m_pNext = m_pIterator->m_pNext;
		}

		ListNode_t & operator* () { return *m_pIterator; };

		Iterator_c & operator++ ()
		{
			assert ( m_pNext );
			m_pIterator = m_pNext;
			m_pNext = m_pIterator->m_pNext;
			return *this;
		}

		bool operator!= ( const Iterator_c & rhs ) const
		{
			return m_pIterator!=rhs.m_pIterator;
		}
	};

	// c++11 style iteration
	Iterator_c begin () const
	{
		return Iterator_c ( m_tStub.m_pNext );
	}

	Iterator_c end () const
	{
		return Iterator_c ( const_cast<ListNode_t*> (&m_tStub) );
	}

private:
	ListNode_t m_tStub;	///< stub node
	volatile int m_iCount;
};

/// wrap raw void* into ListNode_t to store it in List_t
struct ListedData_t: public ListNode_t
{
	const void* m_pData = nullptr;

	ListedData_t() = default;
	explicit ListedData_t ( const void* pData )
		: m_pData ( pData )
	{}
};


struct NameValueStr_t
{
	CSphString	m_sName;
	CSphString	m_sValue;
};


template <typename T>
inline int sphCalcZippedLen ( T tValue )
{
	int nBytes = 1;
	tValue>>=7;
	while ( tValue )
	{
		tValue >>= 7;
		++nBytes;
	}

	return nBytes;
}


template<typename T, typename WRITER>
inline int sphZipValue ( WRITER fnPut, T tValue )
{
	int nBytes = sphCalcZippedLen ( tValue );
	for ( int i = nBytes-1; i>=0; --i )
		fnPut ( ( 0x7f & ( tValue >> ( 7 * i ) ) ) | ( i ? 0x80 : 0 ) );

	return nBytes;
}


template <typename T>
inline int sphZipToPtr ( BYTE * pData, T tValue )
{
	return sphZipValue ( [pData] ( BYTE b ) mutable { *pData++ = b; }, tValue );
}

/// Allocation for small objects (namely - for movable dynamic attributes).
/// internals based on Alexandresku's 'loki' implementation - 'Allocator for small objects'
static const int MAX_SMALL_OBJECT_SIZE = 64;

#ifdef USE_SMALLALLOC
BYTE * sphAllocateSmall ( int iBytes );
void sphDeallocateSmall ( BYTE * pBlob, int iBytes );
size_t sphGetSmallAllocatedSize ();	// how many allocated right now
size_t sphGetSmallReservedSize ();	// how many pooled from the sys right now
#else
inline BYTE * sphAllocateSmall(int iBytes) {return new BYTE[iBytes];};
inline void sphDeallocateSmall(const BYTE* pBlob, int) {delete[]pBlob;};
inline void sphDeallocateSmall(const BYTE* pBlob) {delete[]pBlob;};
inline size_t sphGetSmallAllocatedSize() {return 0;};    // how many allocated right now
inline size_t sphGetSmallReservedSize() {return 0;};    // how many pooled from the sys right now
#endif // USE_SMALLALLOC

// helper to use in vector as custom allocator
namespace sph {
	template<typename T>
	class CustomStorage_T
	{
	protected:
		/// grow enough to hold that much entries.
		inline static T * Allocate ( int iLimit )
		{
			return sphAllocateSmall ( iLimit*sizeof(T) );
		}

		inline static void Deallocate ( T * pData )
		{
			sphDeallocateSmall ( (BYTE*) pData );
		}

		static const bool is_constructed = true;
		static const bool is_owned = false;
	};
}

template<typename T>
using TightPackedVec_T = sph::Vector_T<T, sph::DefaultCopy_T<T>, sph::TightRelimit, sph::CustomStorage_T<T>>;

void sphDeallocatePacked ( BYTE * pBlob );

DWORD		sphUnzipInt ( const BYTE * & pBuf );
SphOffset_t sphUnzipOffset ( const BYTE * & pBuf );


// fast diagnostic logging.
// Being a macro, it will be optimized out by compiler when not in use

struct LogMessage_t
{
	LogMessage_t ();
	~LogMessage_t ();

	template<typename T>
	LogMessage_t & operator<< ( T && t )
	{
		m_dLog << std::forward<T> ( t );
		return *this;
	}

private:
	StringBuilder_c m_dLog;
};

// for LOG (foo, bar) -> define LOG_LEVEL_foo as boolean, define LOG_COMPONENT_bar as expression

#define LOG_MSG LogMessage_t {}
#define LOG( Level, Component ) \
    if (LOG_LEVEL_##Level) \
        LOG_MSG << LOG_COMPONENT_##Component

class LocMessages_c;
class LocMessage_c
{

	friend class LocMessages_c;
	LocMessage_c ( LocMessages_c* pOwner );

public:

	void Swap ( LocMessage_c& rhs ) noexcept
	{
		::Swap ( m_dLog, rhs.m_dLog );
	}

	LocMessage_c ( const LocMessage_c& rhs )
	{
		assert (false && "NRVO failed");
	}

	MOVE_BYSWAP ( LocMessage_c);
	~LocMessage_c ();

	template<typename T>
	LocMessage_c & operator<< ( T && t )
	{
		m_dLog << std::forward<T> ( t );
		return *this;
	}

private:
	StringBuilder_c m_dLog;
	LocMessages_c * m_pOwner = nullptr;
};

struct MsgList
{
	CSphString m_sMsg = nullptr;
	MsgList* m_pNext = nullptr;
};

class LocMessages_c : public ISphNoncopyable
{
public:
	~LocMessages_c ();

	LocMessage_c GetLoc()
	{
		return LocMessage_c(this);
	}

	int Print() const;

	void Append ( StringBuilder_c & dMsg );
	void Swap ( LocMessages_c& rhs ) noexcept;

//	CSphMutex m_tLock;
	MsgList * m_sMsgs = nullptr;
	int m_iMsgs = 0;
};

/*
 * unit logger.
 * Use LOC_ADD to add logger to a class/struct
 * Use #define LOG_LEVEL_FOO 1 - to enable logging
 * Use #define LOG_COMPONENT_BAR as informative prefix
 * Use logger as LOC(FOO,BAR) << "my cool message" for logging
 * Use m_dLogger.Print() either as direct call, either as 'evaluate expression' in debugger.
 */

#define LOC_ADD LocMessages_c    m_dLogger
#define LOC_SWAP( RHS ) m_dLogger.Swap(RHS.m_dLogger)
#define LOC_MSG m_dLogger.GetLoc()
#define LOC( Level, Component ) \
    if_const (LOG_LEVEL_##Level) \
        LOC_MSG << LOG_COMPONENT_##Component

#endif // _sphinxstd_
