/******************************************************************************
 * Copyright (c) 2011, Duane Merrill.  All rights reserved.
 * Copyright (c) 2011-2017, NVIDIA CORPORATION.  All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the NVIDIA CORPORATION nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NVIDIA CORPORATION BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 ******************************************************************************/

/**
 * \file
 * Common type manipulation (metaprogramming) utilities
 */

#pragma once

#include <iostream>
#include <limits>
#include <cfloat>

#include "util_macro.cuh"
#include "util_arch.cuh"
#include "util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup UtilModule
 * @{
 */



/******************************************************************************
 * Type equality
 ******************************************************************************/

/**
 * \brief Type selection (<tt>IF ? ThenType : ElseType</tt>)
 */
template <bool IF, typename ThenType, typename ElseType>
struct If
{
    /// Conditional type result
    typedef ThenType Type;      // true
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

template <typename ThenType, typename ElseType>
struct If<false, ThenType, ElseType>
{
    typedef ElseType Type;      // false
};

#endif // DOXYGEN_SHOULD_SKIP_THIS



/******************************************************************************
 * Conditional types
 ******************************************************************************/

/**
 * \brief Type equality test
 */
template <typename A, typename B>
struct Equals
{
    enum {
        VALUE = 0,
        NEGATE = 1
    };
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

template <typename A>
struct Equals <A, A>
{
    enum {
        VALUE = 1,
        NEGATE = 0
    };
};

#endif // DOXYGEN_SHOULD_SKIP_THIS


/******************************************************************************
 * Static math
 ******************************************************************************/

/**
 * \brief Statically determine log2(N), rounded up.
 *
 * For example:
 *     Log2<8>::VALUE   // 3
 *     Log2<3>::VALUE   // 2
 */
template <int N, int CURRENT_VAL = N, int COUNT = 0>
struct Log2
{
    /// Static logarithm value
    enum { VALUE = Log2<N, (CURRENT_VAL >> 1), COUNT + 1>::VALUE };         // Inductive case
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

template <int N, int COUNT>
struct Log2<N, 0, COUNT>
{
    enum {VALUE = (1 << (COUNT - 1) < N) ?                                  // Base case
        COUNT :
        COUNT - 1 };
};

#endif // DOXYGEN_SHOULD_SKIP_THIS


/**
 * \brief Statically determine if N is a power-of-two
 */
template <int N>
struct PowerOfTwo
{
    enum { VALUE = ((N & (N - 1)) == 0) };
};



/******************************************************************************
 * Pointer vs. iterator detection
 ******************************************************************************/

/**
 * \brief Pointer vs. iterator
 */
template <typename Tp>
struct IsPointer
{
    enum { VALUE = 0 };
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

template <typename Tp>
struct IsPointer<Tp*>
{
    enum { VALUE = 1 };
};

#endif // DOXYGEN_SHOULD_SKIP_THIS



/******************************************************************************
 * Qualifier detection
 ******************************************************************************/

/**
 * \brief Volatile modifier test
 */
template <typename Tp>
struct IsVolatile
{
    enum { VALUE = 0 };
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

template <typename Tp>
struct IsVolatile<Tp volatile>
{
    enum { VALUE = 1 };
};

#endif // DOXYGEN_SHOULD_SKIP_THIS


/******************************************************************************
 * Qualifier removal
 ******************************************************************************/

/**
 * \brief Removes \p const and \p volatile qualifiers from type \p Tp.
 *
 * For example:
 *     <tt>typename RemoveQualifiers<volatile int>::Type         // int;</tt>
 */
template <typename Tp, typename Up = Tp>
struct RemoveQualifiers
{
    /// Type without \p const and \p volatile qualifiers
    typedef Up Type;
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

template <typename Tp, typename Up>
struct RemoveQualifiers<Tp, volatile Up>
{
    typedef Up Type;
};

template <typename Tp, typename Up>
struct RemoveQualifiers<Tp, const Up>
{
    typedef Up Type;
};

template <typename Tp, typename Up>
struct RemoveQualifiers<Tp, const volatile Up>
{
    typedef Up Type;
};


/******************************************************************************
 * Marker types
 ******************************************************************************/

/**
 * \brief A simple "NULL" marker type
 */
struct NullType
{
#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

    template <typename T>
    __host__ __device__ __forceinline__ NullType& operator =(const T&) { return *this; }

    __host__ __device__ __forceinline__ bool operator ==(const NullType&) { return true; }

    __host__ __device__ __forceinline__ bool operator !=(const NullType&) { return false; }

#endif // DOXYGEN_SHOULD_SKIP_THIS
};


/**
 * \brief Allows for the treatment of an integral constant as a type at compile-time (e.g., to achieve static call dispatch based on constant integral values)
 */
template <int A>
struct Int2Type
{
   enum {VALUE = A};
};


#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document


/******************************************************************************
 * Size and alignment
 ******************************************************************************/

/// Structure alignment
template <typename T>
struct AlignBytes
{
    struct Pad
    {
        T       val;
        char    byte;
    };

    enum
    {
        /// The "true CUDA" alignment of T in bytes
        ALIGN_BYTES = sizeof(Pad) - sizeof(T)
    };

    /// The "truly aligned" type
    typedef T Type;
};

// Specializations where host C++ compilers (e.g., 32-bit Windows) may disagree
// with device C++ compilers (EDG) on types passed as template parameters through
// kernel functions

#define __CUB_ALIGN_BYTES(t, b)         \
    template <> struct AlignBytes<t>    \
    { enum { ALIGN_BYTES = b }; typedef __align__(b) t Type; };

__CUB_ALIGN_BYTES(short4, 8)
__CUB_ALIGN_BYTES(ushort4, 8)
__CUB_ALIGN_BYTES(int2, 8)
__CUB_ALIGN_BYTES(uint2, 8)
__CUB_ALIGN_BYTES(long long, 8)
__CUB_ALIGN_BYTES(unsigned long long, 8)
__CUB_ALIGN_BYTES(float2, 8)
__CUB_ALIGN_BYTES(double, 8)
#ifdef _WIN32
    __CUB_ALIGN_BYTES(long2, 8)
    __CUB_ALIGN_BYTES(ulong2, 8)
#else
    __CUB_ALIGN_BYTES(long2, 16)
    __CUB_ALIGN_BYTES(ulong2, 16)
#endif
__CUB_ALIGN_BYTES(int4, 16)
__CUB_ALIGN_BYTES(uint4, 16)
__CUB_ALIGN_BYTES(float4, 16)
__CUB_ALIGN_BYTES(long4, 16)
__CUB_ALIGN_BYTES(ulong4, 16)
__CUB_ALIGN_BYTES(longlong2, 16)
__CUB_ALIGN_BYTES(ulonglong2, 16)
__CUB_ALIGN_BYTES(double2, 16)
__CUB_ALIGN_BYTES(longlong4, 16)
__CUB_ALIGN_BYTES(ulonglong4, 16)
__CUB_ALIGN_BYTES(double4, 16)

template <typename T> struct AlignBytes<volatile T> : AlignBytes<T> {};
template <typename T> struct AlignBytes<const T> : AlignBytes<T> {};
template <typename T> struct AlignBytes<const volatile T> : AlignBytes<T> {};


/// Unit-words of data movement
template <typename T>
struct UnitWord
{
    enum {
        ALIGN_BYTES = AlignBytes<T>::ALIGN_BYTES
    };

    template <typename Unit>
    struct IsMultiple
    {
        enum {
            UNIT_ALIGN_BYTES    = AlignBytes<Unit>::ALIGN_BYTES,
            IS_MULTIPLE         = (sizeof(T) % sizeof(Unit) == 0) && (ALIGN_BYTES % UNIT_ALIGN_BYTES == 0)
        };
    };

    /// Biggest shuffle word that T is a whole multiple of and is not larger than the alignment of T
    typedef typename If<IsMultiple<int>::IS_MULTIPLE,
        unsigned int,
        typename If<IsMultiple<short>::IS_MULTIPLE,
            unsigned short,
            unsigned char>::Type>::Type         ShuffleWord;

    /// Biggest volatile word that T is a whole multiple of and is not larger than the alignment of T
    typedef typename If<IsMultiple<long long>::IS_MULTIPLE,
        unsigned long long,
        ShuffleWord>::Type                      VolatileWord;

    /// Biggest memory-access word that T is a whole multiple of and is not larger than the alignment of T
    typedef typename If<IsMultiple<longlong2>::IS_MULTIPLE,
        ulonglong2,
        VolatileWord>::Type                     DeviceWord;

    /// Biggest texture reference word that T is a whole multiple of and is not larger than the alignment of T
    typedef typename If<IsMultiple<int4>::IS_MULTIPLE,
        uint4,
        typename If<IsMultiple<int2>::IS_MULTIPLE,
            uint2,
            ShuffleWord>::Type>::Type           TextureWord;
};


// float2 specialization workaround (for SM10-SM13)
template <>
struct UnitWord <float2>
{
    typedef int         ShuffleWord;
#if (CUB_PTX_ARCH > 0) && (CUB_PTX_ARCH <= 130)
    typedef float       VolatileWord;
    typedef uint2       DeviceWord;
#else
    typedef unsigned long long   VolatileWord;
    typedef unsigned long long   DeviceWord;
#endif
    typedef float2      TextureWord;
};

// float4 specialization workaround (for SM10-SM13)
template <>
struct UnitWord <float4>
{
    typedef int         ShuffleWord;
#if (CUB_PTX_ARCH > 0) && (CUB_PTX_ARCH <= 130)
    typedef float               VolatileWord;
    typedef uint4               DeviceWord;
#else
    typedef unsigned long long  VolatileWord;
    typedef ulonglong2          DeviceWord;
#endif
    typedef float4              TextureWord;
};


// char2 specialization workaround (for SM10-SM13)
template <>
struct UnitWord <char2>
{
    typedef unsigned short      ShuffleWord;
#if (CUB_PTX_ARCH > 0) && (CUB_PTX_ARCH <= 130)
    typedef unsigned short      VolatileWord;
    typedef short               DeviceWord;
#else
    typedef unsigned short      VolatileWord;
    typedef unsigned short      DeviceWord;
#endif
    typedef unsigned short      TextureWord;
};


template <typename T> struct UnitWord<volatile T> : UnitWord<T> {};
template <typename T> struct UnitWord<const T> : UnitWord<T> {};
template <typename T> struct UnitWord<const volatile T> : UnitWord<T> {};


#endif // DOXYGEN_SHOULD_SKIP_THIS



/******************************************************************************
 * Vector type inference utilities.
 ******************************************************************************/

/**
 * \brief Exposes a member typedef \p Type that names the corresponding CUDA vector type if one exists.  Otherwise \p Type refers to the CubVector structure itself, which will wrap the corresponding \p x, \p y, etc. vector fields.
 */
template <typename T, int vec_elements> struct CubVector;

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

enum
{
    /// The maximum number of elements in CUDA vector types
    MAX_VEC_ELEMENTS = 4,
};


/**
 * Generic vector-1 type
 */
template <typename T>
struct CubVector<T, 1>
{
    T x;

    typedef T BaseType;
    typedef CubVector<T, 1> Type;
};

/**
 * Generic vector-2 type
 */
template <typename T>
struct CubVector<T, 2>
{
    T x;
    T y;

    typedef T BaseType;
    typedef CubVector<T, 2> Type;
};

/**
 * Generic vector-3 type
 */
template <typename T>
struct CubVector<T, 3>
{
    T x;
    T y;
    T z;

    typedef T BaseType;
    typedef CubVector<T, 3> Type;
};

/**
 * Generic vector-4 type
 */
template <typename T>
struct CubVector<T, 4>
{
    T x;
    T y;
    T z;
    T w;

    typedef T BaseType;
    typedef CubVector<T, 4> Type;
};


/**
 * Macro for expanding partially-specialized built-in vector types
 */
#define CUB_DEFINE_VECTOR_TYPE(base_type,short_type)                                                    \
                                                                                                        \
    template<> struct CubVector<base_type, 1> : short_type##1                                           \
    {                                                                                                   \
      typedef base_type       BaseType;                                                                 \
      typedef short_type##1   Type;                                                                     \
      __host__ __device__ __forceinline__ CubVector operator+(const CubVector &other) const {           \
          CubVector retval;                                                                             \
          retval.x = x + other.x;                                                                       \
          return retval;                                                                                \
      }                                                                                                 \
      __host__ __device__ __forceinline__ CubVector operator-(const CubVector &other) const {           \
          CubVector retval;                                                                             \
          retval.x = x - other.x;                                                                       \
          return retval;                                                                                \
      }                                                                                                 \
    };                                                                                                  \
                                                                                                        \
    template<> struct CubVector<base_type, 2> : short_type##2                                           \
    {                                                                                                   \
        typedef base_type       BaseType;                                                               \
        typedef short_type##2   Type;                                                                   \
        __host__ __device__ __forceinline__ CubVector operator+(const CubVector &other) const {         \
            CubVector retval;                                                                           \
            retval.x = x + other.x;                                                                     \
            retval.y = y + other.y;                                                                     \
            return retval;                                                                              \
        }                                                                                               \
        __host__ __device__ __forceinline__ CubVector operator-(const CubVector &other) const {         \
            CubVector retval;                                                                           \
            retval.x = x - other.x;                                                                     \
            retval.y = y - other.y;                                                                     \
            return retval;                                                                              \
        }                                                                                               \
    };                                                                                                  \
                                                                                                        \
    template<> struct CubVector<base_type, 3> : short_type##3                                           \
    {                                                                                                   \
        typedef base_type       BaseType;                                                               \
        typedef short_type##3   Type;                                                                   \
        __host__ __device__ __forceinline__ CubVector operator+(const CubVector &other) const {         \
            CubVector retval;                                                                           \
            retval.x = x + other.x;                                                                     \
            retval.y = y + other.y;                                                                     \
            retval.z = z + other.z;                                                                     \
            return retval;                                                                              \
        }                                                                                               \
        __host__ __device__ __forceinline__ CubVector operator-(const CubVector &other) const {         \
            CubVector retval;                                                                           \
            retval.x = x - other.x;                                                                     \
            retval.y = y - other.y;                                                                     \
            retval.z = z - other.z;                                                                     \
            return retval;                                                                              \
        }                                                                                               \
    };                                                                                                  \
                                                                                                        \
    template<> struct CubVector<base_type, 4> : short_type##4                                           \
    {                                                                                                   \
        typedef base_type       BaseType;                                                               \
        typedef short_type##4   Type;                                                                   \
        __host__ __device__ __forceinline__ CubVector operator+(const CubVector &other) const {         \
            CubVector retval;                                                                           \
            retval.x = x + other.x;                                                                     \
            retval.y = y + other.y;                                                                     \
            retval.z = z + other.z;                                                                     \
            retval.w = w + other.w;                                                                     \
            return retval;                                                                              \
        }                                                                                               \
        __host__ __device__ __forceinline__ CubVector operator-(const CubVector &other) const {         \
            CubVector retval;                                                                           \
            retval.x = x - other.x;                                                                     \
            retval.y = y - other.y;                                                                     \
            retval.z = z - other.z;                                                                     \
            retval.w = w - other.w;                                                                     \
            return retval;                                                                              \
        }                                                                                               \
    };



// Expand CUDA vector types for built-in primitives
CUB_DEFINE_VECTOR_TYPE(char,               char)
CUB_DEFINE_VECTOR_TYPE(signed char,        char)
CUB_DEFINE_VECTOR_TYPE(short,              short)
CUB_DEFINE_VECTOR_TYPE(int,                int)
CUB_DEFINE_VECTOR_TYPE(long,               long)
CUB_DEFINE_VECTOR_TYPE(long long,          longlong)
CUB_DEFINE_VECTOR_TYPE(unsigned char,      uchar)
CUB_DEFINE_VECTOR_TYPE(unsigned short,     ushort)
CUB_DEFINE_VECTOR_TYPE(unsigned int,       uint)
CUB_DEFINE_VECTOR_TYPE(unsigned long,      ulong)
CUB_DEFINE_VECTOR_TYPE(unsigned long long, ulonglong)
CUB_DEFINE_VECTOR_TYPE(float,              float)
CUB_DEFINE_VECTOR_TYPE(double,             double)
CUB_DEFINE_VECTOR_TYPE(bool,               uchar)

// Undefine macros
#undef CUB_DEFINE_VECTOR_TYPE

#endif // DOXYGEN_SHOULD_SKIP_THIS



/******************************************************************************
 * Wrapper types
 ******************************************************************************/

/**
 * \brief A storage-backing wrapper that allows types with non-trivial constructors to be aliased in unions
 */
template <typename T>
struct Uninitialized
{
    /// Biggest memory-access word that T is a whole multiple of and is not larger than the alignment of T
    typedef typename UnitWord<T>::DeviceWord DeviceWord;

    enum
    {
        WORDS = sizeof(T) / sizeof(DeviceWord)
    };

    /// Backing storage
    DeviceWord storage[WORDS];

    /// Alias
    __host__ __device__ __forceinline__ T& Alias()
    {
        return reinterpret_cast<T&>(*this);
    }
};


/**
 * \brief A key identifier paired with a corresponding value
 */
template <
    typename    _Key,
    typename    _Value
#if defined(_WIN32) && !defined(_WIN64)
    , bool KeyIsLT = (AlignBytes<_Key>::ALIGN_BYTES < AlignBytes<_Value>::ALIGN_BYTES)
    , bool ValIsLT = (AlignBytes<_Value>::ALIGN_BYTES < AlignBytes<_Key>::ALIGN_BYTES)
#endif // #if defined(_WIN32) && !defined(_WIN64)
    >
struct KeyValuePair
{
    typedef _Key    Key;                ///< Key data type
    typedef _Value  Value;              ///< Value data type

    Key     key;                        ///< Item key
    Value   value;                      ///< Item value

    /// Constructor
    __host__ __device__ __forceinline__
    KeyValuePair() {}

    /// Constructor
    __host__ __device__ __forceinline__
    KeyValuePair(Key const& key, Value const& value) : key(key), value(value) {}

    /// Inequality operator
    __host__ __device__ __forceinline__ bool operator !=(const KeyValuePair &b)
    {
        return (value != b.value) || (key != b.key);
    }
};

#if defined(_WIN32) && !defined(_WIN64)

/**
 * Win32 won't do 16B alignment.  This can present two problems for
 * should-be-16B-aligned (but actually 8B aligned) built-in and intrinsics members:
 * 1) If a smaller-aligned item were to be listed first, the host compiler places the
 *    should-be-16B item at too early an offset (and disagrees with device compiler)
 * 2) Or, if a smaller-aligned item lists second, the host compiler gets the size
 *    of the struct wrong (and disagrees with device compiler)
 *
 * So we put the larger-should-be-aligned item first, and explicitly pad the
 * end of the struct
 */

/// Smaller key specialization
template <typename K, typename V>
struct KeyValuePair<K, V, true, false>
{
    typedef K Key;
    typedef V Value;

    typedef char Pad[AlignBytes<V>::ALIGN_BYTES - AlignBytes<K>::ALIGN_BYTES];

    Value   value;  // Value has larger would-be alignment and goes first
    Key     key;
    Pad     pad;

    /// Constructor
    __host__ __device__ __forceinline__
    KeyValuePair() {}

    /// Constructor
    __host__ __device__ __forceinline__
    KeyValuePair(Key const& key, Value const& value) : key(key), value(value) {}

    /// Inequality operator
    __host__ __device__ __forceinline__ bool operator !=(const KeyValuePair &b)
    {
        return (value != b.value) || (key != b.key);
    }
};


/// Smaller value specialization
template <typename K, typename V>
struct KeyValuePair<K, V, false, true>
{
    typedef K Key;
    typedef V Value;

    typedef char Pad[AlignBytes<K>::ALIGN_BYTES - AlignBytes<V>::ALIGN_BYTES];

    Key     key;    // Key has larger would-be alignment and goes first
    Value   value;
    Pad     pad;

    /// Constructor
    __host__ __device__ __forceinline__
    KeyValuePair() {}

    /// Constructor
    __host__ __device__ __forceinline__
    KeyValuePair(Key const& key, Value const& value) : key(key), value(value) {}

    /// Inequality operator
    __host__ __device__ __forceinline__ bool operator !=(const KeyValuePair &b)
    {
        return (value != b.value) || (key != b.key);
    }
};

#endif // #if defined(_WIN32) && !defined(_WIN64)


#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document


/**
 * \brief A wrapper for passing simple static arrays as kernel parameters
 */
template <typename T, int COUNT>
struct ArrayWrapper
{

    /// Statically-sized array of type \p T
    T array[COUNT];

    /// Constructor
    __host__ __device__ __forceinline__ ArrayWrapper() {}
};

#endif // DOXYGEN_SHOULD_SKIP_THIS

/**
 * \brief Double-buffer storage wrapper for multi-pass stream transformations that require more than one storage array for streaming intermediate results back and forth.
 *
 * Many multi-pass computations require a pair of "ping-pong" storage
 * buffers (e.g., one for reading from and the other for writing to, and then
 * vice-versa for the subsequent pass).  This structure wraps a set of device
 * buffers and a "selector" member to track which is "current".
 */
template <typename T>
struct DoubleBuffer
{
    /// Pair of device buffer pointers
    T *d_buffers[2];

    ///  Selector into \p d_buffers (i.e., the active/valid buffer)
    int selector;

    /// \brief Constructor
    __host__ __device__ __forceinline__ DoubleBuffer()
    {
        selector = 0;
        d_buffers[0] = NULL;
        d_buffers[1] = NULL;
    }

    /// \brief Constructor
    __host__ __device__ __forceinline__ DoubleBuffer(
        T *d_current,         ///< The currently valid buffer
        T *d_alternate)       ///< Alternate storage buffer of the same size as \p d_current
    {
        selector = 0;
        d_buffers[0] = d_current;
        d_buffers[1] = d_alternate;
    }

    /// \brief Return pointer to the currently valid buffer
    __host__ __device__ __forceinline__ T* Current() { return d_buffers[selector]; }

    /// \brief Return pointer to the currently invalid buffer
    __host__ __device__ __forceinline__ T* Alternate() { return d_buffers[selector ^ 1]; }

};



/******************************************************************************
 * Typedef-detection
 ******************************************************************************/


/**
 * \brief Defines a structure \p detector_name that is templated on type \p T.  The \p detector_name struct exposes a constant member \p VALUE indicating whether or not parameter \p T exposes a nested type \p nested_type_name
 */
#define CUB_DEFINE_DETECT_NESTED_TYPE(detector_name, nested_type_name)  \
    template <typename T>                                               \
    struct detector_name                                                \
    {                                                                   \
        template <typename C>                                           \
        static char& test(typename C::nested_type_name*);               \
        template <typename>                                             \
        static int& test(...);                                          \
        enum                                                            \
        {                                                               \
            VALUE = sizeof(test<T>(0)) < sizeof(int)                    \
        };                                                              \
    };



/******************************************************************************
 * Simple enable-if (similar to Boost)
 ******************************************************************************/

/**
 * \brief Simple enable-if (similar to Boost)
 */
template <bool Condition, class T = void>
struct EnableIf
{
    /// Enable-if type for SFINAE dummy variables
    typedef T Type;
};


template <class T>
struct EnableIf<false, T> {};



/******************************************************************************
 * Typedef-detection
 ******************************************************************************/

/**
 * \brief Determine whether or not BinaryOp's functor is of the form <tt>bool operator()(const T& a, const T&b)</tt> or <tt>bool operator()(const T& a, const T&b, unsigned int idx)</tt>
 */
template <typename T, typename BinaryOp>
struct BinaryOpHasIdxParam
{
private:
/*
    template <typename BinaryOpT, bool (BinaryOpT::*)(const T &a, const T &b, unsigned int idx) const>  struct SFINAE1 {};
    template <typename BinaryOpT, bool (BinaryOpT::*)(const T &a, const T &b, unsigned int idx)>        struct SFINAE2 {};
    template <typename BinaryOpT, bool (BinaryOpT::*)(T a, T b, unsigned int idx) const>                struct SFINAE3 {};
    template <typename BinaryOpT, bool (BinaryOpT::*)(T a, T b, unsigned int idx)>                      struct SFINAE4 {};
*/
    template <typename BinaryOpT, bool (BinaryOpT::*)(const T &a, const T &b, int idx) const>           struct SFINAE5 {};
    template <typename BinaryOpT, bool (BinaryOpT::*)(const T &a, const T &b, int idx)>                 struct SFINAE6 {};
    template <typename BinaryOpT, bool (BinaryOpT::*)(T a, T b, int idx) const>                         struct SFINAE7 {};
    template <typename BinaryOpT, bool (BinaryOpT::*)(T a, T b, int idx)>                               struct SFINAE8 {};
/*
    template <typename BinaryOpT> static char Test(SFINAE1<BinaryOpT, &BinaryOpT::operator()> *);
    template <typename BinaryOpT> static char Test(SFINAE2<BinaryOpT, &BinaryOpT::operator()> *);
    template <typename BinaryOpT> static char Test(SFINAE3<BinaryOpT, &BinaryOpT::operator()> *);
    template <typename BinaryOpT> static char Test(SFINAE4<BinaryOpT, &BinaryOpT::operator()> *);
*/
    template <typename BinaryOpT> static char Test(SFINAE5<BinaryOpT, &BinaryOpT::operator()> *);
    template <typename BinaryOpT> static char Test(SFINAE6<BinaryOpT, &BinaryOpT::operator()> *);
    template <typename BinaryOpT> static char Test(SFINAE7<BinaryOpT, &BinaryOpT::operator()> *);
    template <typename BinaryOpT> static char Test(SFINAE8<BinaryOpT, &BinaryOpT::operator()> *);

    template <typename BinaryOpT> static int Test(...);

public:

    /// Whether the functor BinaryOp has a third <tt>unsigned int</tt> index param
    static const bool HAS_PARAM = sizeof(Test<BinaryOp>(NULL)) == sizeof(char);
};




/******************************************************************************
 * Simple type traits utilities.
 *
 * For example:
 *     Traits<int>::CATEGORY             // SIGNED_INTEGER
 *     Traits<NullType>::NULL_TYPE       // true
 *     Traits<uint4>::CATEGORY           // NOT_A_NUMBER
 *     Traits<uint4>::PRIMITIVE;         // false
 *
 ******************************************************************************/

/**
 * \brief Basic type traits categories
 */
enum Category
{
    NOT_A_NUMBER,
    SIGNED_INTEGER,
    UNSIGNED_INTEGER,
    FLOATING_POINT
};


/**
 * \brief Basic type traits
 */
template <Category _CATEGORY, bool _PRIMITIVE, bool _NULL_TYPE, typename _UnsignedBits, typename T>
struct BaseTraits
{
    /// Category
    static const Category CATEGORY      = _CATEGORY;
    enum
    {
        PRIMITIVE       = _PRIMITIVE,
        NULL_TYPE       = _NULL_TYPE,
    };
};


/**
 * Basic type traits (unsigned primitive specialization)
 */
template <typename _UnsignedBits, typename T>
struct BaseTraits<UNSIGNED_INTEGER, true, false, _UnsignedBits, T>
{
    typedef _UnsignedBits       UnsignedBits;

    static const Category       CATEGORY    = UNSIGNED_INTEGER;
    static const UnsignedBits   LOWEST_KEY  = UnsignedBits(0);
    static const UnsignedBits   MAX_KEY     = UnsignedBits(-1);

    enum
    {
        PRIMITIVE       = true,
        NULL_TYPE       = false,
    };


    static __device__ __forceinline__ UnsignedBits TwiddleIn(UnsignedBits key)
    {
        return key;
    }

    static __device__ __forceinline__ UnsignedBits TwiddleOut(UnsignedBits key)
    {
        return key;
    }

    static __host__ __device__ __forceinline__ T Max()
    {
        UnsignedBits retval = MAX_KEY;
        return reinterpret_cast<T&>(retval);
    }

    static __host__ __device__ __forceinline__ T Lowest()
    {
        UnsignedBits retval = LOWEST_KEY;
        return reinterpret_cast<T&>(retval);
    }
};


/**
 * Basic type traits (signed primitive specialization)
 */
template <typename _UnsignedBits, typename T>
struct BaseTraits<SIGNED_INTEGER, true, false, _UnsignedBits, T>
{
    typedef _UnsignedBits       UnsignedBits;

    static const Category       CATEGORY    = SIGNED_INTEGER;
    static const UnsignedBits   HIGH_BIT    = UnsignedBits(1) << ((sizeof(UnsignedBits) * 8) - 1);
    static const UnsignedBits   LOWEST_KEY  = HIGH_BIT;
    static const UnsignedBits   MAX_KEY     = UnsignedBits(-1) ^ HIGH_BIT;

    enum
    {
        PRIMITIVE       = true,
        NULL_TYPE       = false,
    };

    static __device__ __forceinline__ UnsignedBits TwiddleIn(UnsignedBits key)
    {
        return key ^ HIGH_BIT;
    };

    static __device__ __forceinline__ UnsignedBits TwiddleOut(UnsignedBits key)
    {
        return key ^ HIGH_BIT;
    };

    static __host__ __device__ __forceinline__ T Max()
    {
        UnsignedBits retval = MAX_KEY;
        return reinterpret_cast<T&>(retval);
    }

    static __host__ __device__ __forceinline__ T Lowest()
    {
        UnsignedBits retval = LOWEST_KEY;
        return reinterpret_cast<T&>(retval);
    }
};

template <typename _T>
struct FpLimits;

template <>
struct FpLimits<float>
{
    static __host__ __device__ __forceinline__ float Max() {
        return FLT_MAX;
    }

    static __host__ __device__ __forceinline__ float Lowest() {
        return FLT_MAX * float(-1);
    }
};

template <>
struct FpLimits<double>
{
    static __host__ __device__ __forceinline__ double Max() {
        return DBL_MAX;
    }

    static __host__ __device__ __forceinline__ double Lowest() {
        return DBL_MAX  * double(-1);
    }
};


/**
 * Basic type traits (fp primitive specialization)
 */
template <typename _UnsignedBits, typename T>
struct BaseTraits<FLOATING_POINT, true, false, _UnsignedBits, T>
{
    typedef _UnsignedBits       UnsignedBits;

    static const Category       CATEGORY    = FLOATING_POINT;
    static const UnsignedBits   HIGH_BIT    = UnsignedBits(1) << ((sizeof(UnsignedBits) * 8) - 1);
    static const UnsignedBits   LOWEST_KEY  = UnsignedBits(-1);
    static const UnsignedBits   MAX_KEY     = UnsignedBits(-1) ^ HIGH_BIT;

    enum
    {
        PRIMITIVE       = true,
        NULL_TYPE       = false,
    };

    static __device__ __forceinline__ UnsignedBits TwiddleIn(UnsignedBits key)
    {
        UnsignedBits mask = (key & HIGH_BIT) ? UnsignedBits(-1) : HIGH_BIT;
        return key ^ mask;
    };

    static __device__ __forceinline__ UnsignedBits TwiddleOut(UnsignedBits key)
    {
        UnsignedBits mask = (key & HIGH_BIT) ? HIGH_BIT : UnsignedBits(-1);
        return key ^ mask;
    };

    static __host__ __device__ __forceinline__ T Max() {
        return FpLimits<T>::Max();
    }

    static __host__ __device__ __forceinline__ T Lowest() {
        return FpLimits<T>::Lowest();
    }
};


/**
 * \brief Numeric type traits
 */
template <typename T> struct NumericTraits :            BaseTraits<NOT_A_NUMBER, false, false, T, T> {};

template <> struct NumericTraits<NullType> :            BaseTraits<NOT_A_NUMBER, false, true, NullType, NullType> {};

template <> struct NumericTraits<char> :                BaseTraits<(std::numeric_limits<char>::is_signed) ? SIGNED_INTEGER : UNSIGNED_INTEGER, true, false, unsigned char, char> {};
template <> struct NumericTraits<signed char> :         BaseTraits<SIGNED_INTEGER, true, false, unsigned char, signed char> {};
template <> struct NumericTraits<short> :               BaseTraits<SIGNED_INTEGER, true, false, unsigned short, short> {};
template <> struct NumericTraits<int> :                 BaseTraits<SIGNED_INTEGER, true, false, unsigned int, int> {};
template <> struct NumericTraits<long> :                BaseTraits<SIGNED_INTEGER, true, false, unsigned long, long> {};
template <> struct NumericTraits<long long> :           BaseTraits<SIGNED_INTEGER, true, false, unsigned long long, long long> {};

template <> struct NumericTraits<unsigned char> :       BaseTraits<UNSIGNED_INTEGER, true, false, unsigned char, unsigned char> {};
template <> struct NumericTraits<unsigned short> :      BaseTraits<UNSIGNED_INTEGER, true, false, unsigned short, unsigned short> {};
template <> struct NumericTraits<unsigned int> :        BaseTraits<UNSIGNED_INTEGER, true, false, unsigned int, unsigned int> {};
template <> struct NumericTraits<unsigned long> :       BaseTraits<UNSIGNED_INTEGER, true, false, unsigned long, unsigned long> {};
template <> struct NumericTraits<unsigned long long> :  BaseTraits<UNSIGNED_INTEGER, true, false, unsigned long long, unsigned long long> {};

template <> struct NumericTraits<float> :               BaseTraits<FLOATING_POINT, true, false, unsigned int, float> {};
template <> struct NumericTraits<double> :              BaseTraits<FLOATING_POINT, true, false, unsigned long long, double> {};

template <> struct NumericTraits<bool> :                BaseTraits<UNSIGNED_INTEGER, true, false, typename UnitWord<bool>::VolatileWord, bool> {};



/**
 * \brief Type traits
 */
template <typename T>
struct Traits : NumericTraits<typename RemoveQualifiers<T>::Type> {};


#endif // DOXYGEN_SHOULD_SKIP_THIS


/** @} */       // end group UtilModule

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
