//
// XMLString.h
//
// Library: XML
// Package: XML
// Module:  XMLString
//
// Definition of the XMLString class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLString_INCLUDED
#define XML_XMLString_INCLUDED


#include "Poco/XML/XML.h"

#include <cstddef>
#include <string>
#include <string_view>


namespace Poco
{
namespace XML
{


/// Storage for XML strings, which may hold secrets from configuration files.
/// The defaults use malloc; the host application overrides them with memory excluded from core dumps.
void * allocateNoDump(std::size_t bytes);
void deallocateNoDump(void * ptr) noexcept;

template <class T>
struct NoDumpAllocator
{
    using value_type = T;
    NoDumpAllocator() = default;
    template <class U>
    NoDumpAllocator(const NoDumpAllocator<U> &)
    {
    }
    T * allocate(std::size_t n) { return static_cast<T *>(allocateNoDump(n * sizeof(T))); }
    void deallocate(T * ptr, std::size_t) noexcept { deallocateNoDump(ptr); }
};
template <class T, class U>
bool operator==(const NoDumpAllocator<T> &, const NoDumpAllocator<U> &)
{
    return true;
}


//
// The XML parser uses the string classes provided by the C++
// standard library (based on the basic_string<> template)
// with NoDumpAllocator. In Unicode mode, wchar_t characters
// are used, otherwise char.
// To turn on Unicode mode, #define XML_UNICODE and
// XML_UNICODE_WCHAR_T when compiling the library.
//
// XML_UNICODE  XML_UNICODE_WCHAR_T  XMLChar    XMLString
// --------------------------------------------------------------
//     N                 N           char       std::basic_string<char, ..., NoDumpAllocator<char>>
//     N                 Y           wchar_t    std::basic_string<wchar_t, ..., NoDumpAllocator<wchar_t>>
//     Y                 Y           wchar_t    std::basic_string<wchar_t, ..., NoDumpAllocator<wchar_t>>
//     Y                 N           <not supported>
//
#if defined(XML_UNICODE_WCHAR_T)

    // Unicode - use wchar_t
    using XMLChar = wchar_t;
    using XMLString = std::basic_string<wchar_t, std::char_traits<wchar_t>, NoDumpAllocator<wchar_t>>;

    std::string fromXMLString(const XMLString & str);
    /// Converts an XMLString into an UTF-8 encoded
    /// string.

    XMLString toXMLString(const std::string & str);
    /// Converts an UTF-8 encoded string into an
    /// XMLString

#    define XML_LIT(lit) L##lit

#elif defined(XML_UNICODE)

    // not supported - leave XMLString undefined

#else

    // Characters are UTF-8 encoded
    using XMLChar = char;
    using XMLString = std::basic_string<char, std::char_traits<char>, NoDumpAllocator<char>>;

    inline std::string fromXMLString(const XMLString & str)
    {
        return {str.data(), str.size()};
    }

    inline XMLString toXMLString(std::string_view str)
    {
        return {str.data(), str.size()};
    }

#    define XML_LIT(lit) lit

#endif


}
} // namespace Poco::XML


#endif // XML_XMLString_INCLUDED
