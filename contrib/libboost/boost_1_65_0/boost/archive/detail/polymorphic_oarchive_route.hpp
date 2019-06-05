#ifndef BOOST_ARCHIVE_DETAIL_POLYMORPHIC_OARCHIVE_ROUTE_HPP
#define BOOST_ARCHIVE_DETAIL_POLYMORPHIC_OARCHIVE_ROUTE_HPP

// MS compatible compilers support #pragma once
#if defined(_MSC_VER)
# pragma once
#endif

/////////1/////////2/////////3/////////4/////////5/////////6/////////7/////////8
// polymorphic_oarchive_route.hpp

// (C) Copyright 2002 Robert Ramey - http://www.rrsd.com .
// Use, modification and distribution is subject to the Boost Software
// License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org for updates, documentation, and revision history.

#include <string>
#include <ostream>
#include <cstddef> // size_t

#include <boost/config.hpp>
#if defined(BOOST_NO_STDC_NAMESPACE)
namespace std{
    using ::size_t;
} // namespace std
#endif

#include <boost/cstdint.hpp>
#include <boost/integer_traits.hpp>
#include <boost/archive/polymorphic_oarchive.hpp>
#include <boost/archive/detail/abi_prefix.hpp> // must be the last header

namespace boost {
namespace serialization {
    class extended_type_info;
} // namespace serialization
namespace archive {
namespace detail{

class basic_oserializer;
class basic_pointer_oserializer;

#ifdef BOOST_MSVC
#  pragma warning(push)
#  pragma warning(disable : 4511 4512)
#endif

template<class ArchiveImplementation>
class polymorphic_oarchive_route :
    public polymorphic_oarchive,
    // note: gcc dynamic cross cast fails if the the derivation below is
    // not public.  I think this is a mistake.
    public /*protected*/ ArchiveImplementation
{
private:
    // these are used by the serialization library.
    virtual void save_object(
        const void *x,
        const detail::basic_oserializer & bos
    ){
        ArchiveImplementation::save_object(x, bos);
    }
    virtual void save_pointer(
        const void * t,
        const detail::basic_pointer_oserializer * bpos_ptr
    ){
        ArchiveImplementation::save_pointer(t, bpos_ptr);
    }
    virtual void save_null_pointer(){
        ArchiveImplementation::save_null_pointer();
    }
    // primitive types the only ones permitted by polymorphic archives
    virtual void save(const bool t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const char t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const signed char t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const unsigned char t){
        ArchiveImplementation::save(t);
    }
    #ifndef BOOST_NO_CWCHAR
    #ifndef BOOST_NO_INTRINSIC_WCHAR_T
    virtual void save(const wchar_t t){
        ArchiveImplementation::save(t);
    }
    #endif
    #endif
    virtual void save(const short t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const unsigned short t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const int t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const unsigned int t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const long t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const unsigned long t){
        ArchiveImplementation::save(t);
    }
    #if defined(BOOST_HAS_LONG_LONG)
    virtual void save(const boost::long_long_type t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const boost::ulong_long_type t){
        ArchiveImplementation::save(t);
    }
    #elif defined(BOOST_HAS_MS_INT64)
    virtual void save(const boost::int64_t t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const boost::uint64_t t){
        ArchiveImplementation::save(t);
    }
    #endif
    virtual void save(const float t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const double t){
        ArchiveImplementation::save(t);
    }
    virtual void save(const std::string & t){
        ArchiveImplementation::save(t);
    }
    #ifndef BOOST_NO_STD_WSTRING
    virtual void save(const std::wstring & t){
        ArchiveImplementation::save(t);
    }
    #endif
    virtual library_version_type get_library_version() const{
        return ArchiveImplementation::get_library_version();
    }
    virtual unsigned int get_flags() const {
        return ArchiveImplementation::get_flags();
    }
    virtual void save_binary(const void * t, std::size_t size){
        ArchiveImplementation::save_binary(t, size);
    }
    // used for xml and other tagged formats default does nothing
    virtual void save_start(const char * name){
        ArchiveImplementation::save_start(name);
    }
    virtual void save_end(const char * name){
        ArchiveImplementation::save_end(name);
    }
    virtual void end_preamble(){
        ArchiveImplementation::end_preamble();
    }
    virtual void register_basic_serializer(const detail::basic_oserializer & bos){
        ArchiveImplementation::register_basic_serializer(bos);
    }
    virtual helper_collection &
    get_helper_collection(){
        return ArchiveImplementation::get_helper_collection();
    }
public:
    // this can't be inheriteded because they appear in mulitple
    // parents
    typedef mpl::bool_<false> is_loading;
    typedef mpl::bool_<true> is_saving;
    // the << operator
    template<class T>
    polymorphic_oarchive & operator<<(T & t){
        return polymorphic_oarchive::operator<<(t);
    }
    // the & operator
    template<class T>
    polymorphic_oarchive & operator&(T & t){
        return polymorphic_oarchive::operator&(t);
    }
    // register type function
    template<class T>
    const basic_pointer_oserializer * 
    register_type(T * t = NULL){
        return ArchiveImplementation::register_type(t);
    }
    // all current archives take a stream as constructor argument
    template <class _Elem, class _Tr>
    polymorphic_oarchive_route(
        std::basic_ostream<_Elem, _Tr> & os,
        unsigned int flags = 0
    ) :
        ArchiveImplementation(os, flags)
    {}
    virtual ~polymorphic_oarchive_route(){};
};

} // namespace detail
} // namespace archive
} // namespace boost

#ifdef BOOST_MSVC
#pragma warning(pop)
#endif

#include <boost/archive/detail/abi_suffix.hpp> // pops abi_suffix.hpp pragmas

#endif // BOOST_ARCHIVE_DETAIL_POLYMORPHIC_OARCHIVE_DISPATCH_HPP
