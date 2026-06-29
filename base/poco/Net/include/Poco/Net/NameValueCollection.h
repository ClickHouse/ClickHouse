//
// NameValueCollection.h
//
// Library: Net
// Package: Messages
// Module:  NameValueCollection
//
// Definition of the NameValueCollection class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_NameValueCollection_INCLUDED
#define Net_NameValueCollection_INCLUDED


#include <cstddef>
#include <vector>
#include "Poco/ListMap.h"
#include "Poco/Net/Net.h"
#include "Poco/String.h"


namespace Poco
{
namespace Net
{


    class Net_API NameValueCollection
    /// A collection of name-value pairs that are used in
    /// various internet protocols like HTTP and SMTP.
    ///
    /// The name is case-insensitive.
    ///
    /// There can be more than one name-value pair with the
    /// same name.
    {
    public:
        typedef Poco::ListMap<std::string, std::string> HeaderMap;
        typedef HeaderMap::Iterator Iterator;
        typedef HeaderMap::ConstIterator ConstIterator;

        NameValueCollection();
        /// Creates an empty NameValueCollection.

        NameValueCollection(const NameValueCollection & nvc);
        /// Creates a NameValueCollection by copying another one.

        virtual ~NameValueCollection();
        /// Destroys the NameValueCollection.

        NameValueCollection & operator=(const NameValueCollection & nvc);
        /// Assigns the name-value pairs of another NameValueCollection to this one.

        void swap(NameValueCollection & nvc);
        /// Swaps the NameValueCollection with another one.

        const std::string & operator[](const std::string & name) const;
        /// Returns the value of the (first) name-value pair with the given name.
        ///
        /// Throws a NotFoundException if the name-value pair does not exist.

        void set(const std::string & name, const std::string & value);
        /// Sets the value of the (first) name-value pair with the given name.

        void add(const std::string & name, const std::string & value);
        /// Adds a new name-value pair with the given name and value.

        const std::string & get(const std::string & name) const;
        /// Returns the value of the first name-value pair with the given name.
        ///
        /// Throws a NotFoundException if the name-value pair does not exist.

        const std::string & get(const std::string & name, const std::string & defaultValue) const;
        /// Returns the value of the first name-value pair with the given name.
        /// If no value with the given name has been found, the defaultValue is returned.

        std::vector<std::string> getAll(const std::string & name) const;
        /// Returns all values of all name-value pairs with the given name.
        ///
        /// Returns an empty vector if there are no name-value pairs with the given name.

        bool has(const std::string & name) const;
        /// Returns true if there is at least one name-value pair
        /// with the given name.

        ConstIterator find(const std::string & name) const;
        /// Returns an iterator pointing to the first name-value pair
        /// with the given name.

        ConstIterator findLast(const std::string & name) const;
        /// Returns an iterator pointing to the last name-value pair
        /// with the given name.

        ConstIterator begin() const;
        /// Returns an iterator pointing to the begin of
        /// the name-value pair collection.

        ConstIterator end() const;
        /// Returns an iterator pointing to the end of
        /// the name-value pair collection.

        bool empty() const;
        /// Returns true iff the header does not have any content.

        std::size_t size() const;
        /// Returns the number of name-value pairs in the
        /// collection.

        void erase(const std::string & name);
        /// Removes all name-value pairs with the given name.

        void clear();
        /// Removes all name-value pairs and their values.

    private:
        HeaderMap _map;
    };


    //
    // inlines
    //
    inline void swap(NameValueCollection & nvc1, NameValueCollection & nvc2)
    {
        nvc1.swap(nvc2);
    }


}
} // namespace Poco::Net


#endif // Net_NameValueCollection_INCLUDED
