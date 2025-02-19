//
// UUID.h
//
// Library: Foundation
// Package: UUID
// Module:  UUID
//
// Definition of the UUID class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UUID_INCLUDED
#define Foundation_UUID_INCLUDED


#include "Poco/Foundation.h"
#include <Poco/Types.h>


namespace Poco
{


class Foundation_API UUID
/// A UUID is an identifier that is unique across both space and time,
/// with respect to the space of all UUIDs. Since a UUID is a fixed
/// size and contains a time field, it is possible for values to
/// rollover (around A.D. 3400, depending on the specific algorithm
/// used). A UUID can be used for multiple purposes, from tagging
/// objects with an extremely short lifetime, to reliably identifying
/// very persistent objects across a network.
///
/// This class implements a Universal Unique Identifier,
/// as specified in Appendix A of the DCE 1.1 Remote Procedure
/// Call Specification (http://www.opengroup.org/onlinepubs/9629399/),
/// RFC 2518 (WebDAV), section 6.4.1 and the UUIDs and GUIDs internet
/// draft by Leach/Salz from February, 1998
/// (http://www.ics.uci.edu/~ejw/authoring/uuid-guid/draft-leach-uuids-guids-01.txt)
/// and also http://tools.ietf.org/html/draft-mealling-uuid-urn-05
{
public:
    enum Version
    {
        UUID_TIME_BASED = 0x01,
        UUID_DCE_UID = 0x02,
        UUID_NAME_BASED = 0x03,
        UUID_RANDOM = 0x04,
        UUID_NAME_BASED_SHA1 = 0x05

    };

    UUID();
    /// Creates a nil (all zero) UUID.

    UUID(const UUID & uuid);
    /// Copy constructor.

    explicit UUID(const std::string & uuid);
    /// Parses the UUID from a string.

    explicit UUID(const char * uuid);
    /// Parses the UUID from a string.

    ~UUID();
    /// Destroys the UUID.

    UUID & operator=(const UUID & uuid);
    /// Assignment operator.

    void swap(UUID & uuid);
    /// Swaps the UUID with another one.

    void parse(const std::string & uuid);
    /// Parses the UUID from its string representation.

    bool tryParse(const std::string & uuid);
    /// Tries to interpret the given string as an UUID.
    /// If the UUID is syntactically valid, assigns the
    /// members and returns true. Otherwise leaves the
    /// object unchanged and returns false.

    std::string toString() const;
    /// Returns a string representation of the UUID consisting
    /// of groups of hexadecimal digits separated by hyphens.

    void copyFrom(const char * buffer);
    /// Copies the UUID (16 bytes) from a buffer or byte array.
    /// The UUID fields are expected to be
    /// stored in network byte order.
    /// The buffer need not be aligned.

    void copyTo(char * buffer) const;
    /// Copies the UUID to the buffer. The fields
    /// are in network byte order.
    /// The buffer need not be aligned.
    /// There must have room for at least 16 bytes.

    Version version() const;
    /// Returns the version of the UUID.

    int variant() const;
    /// Returns the variant number of the UUID:
    ///   - 0 reserved for NCS backward compatibility
    ///   - 2 the Leach-Salz variant (used by this class)
    ///   - 6 reserved, Microsoft Corporation backward compatibility
    ///   - 7 reserved for future definition

    bool operator==(const UUID & uuid) const;
    bool operator!=(const UUID & uuid) const;
    bool operator<(const UUID & uuid) const;
    bool operator<=(const UUID & uuid) const;
    bool operator>(const UUID & uuid) const;
    bool operator>=(const UUID & uuid) const;

    bool isNull() const;
    /// Returns true iff the UUID is nil (in other words,
    /// consists of all zeros).

    static const UUID & null();
    /// Returns a null/nil UUID.

    static const UUID & dns();
    /// Returns the namespace identifier for the DNS namespace.

    static const UUID & uri();
    /// Returns the namespace identifier for the URI (former URL) namespace.

    static const UUID & oid();
    /// Returns the namespace identifier for the OID namespace.

    static const UUID & x500();
    /// Returns the namespace identifier for the X500 namespace.

    UInt32 getTimeLow() const { return _timeLow; }
    UInt16 getTimeMid() const { return _timeMid; }
    UInt16 getTimeHiAndVersion() const { return _timeHiAndVersion; }
    UInt16 getClockSeq() const { return _clockSeq; }
    std::array<UInt8, 6> getNode() const { return std::array<UInt8, 6>{_node[0], _node[1], _node[2], _node[3], _node[4], _node[5]}; }

protected:
    UUID(UInt32 timeLow, UInt32 timeMid, UInt32 timeHiAndVersion, UInt16 clockSeq, UInt8 node[]);
    UUID(const char * bytes, Version version);
    int compare(const UUID & uuid) const;
    static void appendHex(std::string & str, UInt8 n);
    static void appendHex(std::string & str, UInt16 n);
    static void appendHex(std::string & str, UInt32 n);
    static Int16 nibble(char hex);
    void fromNetwork();
    void toNetwork();

private:
    UInt32 _timeLow;
    UInt16 _timeMid;
    UInt16 _timeHiAndVersion;
    UInt16 _clockSeq;
    UInt8 _node[6];

    friend class UUIDGenerator;
};


//
// inlines
//
inline bool UUID::operator==(const UUID & uuid) const
{
    return compare(uuid) == 0;
}


inline bool UUID::operator!=(const UUID & uuid) const
{
    return compare(uuid) != 0;
}


inline bool UUID::operator<(const UUID & uuid) const
{
    return compare(uuid) < 0;
}


inline bool UUID::operator<=(const UUID & uuid) const
{
    return compare(uuid) <= 0;
}


inline bool UUID::operator>(const UUID & uuid) const
{
    return compare(uuid) > 0;
}


inline bool UUID::operator>=(const UUID & uuid) const
{
    return compare(uuid) >= 0;
}


inline UUID::Version UUID::version() const
{
    return Version(_timeHiAndVersion >> 12);
}


inline bool UUID::isNull() const
{
    return compare(null()) == 0;
}


inline void swap(UUID & u1, UUID & u2)
{
    u1.swap(u2);
}


} // namespace Poco


#endif // Foundation_UUID_INCLUDED
