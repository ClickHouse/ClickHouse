#pragma once

#include "config.h"
#include <base/types.h>
#include <boost/container/flat_set.hpp>

#if USE_SSL
#    include <Poco/Net/X509Certificate.h>
#endif

namespace DB
{
class SSLCertificateSubjects
{
public:
    using container = boost::container::flat_set<String>;
    enum class Type
    {
        CN,
        SAN
    };

private:
    std::array<container, size_t(Type::SAN) + 1> subjects;

public:
    inline const container & at(Type type_) const { return subjects[static_cast<size_t>(type_)]; }
    inline bool empty()
    {
        for (auto & subject_list : subjects)
        {
            if (!subject_list.empty())
                return false;
        }
        return true;
    }
    void insert(const String & subject_type_, String && subject);
    void insert(Type type_, String && subject);
    friend bool operator==(const SSLCertificateSubjects & lhs, const SSLCertificateSubjects & rhs);
};

String toString(SSLCertificateSubjects::Type type_);
SSLCertificateSubjects::Type parseSSLCertificateSubjectType(const String & type_);

#if USE_SSL
SSLCertificateSubjects extractSSLCertificateSubjects(const Poco::Net::X509Certificate & certificate);
#endif
}
