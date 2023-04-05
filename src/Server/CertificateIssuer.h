#pragma once

#include "config.h"

#if USE_SSL

#include <atomic>

#include <Poco/Logger.h>

namespace DB
{

class CertificateIssuer
{
public:
    /// Singleton class for issuing let's enrypt certificates
    CertificateIssuer(CertificateIssuer const &) = delete;
    void operator=(CertificateIssuer const &) = delete;
    static CertificateIssuer & instance()
    {
        static CertificateIssuer instance;
        return instance;
    }

    void UpdateCertificates();

private:
    CertificateIssuer() = default;

    Poco::Logger * log = &Poco::Logger::get("CertificateIssuer");

    std::atomic<bool> is_update_started;
};

}

#endif
