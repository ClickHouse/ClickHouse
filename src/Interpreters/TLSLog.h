#pragma once

#include <atomic>
#include <chrono>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>
#include "base/extended_types.h"

namespace Poco
{
namespace Net
{
class Socket;
class X509Certificate;
class VerificationErrorArgs;
}
}

namespace DB
{


constexpr int64_t TLSLogErrorFlushIntervalSeconds = 10;
enum class TLSLogElementType : int8_t
{
    SESSION_FAILURE = 0,
    SESSION_SUCCESS = 1,
};

/**
A struct which will be inserted as row into tls_log table.
*/
struct TLSLogElement
{
    using Type = TLSLogElementType;
    using TimePoint = std::chrono::system_clock::time_point;

    explicit TLSLogElement(const String &failure_reason = "", uint64_t failure_count = 0);
#if USE_SSL
    explicit TLSLogElement(const Poco::Net::X509Certificate &tls_certificate, const String &user, const TimePoint &event_time, const String &failure_reason = "", uint64_t failure_count = 0);
#endif

    TLSLogElementType type;
    // time_t event_time{};
    // Decimal64 event_time_microseconds{};
    TimePoint event_time;
    Strings certificate_subjects;
    std::pair<time_t, time_t> certificate_validity_period;
    String certificate_serial;
    String certificate_issuer;
    String user;
    String failure_reason;
    uint64_t failure_count = 0;

    static std::string name() { return "TLSLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    static ColumnsDescription getColumnsDescription();
    static const char * getCustomColumnList() { return nullptr; }

    void appendToBlock(MutableColumns & columns) const;
};

/** TLSLog contains information about client certificates received
by ClickHouse server during TLS handshakes.*/
class TLSLog : public SystemLog<TLSLogElement>
{
    struct CertStats
    {
        explicit CertStats(const Poco::Net::X509Certificate &);
        std::unique_ptr<Poco::Net::X509Certificate> certificate;
        int64_t count = 0;
        String last_error_message;
        TLSLogElement::TimePoint last_error_time;
    };

    using SystemLog<TLSLogElement>::SystemLog;
public:
    explicit TLSLog(ContextPtr context_, const SystemLogSettings & settings);
    ~TLSLog() override;
#if USE_SSL
    void logTLSConnection(const Poco::Net::X509Certificate &tls_certificate, const String &user);
    void onInvalidCertificate(const void *, Poco::Net::VerificationErrorArgs & errorCert);
private:
    void onFlushNotification() override;
    std::unordered_map<UInt128, std::unique_ptr<CertStats>> tls_failure_stats;
    std::mutex tls_failure_stats_mutex;
#endif
};

}
