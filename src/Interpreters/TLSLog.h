#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>

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

    explicit TLSLogElement(const String &failure_reason = "");
#if USE_SSL
    explicit TLSLogElement(const Poco::Net::X509Certificate &tls_certificate, const String &user, const String &failure_reason = "");
#endif

    TLSLogElementType type;
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    Strings certificate_subjects;
    std::pair<time_t, time_t> time_range;
    String certificate_serial;
    String certificate_issuer;
    String user;
    String failure_reason;

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
    using SystemLog<TLSLogElement>::SystemLog;
public:
    explicit TLSLog(ContextPtr context_, const SystemLogSettings & settings);
    ~TLSLog() override;
#if USE_SSL
    void logTLSConnection(const Poco::Net::X509Certificate &tls_certificate, const String &user);
    void onInvalidCertificate(const void *, Poco::Net::VerificationErrorArgs & errorCert);
#endif
};

}
