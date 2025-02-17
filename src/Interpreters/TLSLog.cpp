#include <Interpreters/TLSLog.h>

#include <chrono>
#include <memory>
#include <Columns/ColumnArray.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Poco/Delegate.h>
#include <base/scope_guard.h>
#if USE_SSL
#include <Poco/Net/X509Certificate.h>
#include <Poco/Net/SSLManager.h>
#include <Access/Common/SSLCertificateSubjects.h>
#include <Poco/Net/VerificationErrorArgs.h>
#endif

namespace
{
using namespace DB;

void fillColumnArray(const Strings & data, IColumn & column)
{
    auto & array = typeid_cast<ColumnArray &>(column);
    size_t size = 0;
    auto & data_col = array.getData();
    for (const auto & name : data)
    {
        data_col.insertData(name.data(), name.size());
        ++size;
    }
    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + size);
}

}

namespace DB
{

#if USE_SSL
UInt128 X509CertificateHash(const Poco::Net::X509Certificate &cert)
{
    BIO * bio = BIO_new(BIO_s_mem());
    SCOPE_EXIT({ BIO_free(bio); });

    PEM_write_bio_X509(bio, cert.certificate());

    char * cert_data = nullptr;
    auto size = BIO_get_mem_data(bio, &cert_data);
    return sipHash128(cert_data, size);
}
#endif


TLSLogElement::TLSLogElement(const String &failure_reason_, uint64_t failure_count_)
    : type(failure_reason_.empty() ? TLSLogElementType::SESSION_SUCCESS : TLSLogElementType::SESSION_FAILURE)
    , event_time(std::chrono::system_clock::now())
    , failure_reason(failure_reason_)
    , failure_count(failure_count_)
{
}

#if USE_SSL
void TLSLog::onInvalidCertificate(const void *, Poco::Net::VerificationErrorArgs & errorCert)
{
    // Don't put every certificate failure into the log to save disk space and protect from misbehaving clients.
    // Instead, produce a log per certificate hash once in `TLSLogErrorFlushIntervalSeconds` seconds.
    std::unique_lock lock{tls_failure_stats_mutex};
    const UInt128 &cert_hash = X509CertificateHash(errorCert.certificate());
    if (!tls_failure_stats.contains(cert_hash))
        tls_failure_stats[cert_hash] = std::make_unique<CertStats>(errorCert.certificate());

    auto &stats = tls_failure_stats[cert_hash];
    ++stats->count;
    stats->last_error_message = errorCert.errorMessage();
    stats->last_error_time = std::chrono::system_clock::now();
}

void TLSLog::logTLSConnection(const Poco::Net::X509Certificate &tls_certificate, const String &user)
{
    add(TLSLogElement(tls_certificate, user, std::chrono::system_clock::now()));
}

void TLSLog::onFlushNotification()
{
    try
    {
        auto tls_failure_stats_tmp = decltype(tls_failure_stats){};
        {
            std::unique_lock lock{tls_failure_stats_mutex};
            tls_failure_stats_tmp = std::move(tls_failure_stats);
            tls_failure_stats.clear();
        }
        for (auto &cert_stat : tls_failure_stats_tmp)
        {
            auto &stat = cert_stat.second;
            add(TLSLogElement(*stat->certificate, "", stat->last_error_time, stat->last_error_message, stat->count));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

TLSLog::CertStats::CertStats(const Poco::Net::X509Certificate &tls_certificate) :
    certificate(std::make_unique<Poco::Net::X509Certificate>(tls_certificate))
{

}
#endif

TLSLog::TLSLog(ContextPtr context_, const SystemLogSettings & settings) : SystemLog<TLSLogElement>(context_, settings)
{
#if USE_SSL
    Poco::Net::SSLManager::instance().ServerVerificationError += Poco::Delegate<TLSLog, Poco::Net::VerificationErrorArgs>(this, &TLSLog::onInvalidCertificate);
#endif
}

TLSLog::~TLSLog()
{
#if USE_SSL
    Poco::Net::SSLManager::instance().ServerVerificationError -= Poco::Delegate<TLSLog, Poco::Net::VerificationErrorArgs>(this, &TLSLog::onInvalidCertificate);
#endif
}

#if USE_SSL
TLSLogElement::TLSLogElement(const Poco::Net::X509Certificate &tls_certificate, const String &user_, const TimePoint &event_time_, const String &failure_reason_, uint64_t failure_count_)
    : TLSLogElement(failure_reason_, failure_count_)
{
    event_time = event_time_;
    certificate_validity_period = {tls_certificate.validFrom().timestamp().epochTime(), tls_certificate.expiresOn().timestamp().epochTime()};
    using SubjectType = SSLCertificateSubjects::Type;
    SSLCertificateSubjects cert_subjects = extractSSLCertificateSubjects(tls_certificate);
    for (auto subject_type : {SubjectType::CN, SubjectType::SAN})
        for (const auto & subject : cert_subjects.at(subject_type))
            certificate_subjects.push_back(toString(subject_type) + ":" + subject);

    user = user_;
    certificate_serial = tls_certificate.serialNumber();
    certificate_issuer = tls_certificate.issuerName();
}
#endif

NamesAndTypesList TLSLogElement::getNamesAndTypes()
{
    auto event_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"failure",           static_cast<Int8>(TLSLogElementType::SESSION_FAILURE)},
            {"success",           static_cast<Int8>(TLSLogElementType::SESSION_SUCCESS)},
        });

    auto lc_string_datatype = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return
    {
        {"type", std::move(event_type)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"certificate_subjects", std::make_shared<DataTypeArray>(lc_string_datatype)},
        {"certificate_not_before", std::make_shared<DataTypeDateTime>()},
        {"certificate_not_after", std::make_shared<DataTypeDateTime>()},
        {"certificate_serial", lc_string_datatype},
        {"certificate_issuer", lc_string_datatype},
        {"user", lc_string_datatype},
        {"failure_reason", std::make_shared<DataTypeString>()},
        {"failure_count", std::make_shared<DataTypeUInt64>()},
    };
}

ColumnsDescription TLSLogElement::getColumnsDescription()
{
    auto event_type = std::make_shared<DataTypeEnum8>(
    DataTypeEnum8::Values
    {
        {"failure",           static_cast<Int8>(TLSLogElementType::SESSION_FAILURE)},
        {"success",           static_cast<Int8>(TLSLogElementType::SESSION_SUCCESS)},
    });

    auto lc_string_datatype = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"type", std::move(event_type), "Type of event, can either be FAILURE or SUCCESS."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the event."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the event"},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the event in microseconds."},
        {"certificate_subjects", std::make_shared<DataTypeArray>(lc_string_datatype), "The list of subjects in the certificate."},
        {"certificate_not_before", std::make_shared<DataTypeDateTime>(), "Certificate is valid only after this time."},
        {"certificate_not_after", std::make_shared<DataTypeDateTime>(), "Certificate is not valid after this time."},
        {"certificate_serial", lc_string_datatype, "Certificate serial number."},
        {"certificate_issuer", lc_string_datatype, "Certificate issuer."},
        {"user", lc_string_datatype, "Connecting user."},
        {"failure_reason", std::make_shared<DataTypeString>(), "In case type of event is FAILURE, provides the motive of failure."},
        {"failure_count", std::make_shared<DataTypeUInt64>(), "Number of times the error repeated since the last event"},
    };
}


void TLSLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    time_t event_time_seconds = timeInSeconds(event_time);
    Decimal64 event_time_microseconds = timeInMicroseconds(event_time);
    columns[i++]->insert(type);
    columns[i++]->insert(static_cast<DayNum>(DateLUT::instance().toDayNum(event_time_seconds).toUnderType()));
    columns[i++]->insert(event_time_seconds);
    columns[i++]->insert(event_time_microseconds);
    fillColumnArray(certificate_subjects, *columns[i++]);
    columns[i++]->insert(certificate_validity_period.first);
    columns[i++]->insert(certificate_validity_period.second);
    columns[i++]->insert(certificate_serial);
    columns[i++]->insert(certificate_issuer);
    columns[i++]->insert(user);
    columns[i++]->insertData(failure_reason.data(), failure_reason.length());
    columns[i++]->insert(failure_count);
}

}
