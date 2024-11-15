#include "ExternalIntegrations.h"

#include <array>
#include <cctype>
#include <cinttypes>
#include <cstring>
#include <ctime>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "Hugeint.h"
#include "UHugeint.h"

namespace BuzzHouse
{

static bool executeCommand(const char * cmd, std::string & result)
{
    char buffer[1024];
    FILE * pp = popen(cmd, "r");

    if (!pp)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        std::cerr << "popen error: " << buffer << std::endl;
        return false;
    }
    std::unique_ptr<FILE, decltype(&pclose)> pipe(pp, pclose);
    if (!pipe)
    {
        return false;
    }
    while (fgets(buffer, static_cast<int>(sizeof(buffer)), pipe.get()))
    {
        result += buffer;
    }
    return true;
}

bool MinIOIntegration::sendRequest(const std::string & resource)
{
    struct tm ttm;
    std::string sign;
    ssize_t nbytes = 0;
    bool created = false;
    int sock = -1, error = 0;
    char buffer[1024], found_ip[1024];
    const std::time_t time = std::time({});
    DB::WriteBufferFromOwnString http_request, sign_cmd;
    struct addrinfo hints = {}, *result = nullptr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    (void)std::sprintf(buffer, "%" PRIu32 "", sc.port);
    if ((error = getaddrinfo(sc.hostname.c_str(), buffer, &hints, &result)) != 0)
    {
        if (error == EAI_SYSTEM)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            std::cerr << "getaddrinfo: " << buffer << std::endl;
        }
        else
        {
            std::cerr << "getaddrinfo: " << gai_strerror(error) << std::endl;
        }
        return false;
    }
    /* Loop through results */
    for (const struct addrinfo * p = result; p; p = p->ai_next)
    {
        if ((sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            std::cerr << "Could not connect: " << buffer << std::endl;
            return false;
        }
        if (connect(sock, p->ai_addr, p->ai_addrlen) == 0)
        {
            if ((error = getnameinfo(p->ai_addr, p->ai_addrlen, found_ip, sizeof(found_ip), nullptr, 0, NI_NUMERICHOST)) != 0)
            {
                if (error == EAI_SYSTEM)
                {
                    strerror_r(errno, buffer, sizeof(buffer));
                    std::cerr << "getnameinfo: " << buffer << std::endl;
                }
                else
                {
                    std::cerr << "getnameinfo: " << gai_strerror(error) << std::endl;
                }
                return false;
            }
            break;
        }
        error = errno;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(result);
    if (sock == -1)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        std::cerr << "Could not connect: " << buffer << std::endl;
        return false;
    }
    (void)gmtime_r(&time, &ttm);
    (void)std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S %z", &ttm);
    sign_cmd << R"(printf "PUT\n\napplication/octet-stream\n)" << buffer << "\\n"
             << resource << "\""
             << " | openssl sha1 -hmac " << sc.password << " -binary | base64";
    if (!executeCommand(sign_cmd.str().c_str(), sign))
    {
        close(sock);
        return false;
    }

    http_request << "PUT " << resource << " HTTP/1.1\n"
                 << "Host: " << found_ip << ":" << std::to_string(sc.port) << "\n"
                 << "Accept: */*\n"
                 << "Date: " << buffer << "\n"
                 << "Content-Type: application/octet-stream\n"
                 << "Authorization: AWS " << sc.user << ":" << sign << "Content-Length: 0\n\n\n";

    if (send(sock, http_request.str().c_str(), http_request.str().length(), 0) != static_cast<int>(http_request.str().length()))
    {
        strerror_r(errno, buffer, sizeof(buffer));
        close(sock);
        std::cerr << "Error sending request: " << http_request.str() << std::endl << buffer << std::endl;
        return false;
    }
    if ((nbytes = read(sock, buffer, sizeof(buffer))) > 0 && nbytes < static_cast<ssize_t>(sizeof(buffer)) && nbytes > 12
        && !(created = (std::strncmp(buffer + 9, "200", 3) == 0)))
    {
        std::cerr << "Request not successful: " << http_request.str() << std::endl << buffer << std::endl;
    }
    close(sock);
    return created;
}

#ifdef USE_MONGODB
template <typename T>
void MongoDBIntegration::documentAppendBottomType(RandomGenerator & rg, const std::string & cname, T & output, const SQLType * tp)
{
    const IntType * itp;
    const DateType * dtp;
    const DateTimeType * dttp;
    const DecimalType * detp;
    const StringType * stp;
    const EnumType * etp;
    const GeoType * gtp;

    (void)cname;
    if ((itp = dynamic_cast<const IntType *>(tp)))
    {
        switch (itp->size)
        {
            case 8:
            case 16:
            case 32: {
                const int32_t val = rg.nextRandomInt32();

                if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
                {
                    output << cname << val;
                }
                else
                {
                    output << val;
                }
            }
            break;
            case 64: {
                const int64_t val = rg.nextRandomInt64();

                if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
                {
                    output << cname << val;
                }
                else
                {
                    output << val;
                }
            }
            break;
            default: {
                hugeint_t val(rg.nextRandomInt64(), rg.nextRandomUInt64());

                buf.resize(0);
                val.toString(buf);
                if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
                {
                    output << cname << buf;
                }
                else
                {
                    output << buf;
                }
            }
        }
    }
    else if (dynamic_cast<const FloatType *>(tp))
    {
        double value = 0;
        const uint32_t next_option = rg.nextLargeNumber();

        if (next_option < 25)
        {
            value = std::numeric_limits<double>::quiet_NaN();
        }
        else if (next_option < 49)
        {
            value = std::numeric_limits<double>::infinity();
        }
        else if (next_option < 73)
        {
            value = 0.0;
        }
        else
        {
            value = rg.nextRandomDouble();
        }
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << value;
        }
        else
        {
            output << value;
        }
    }
    else if ((dtp = dynamic_cast<const DateType *>(tp)))
    {
        const bsoncxx::types::b_date val(
            {std::chrono::milliseconds(rg.nextBool() ? static_cast<uint64_t>(rg.nextRandomUInt32()) : rg.nextRandomUInt64())});

        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << val;
        }
        else
        {
            output << val;
        }
    }
    else if ((dttp = dynamic_cast<const DateTimeType *>(tp)))
    {
        buf.resize(0);
        if (dttp->extended)
        {
            rg.nextDateTime64(buf);
        }
        else
        {
            rg.nextDateTime(buf);
        }
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((detp = dynamic_cast<const DecimalType *>(tp)))
    {
        const uint32_t right = detp->scale.value_or(0), left = detp->precision.value_or(10) - right;

        buf.resize(0);
        appendDecimal(rg, buf, left, right);
        if (rg.nextBool())
        {
            bsoncxx::types::b_decimal128 decimal_value(buf.c_str());

            if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
            {
                output << cname << decimal_value;
            }
            else
            {
                output << decimal_value;
            }
        }
        else if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((stp = dynamic_cast<const StringType *>(tp)))
    {
        const uint32_t limit = stp->precision.value_or((rg.nextRandomUInt32() % 10000) + 1);

        if (rg.nextBool())
        {
            for (uint32_t i = 0; i < limit; i++)
            {
                binary_data.push_back(static_cast<char>(rg.nextRandomInt8()));
            }
            bsoncxx::types::b_binary val{
                bsoncxx::binary_sub_type::k_binary, limit, reinterpret_cast<const std::uint8_t *>(binary_data.data())};
            if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
            {
                output << cname << val;
            }
            else
            {
                output << val;
            }
            binary_data.clear();
        }
        else
        {
            buf.resize(0);
            rg.nextString(buf, "", true, limit);
            if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
            {
                output << cname << buf;
            }
            else
            {
                output << buf;
            }
        }
    }
    else if (dynamic_cast<const BoolType *>(tp))
    {
        const bool val = rg.nextBool();

        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << val;
        }
        else
        {
            output << val;
        }
    }
    else if ((etp = dynamic_cast<const EnumType *>(tp)))
    {
        const EnumValue & nvalue = rg.pickRandomlyFromVector(etp->values);

        buf.resize(0);
        buf += nvalue.val;
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<const UUIDType *>(tp))
    {
        buf.resize(0);
        rg.nextUUID(buf);
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<const IPv4Type *>(tp))
    {
        buf.resize(0);
        rg.nextIPv4(buf);
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<const IPv6Type *>(tp))
    {
        buf.resize(0);
        rg.nextIPv6(buf);
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<const JSONType *>(tp))
    {
        std::uniform_int_distribution<int> dopt(1, 10), wopt(1, 10);

        buf.resize(0);
        strBuildJSON(rg, dopt(rg.gen), wopt(rg.gen), buf);
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((gtp = dynamic_cast<const GeoType *>(tp)))
    {
        buf.resize(0);
        strAppendGeoValue(rg, buf, gtp->geo_type);
        if constexpr (std::is_same<T, bsoncxx::v_noabi::builder::stream::document>::value)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else
    {
        assert(0);
    }
}

void MongoDBIntegration::documentAppendArray(
    RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, const ArrayType * at)
{
    const uint32_t limit = rg.nextLargeNumber() % 100;
    auto array = document << cname << bsoncxx::builder::stream::open_array; // Array
    const SQLType * tp = at->subtype;
    const Nullable * nl;
    const ArrayType * att;
    const VariantType * vtp;
    const LowCardinality * lc;

    for (uint32_t i = 0; i < limit; i++)
    {
        const uint32_t nopt = rg.nextLargeNumber();

        if (nopt < 31)
        {
            array << bsoncxx::types::b_null{}; // Null Value
        }
        else if (nopt < 41)
        {
            array << bsoncxx::oid{}; // Oid Value
        }
        else if (nopt < 46)
        {
            array << bsoncxx::types::b_maxkey{}; // Max-Key Value
        }
        else if (nopt < 51)
        {
            array << bsoncxx::types::b_minkey{}; // Min-Key Value
        }
        else if (
            dynamic_cast<const IntType *>(tp) || dynamic_cast<const FloatType *>(tp) || dynamic_cast<const DateType *>(tp)
            || dynamic_cast<const DateTimeType *>(tp) || dynamic_cast<const DecimalType *>(tp) || dynamic_cast<const StringType *>(tp)
            || dynamic_cast<const BoolType *>(tp) || dynamic_cast<const EnumType *>(tp) || dynamic_cast<const UUIDType *>(tp)
            || dynamic_cast<const IPv4Type *>(tp) || dynamic_cast<const IPv6Type *>(tp) || dynamic_cast<const JSONType *>(tp)
            || dynamic_cast<const GeoType *>(tp))
        {
            documentAppendBottomType<decltype(array)>(rg, "", array, at->subtype);
        }
        else if ((lc = dynamic_cast<const LowCardinality *>(tp)))
        {
            if ((nl = dynamic_cast<const Nullable *>(lc->subtype)))
            {
                documentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
            }
            else
            {
                documentAppendBottomType<decltype(array)>(rg, "", array, lc->subtype);
            }
        }
        else if ((nl = dynamic_cast<const Nullable *>(tp)))
        {
            documentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
        }
        else if ((att = dynamic_cast<const ArrayType *>(tp)))
        {
            array << bsoncxx::builder::stream::open_array << 1 << bsoncxx::builder::stream::close_array;
        }
        else if ((vtp = dynamic_cast<const VariantType *>(tp)))
        {
            if (vtp->subtypes.empty())
            {
                array << bsoncxx::types::b_null{}; // Null Value
            }
            else
            {
                array << 1;
            }
        }
    }
    array << bsoncxx::builder::stream::close_array;
}

void MongoDBIntegration::documentAppendAnyValue(
    RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, const SQLType * tp)
{
    const Nullable * nl;
    const ArrayType * at;
    const VariantType * vtp;
    const LowCardinality * lc;
    const uint32_t nopt = rg.nextLargeNumber();

    if (nopt < 31)
    {
        document << cname << bsoncxx::types::b_null{}; // Null Value
    }
    else if (nopt < 41)
    {
        document << cname << bsoncxx::oid{}; // Oid Value
    }
    else if (nopt < 46)
    {
        document << cname << bsoncxx::types::b_maxkey{}; // Max-Key Value
    }
    else if (nopt < 51)
    {
        document << cname << bsoncxx::types::b_minkey{}; // Min-Key Value
    }
    else if (
        dynamic_cast<const IntType *>(tp) || dynamic_cast<const FloatType *>(tp) || dynamic_cast<const DateType *>(tp)
        || dynamic_cast<const DateTimeType *>(tp) || dynamic_cast<const DecimalType *>(tp) || dynamic_cast<const StringType *>(tp)
        || dynamic_cast<const BoolType *>(tp) || dynamic_cast<const EnumType *>(tp) || dynamic_cast<const UUIDType *>(tp)
        || dynamic_cast<const IPv4Type *>(tp) || dynamic_cast<const IPv6Type *>(tp) || dynamic_cast<const JSONType *>(tp)
        || dynamic_cast<const GeoType *>(tp))
    {
        documentAppendBottomType<bsoncxx::v_noabi::builder::stream::document>(rg, cname, document, tp);
    }
    else if ((lc = dynamic_cast<const LowCardinality *>(tp)))
    {
        documentAppendAnyValue(rg, cname, document, lc->subtype);
    }
    else if ((nl = dynamic_cast<const Nullable *>(tp)))
    {
        documentAppendAnyValue(rg, cname, document, nl->subtype);
    }
    else if ((at = dynamic_cast<const ArrayType *>(tp)))
    {
        documentAppendArray(rg, cname, document, at);
    }
    else if ((vtp = dynamic_cast<const VariantType *>(tp)))
    {
        if (vtp->subtypes.empty())
        {
            document << cname << bsoncxx::types::b_null{}; // Null Value
        }
        else
        {
            documentAppendAnyValue(rg, cname, document, rg.pickRandomlyFromVector(vtp->subtypes));
        }
    }
    else
    {
        assert(0);
    }
}
#endif

}
