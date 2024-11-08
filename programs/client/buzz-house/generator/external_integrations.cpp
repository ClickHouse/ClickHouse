#include "external_integrations.h"

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
#include <sys/types.h>

#include "hugeint.h"
#include "uhugeint.h"

namespace buzzhouse
{

static bool ExecuteCommand(const char * cmd, std::string & result)
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

bool MinIOIntegration::SendRequest(const std::string & resource)
{
    struct tm ttm;
    std::string sign;
    ssize_t nbytes = 0;
    bool created = false;
    int sock = -1, error = 0;
    char buffer[1024], found_ip[1024];
    const std::time_t time = std::time({});
    std::stringstream http_request, sign_cmd;
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
    if (!ExecuteCommand(sign_cmd.str().c_str(), sign))
    {
        close(sock);
        return false;
    }

    http_request << "PUT " << resource << " HTTP/1.1" << std::endl
                 << "Host: " << found_ip << ":" << std::to_string(sc.port) << std::endl
                 << "Accept: */*" << std::endl
                 << "Date: " << buffer << std::endl
                 << "Content-Type: application/octet-stream" << std::endl
                 << "Authorization: AWS " << sc.user << ":" << sign << "Content-Length: 0" << std::endl
                 << std::endl
                 << std::endl;

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

template <typename T>
void MongoDBIntegration::DocumentAppendBottomType(RandomGenerator & rg, const std::string & cname, T & output, const SQLType * tp)
{
    const IntType * itp;
    const DateType * dtp;
    const DateTimeType * dttp;
    const DecimalType * detp;
    const StringType * stp;
    const EnumType * etp;

    (void)cname;
    if ((itp = dynamic_cast<const IntType *>(tp)))
    {
        switch (itp->size)
        {
            case 8:
            case 16:
            case 32: {
                const int32_t val = rg.NextRandomInt32();

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
                const int64_t val = rg.NextRandomInt64();

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
                hugeint_t val(rg.NextRandomInt64(), rg.NextRandomUInt64());

                buf.resize(0);
                val.ToString(buf);
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
        const uint32_t next_option = rg.NextLargeNumber();

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
            value = rg.NextRandomDouble();
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
            {std::chrono::milliseconds(rg.NextBool() ? static_cast<uint64_t>(rg.NextRandomUInt32()) : rg.NextRandomUInt64())});

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
        if (dtp->extended)
        {
            rg.NextDateTime64(buf);
        }
        else
        {
            rg.NextDateTime(buf);
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
        AppendDecimal(rg, buf, left, right);
        if (rg.NextBool())
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
        const uint32_t limit = stp->precision.value_or((rg.NextRandomUInt32() % 10000) + 1);

        if (rg.NextBool())
        {
            for (uint32_t i = 0; i < limit; i++)
            {
                binary_data.push_back(static_cast<char>(rg.NextRandomInt8()));
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
            rg.NextString(buf, "", true, limit);
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
        const bool val = rg.NextBool();

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
        const EnumValue & nvalue = rg.PickRandomlyFromVector(etp->values);

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
        rg.NextUUID(buf);
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
        rg.NextIPv4(buf);
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
        rg.NextIPv6(buf);
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
        StrBuildJSON(rg, dopt(rg.gen), wopt(rg.gen), buf);
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

void MongoDBIntegration::DocumentAppendArray(
    RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, const ArrayType * at)
{
    const uint32_t limit = rg.NextLargeNumber() % 100;
    auto array = document << cname << bsoncxx::builder::stream::open_array; // Array
    const SQLType * tp = at->subtype;
    const Nullable * nl;
    const ArrayType * att;
    const VariantType * vtp;
    const LowCardinality * lc;

    for (uint32_t i = 0; i < limit; i++)
    {
        const uint32_t nopt = rg.NextLargeNumber();

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
            || dynamic_cast<const IPv4Type *>(tp) || dynamic_cast<const IPv6Type *>(tp) || dynamic_cast<const JSONType *>(tp))
        {
            DocumentAppendBottomType<decltype(array)>(rg, "", array, at->subtype);
        }
        else if ((lc = dynamic_cast<const LowCardinality *>(tp)))
        {
            if ((nl = dynamic_cast<const Nullable *>(lc->subtype)))
            {
                DocumentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
            }
            else
            {
                DocumentAppendBottomType<decltype(array)>(rg, "", array, lc->subtype);
            }
        }
        else if ((nl = dynamic_cast<const Nullable *>(tp)))
        {
            DocumentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
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

void MongoDBIntegration::DocumentAppendAnyValue(
    RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, const SQLType * tp)
{
    const Nullable * nl;
    const ArrayType * at;
    const VariantType * vtp;
    const LowCardinality * lc;
    const uint32_t nopt = rg.NextLargeNumber();

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
        || dynamic_cast<const IPv4Type *>(tp) || dynamic_cast<const IPv6Type *>(tp) || dynamic_cast<const JSONType *>(tp))
    {
        DocumentAppendBottomType<bsoncxx::v_noabi::builder::stream::document>(rg, cname, document, tp);
    }
    else if ((lc = dynamic_cast<const LowCardinality *>(tp)))
    {
        DocumentAppendAnyValue(rg, cname, document, lc->subtype);
    }
    else if ((nl = dynamic_cast<const Nullable *>(tp)))
    {
        DocumentAppendAnyValue(rg, cname, document, nl->subtype);
    }
    else if ((at = dynamic_cast<const ArrayType *>(tp)))
    {
        DocumentAppendArray(rg, cname, document, at);
    }
    else if ((vtp = dynamic_cast<const VariantType *>(tp)))
    {
        if (vtp->subtypes.empty())
        {
            document << cname << bsoncxx::types::b_null{}; // Null Value
        }
        else
        {
            DocumentAppendAnyValue(rg, cname, document, rg.PickRandomlyFromVector(vtp->subtypes));
        }
    }
    else
    {
        assert(0);
    }
}

}
