#include <cinttypes>
#include <cstring>
#include <ctime>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <Client/BuzzHouse/Generator/ExternalIntegrations.h>
#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>

#include <IO/copyData.h>
#include <Common/ShellCommand.h>

namespace BuzzHouse
{

bool MinIOIntegration::sendRequest(const std::string & resource)
{
    struct tm ttm;
    ssize_t nbytes = 0;
    bool created = false;
    int sock = -1;
    int error = 0;
    char buffer[1024];
    char found_ip[1024];
    const std::time_t time = std::time({});
    DB::WriteBufferFromOwnString sign_cmd;
    DB::WriteBufferFromOwnString sign_out;
    DB::WriteBufferFromOwnString sign_err;
    DB::WriteBufferFromOwnString http_request;
    struct addrinfo hints = {};
    struct addrinfo * result = nullptr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    (void)std::sprintf(buffer, "%" PRIu32 "", sc.port);
    if ((error = getaddrinfo(sc.hostname.c_str(), buffer, &hints, &result)) != 0)
    {
        if (error == EAI_SYSTEM)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            LOG_ERROR(fc.log, "getnameinfo error: {}", buffer);
        }
        else
        {
            LOG_ERROR(fc.log, "getnameinfo error: {}", gai_strerror(error));
        }
        return false;
    }
    /* Loop through results */
    for (const struct addrinfo * p = result; p; p = p->ai_next)
    {
        if ((sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            LOG_ERROR(fc.log, "Could not connect: {}", buffer);
            return false;
        }
        if (connect(sock, p->ai_addr, p->ai_addrlen) == 0)
        {
            if ((error = getnameinfo(p->ai_addr, p->ai_addrlen, found_ip, sizeof(found_ip), nullptr, 0, NI_NUMERICHOST)) != 0)
            {
                if (error == EAI_SYSTEM)
                {
                    strerror_r(errno, buffer, sizeof(buffer));
                    LOG_ERROR(fc.log, "getnameinfo error: {}", buffer);
                }
                else
                {
                    LOG_ERROR(fc.log, "getnameinfo error: {}", gai_strerror(error));
                }
                return false;
            }
            break;
        }
        close(sock);
        sock = -1;
    }
    freeaddrinfo(result);
    if (sock == -1)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        LOG_ERROR(fc.log, "Could not connect: {}", buffer);
        return false;
    }
    (void)gmtime_r(&time, &ttm);
    (void)std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S %z", &ttm);
    sign_cmd << R"(printf "PUT\n\napplication/octet-stream\n)" << buffer << "\\n"
             << resource << "\""
             << " | openssl sha1 -hmac " << sc.password << " -binary | base64";
    auto res = DB::ShellCommand::execute(sign_cmd.str());
    res->in.close();
    copyData(res->out, sign_out);
    copyData(res->err, sign_err);
    res->wait();
    if (!sign_err.str().empty())
    {
        close(sock);
        LOG_ERROR(fc.log, "Error while executing shell command: {}", sign_err.str());
        return false;
    }

    http_request << "PUT " << resource << " HTTP/1.1\n"
                 << "Host: " << found_ip << ":" << std::to_string(sc.port) << "\n"
                 << "Accept: */*\n"
                 << "Date: " << buffer << "\n"
                 << "Content-Type: application/octet-stream\n"
                 << "Authorization: AWS " << sc.user << ":" << sign_out.str() << "Content-Length: 0\n\n\n";

    if (send(sock, http_request.str().c_str(), http_request.str().length(), 0) != static_cast<int>(http_request.str().length()))
    {
        strerror_r(errno, buffer, sizeof(buffer));
        close(sock);
        LOG_ERROR(fc.log, "Error sending request {}: {}", http_request.str(), buffer);
        return false;
    }
    if ((nbytes = read(sock, buffer, sizeof(buffer))) > 0 && nbytes < static_cast<ssize_t>(sizeof(buffer)) && nbytes > 12
        && !(created = (std::strncmp(buffer + 9, "200", 3) == 0)))
    {
        LOG_ERROR(fc.log, "Request {} not successful: {}", http_request.str(), buffer);
    }
    close(sock);
    return created;
}

#if defined USE_MONGODB && USE_MONGODB

template <typename T>
constexpr bool is_document = std::is_same_v<T, bsoncxx::v_noabi::builder::stream::document>;

template <typename T>
void MongoDBIntegration::documentAppendBottomType(RandomGenerator & rg, const std::string & cname, T & output, SQLType * tp)
{
    IntType * itp;
    DateType * dtp;
    DateTimeType * dttp;
    DecimalType * detp;
    StringType * stp;
    EnumType * etp;
    GeoType * gtp;

    if ((itp = dynamic_cast<IntType *>(tp)))
    {
        switch (itp->size)
        {
            case 8:
            case 16:
            case 32: {
                const int32_t val = rg.nextRandomInt32();

                if constexpr (is_document<T>)
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

                if constexpr (is_document<T>)
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
                HugeInt val(rg.nextRandomInt64(), rg.nextRandomUInt64());

                buf.resize(0);
                val.toString(buf);
                if constexpr (is_document<T>)
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
    else if (dynamic_cast<FloatType *>(tp))
    {
        const uint32_t next_option = rg.nextLargeNumber();

        buf.resize(0);
        if (next_option < 25)
        {
            buf += "nan";
        }
        else if (next_option < 49)
        {
            buf += "inf";
        }
        else if (next_option < 73)
        {
            buf += "0.0";
        }
        else
        {
            std::uniform_int_distribution<uint32_t> next_dist(0, 8);
            const uint32_t left = next_dist(rg.generator);
            const uint32_t right = next_dist(rg.generator);

            appendDecimal(rg, buf, left, right);
        }
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((dtp = dynamic_cast<DateType *>(tp)))
    {
        const bsoncxx::types::b_date val(
            {std::chrono::milliseconds(rg.nextBool() ? static_cast<uint64_t>(rg.nextRandomUInt32()) : rg.nextRandomUInt64())});

        if constexpr (is_document<T>)
        {
            output << cname << val;
        }
        else
        {
            output << val;
        }
    }
    else if ((dttp = dynamic_cast<DateTimeType *>(tp)))
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
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((detp = dynamic_cast<DecimalType *>(tp)))
    {
        const uint32_t right = detp->scale.value_or(0);
        const uint32_t left = detp->precision.value_or(10) - right;

        buf.resize(0);
        appendDecimal(rg, buf, left, right);
        if (rg.nextBool())
        {
            bsoncxx::types::b_decimal128 decimal_value(buf.c_str());

            if constexpr (is_document<T>)
            {
                output << cname << decimal_value;
            }
            else
            {
                output << decimal_value;
            }
        }
        else if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((stp = dynamic_cast<StringType *>(tp)))
    {
        const uint32_t limit = stp->precision.value_or(rg.nextRandomUInt32() % 1009);

        if (rg.nextBool())
        {
            for (uint32_t i = 0; i < limit; i++)
            {
                binary_data.push_back(static_cast<char>(rg.nextRandomInt8()));
            }
            bsoncxx::types::b_binary val{
                bsoncxx::binary_sub_type::k_binary, limit, reinterpret_cast<const std::uint8_t *>(binary_data.data())};
            if constexpr (is_document<T>)
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
            if constexpr (is_document<T>)
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

        if constexpr (is_document<T>)
        {
            output << cname << val;
        }
        else
        {
            output << val;
        }
    }
    else if ((etp = dynamic_cast<EnumType *>(tp)))
    {
        const EnumValue & nvalue = rg.pickRandomlyFromVector(etp->values);

        buf.resize(0);
        buf += nvalue.val;
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<UUIDType *>(tp))
    {
        buf.resize(0);
        rg.nextUUID(buf);
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<IPv4Type *>(tp))
    {
        buf.resize(0);
        rg.nextIPv4(buf);
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<IPv6Type *>(tp))
    {
        buf.resize(0);
        rg.nextIPv6(buf);
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if (dynamic_cast<JSONType *>(tp))
    {
        std::uniform_int_distribution<int> dopt(1, 10);
        std::uniform_int_distribution<int> wopt(1, 10);

        buf.resize(0);
        strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator), buf);
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((gtp = dynamic_cast<GeoType *>(tp)))
    {
        buf.resize(0);
        strAppendGeoValue(rg, buf, gtp->geo_type);
        if constexpr (is_document<T>)
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
    RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, ArrayType * at)
{
    std::uniform_int_distribution<uint64_t> nested_rows_dist(0, fc.max_nested_rows);
    const uint64_t limit = nested_rows_dist(rg.generator);
    auto array = document << cname << bsoncxx::builder::stream::open_array; // Array
    SQLType * tp = at->subtype;
    Nullable * nl;
    VariantType * vtp;
    LowCardinality * lc;

    for (uint64_t i = 0; i < limit; i++)
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
            dynamic_cast<IntType *>(tp) || dynamic_cast<FloatType *>(tp) || dynamic_cast<DateType *>(tp) || dynamic_cast<DateTimeType *>(tp)
            || dynamic_cast<DecimalType *>(tp) || dynamic_cast<StringType *>(tp) || dynamic_cast<const BoolType *>(tp)
            || dynamic_cast<EnumType *>(tp) || dynamic_cast<UUIDType *>(tp) || dynamic_cast<IPv4Type *>(tp) || dynamic_cast<IPv6Type *>(tp)
            || dynamic_cast<JSONType *>(tp) || dynamic_cast<GeoType *>(tp))
        {
            documentAppendBottomType<decltype(array)>(rg, "", array, at->subtype);
        }
        else if ((lc = dynamic_cast<LowCardinality *>(tp)))
        {
            if ((nl = dynamic_cast<Nullable *>(lc->subtype)))
            {
                documentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
            }
            else
            {
                documentAppendBottomType<decltype(array)>(rg, "", array, lc->subtype);
            }
        }
        else if ((nl = dynamic_cast<Nullable *>(tp)))
        {
            documentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
        }
        else if (dynamic_cast<ArrayType *>(tp))
        {
            array << bsoncxx::builder::stream::open_array << 1 << bsoncxx::builder::stream::close_array;
        }
        else if ((vtp = dynamic_cast<VariantType *>(tp)))
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
    RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, SQLType * tp)
{
    Nullable * nl;
    ArrayType * at;
    VariantType * vtp;
    LowCardinality * lc;
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
        dynamic_cast<IntType *>(tp) || dynamic_cast<FloatType *>(tp) || dynamic_cast<DateType *>(tp) || dynamic_cast<DateTimeType *>(tp)
        || dynamic_cast<DecimalType *>(tp) || dynamic_cast<StringType *>(tp) || dynamic_cast<const BoolType *>(tp)
        || dynamic_cast<EnumType *>(tp) || dynamic_cast<UUIDType *>(tp) || dynamic_cast<IPv4Type *>(tp) || dynamic_cast<IPv6Type *>(tp)
        || dynamic_cast<JSONType *>(tp) || dynamic_cast<GeoType *>(tp))
    {
        documentAppendBottomType<bsoncxx::v_noabi::builder::stream::document>(rg, cname, document, tp);
    }
    else if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        documentAppendAnyValue(rg, cname, document, lc->subtype);
    }
    else if ((nl = dynamic_cast<Nullable *>(tp)))
    {
        documentAppendAnyValue(rg, cname, document, nl->subtype);
    }
    else if ((at = dynamic_cast<ArrayType *>(tp)))
    {
        documentAppendArray(rg, cname, document, at);
    }
    else if ((vtp = dynamic_cast<VariantType *>(tp)))
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
