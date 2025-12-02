#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/IPv6ToBinary.h>
#include <Common/formatIPv6.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <variant>
#include <charconv>


#include <Common/logger_useful.h>
namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

enum class IPKind
{
    IPv4,
    IPv6,
    String
};

template <IPKind kind>
struct IPTrait
{
};

template <>
struct IPTrait<IPKind::IPv4>
{
    using ColumnType = DB::ColumnIPv4;
    using ElementType = DB::IPv4;
};

template <>
struct IPTrait<IPKind::IPv6>
{
    using ColumnType = DB::ColumnIPv6;
    using ElementType = DB::UInt128;
};

template <>
struct IPTrait<IPKind::String>
{
    using ColumnType = DB::ColumnString;
    using ElementType = std::string_view;
};

class IPAddressVariant
{
public:
    explicit IPAddressVariant(DB::IPv4 addr_): addr(addr_)
    {
    }

    explicit IPAddressVariant(UInt128 addr_)
    {
        addr = IPv6AddrType();
        auto * dst = std::get<IPv6AddrType>(addr).data();
        const char * src = reinterpret_cast<const char *>(&addr_.items);
        memcpy(dst, src, IPV6_BINARY_LENGTH);
    }

    explicit IPAddressVariant(std::string_view addr_)
    {
        UInt32 v4;
        if (DB::parseIPv4whole(addr_.data(), addr_.data() + addr_.size(), reinterpret_cast<unsigned char *>(&v4)))
        {
            addr = v4;
        }
        else
        {
            addr = IPv6AddrType();
            bool success = DB::parseIPv6Whole(addr_.data(), addr_.data() + addr_.size(), std::get<IPv6AddrType>(addr).data());
            if (!success)
                throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Neither IPv4 nor IPv6 address: '{}'", addr_);
        }
    }

    UInt32 asV4() const
    {
        if (const auto * val = std::get_if<IPv4AddrType>(&addr))
            return *val;
        return 0;
    }

    const uint8_t * asV6() const
    {
        if (const auto * val = std::get_if<IPv6AddrType>(&addr))
            return val->data();
        return nullptr;
    }

private:
    using IPv4AddrType = UInt32;
    using IPv6AddrType = std::array<uint8_t, IPV6_BINARY_LENGTH>;

    std::variant<IPv4AddrType, IPv6AddrType> addr;
};

struct IPAddressCIDR
{
    IPAddressVariant address;
    UInt8 prefix;
};

IPAddressCIDR parseIPWithCIDR(std::string_view cidr_str)
{
    size_t pos_slash = cidr_str.find('/');

    if (pos_slash == 0)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Error parsing IP address with prefix: {}", std::string(cidr_str));
    if (pos_slash == std::string_view::npos)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "The text does not contain '/': {}", std::string(cidr_str));

    std::string_view addr_str = cidr_str.substr(0, pos_slash);
    auto addr = IPAddressVariant(addr_str);

    uint8_t prefix = 0;
    auto prefix_str = cidr_str.substr(pos_slash+1);

    const auto * prefix_str_end = prefix_str.data() + prefix_str.size();
    auto [parse_end, parse_error] = std::from_chars(prefix_str.data(), prefix_str_end, prefix); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    uint8_t max_prefix = (addr.asV6() ? IPV6_BINARY_LENGTH : IPV4_BINARY_LENGTH) * 8;
    bool has_error = parse_error != std::errc() || parse_end != prefix_str_end || prefix > max_prefix;
    if (has_error)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "The CIDR has a malformed prefix bits: {}", std::string(cidr_str));

    return {addr, static_cast<UInt8>(prefix)};
}

inline bool isAddressInRange(const IPAddressVariant & address, const IPAddressCIDR & cidr)
{
    if (const auto * cidr_v6 = cidr.address.asV6())
    {
        if (const auto * addr_v6 = address.asV6())
            return DB::matchIPv6Subnet(addr_v6, cidr_v6, cidr.prefix);
    }
    else
    {
        if (!address.asV6())
            return DB::matchIPv4Subnet(address.asV4(), cidr.address.asV4(), cidr.prefix);
    }
    return false;
}

}

namespace DB
{
    class FunctionIsIPAddressContainedIn : public IFunction
    {
    public:
        static constexpr auto name = "isIPAddressInRange";
        String getName() const override { return name; }
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsIPAddressContainedIn>(); }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        template <IPKind kind>
        static inline IPAddressVariant parseIP(const IPTrait<kind>::ColumnType * col_addr, size_t n)
        {
            if constexpr (kind == IPKind::IPv4 || kind == IPKind::IPv6)
            {
                return IPAddressVariant(col_addr->getElement(n));
            }
            return IPAddressVariant(col_addr->getDataAt(n).toView());
        }

    #pragma clang diagnostic ignored "-Wshadow"
    #define EXEC_IMPL(col, func, ...) \
        if (const auto * ipv4_column = dynamic_cast<const ColumnIPv4 *>(&(col))) \
            return func<IPKind::IPv4>(ipv4_column, __VA_ARGS__); \
        else if (const auto * ipv6_column = dynamic_cast<const ColumnIPv6 *>(&(col))) \
            return func<IPKind::IPv6>(ipv6_column, __VA_ARGS__); \
        else if (const auto * string_column = dynamic_cast<const ColumnString *>(&(col))) \
            return func<IPKind::String>(string_column, __VA_ARGS__);

        static std::optional<IPAddressVariant> parseConstantIP(const ColumnConst & col_addr)
        {
            EXEC_IMPL(col_addr.getDataColumn(), parseIP, 0)
            else if (col_addr.onlyNull())
                return std::nullopt;
            else if (const auto * nullable_column = dynamic_cast<const ColumnNullable *>(&col_addr.getDataColumn()))
            {
                EXEC_IMPL(nullable_column->getNestedColumn(), parseIP, 0) ///NO-LINT
            }
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The IP column type must be one of: String, IPv4, IPv6, Nullable(IPv4), Nullable(IPv6), or Nullable(String).");
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* return_type */, size_t input_rows_count) const override
        {
            const IColumn * col_addr = arguments[0].column.get();
            const IColumn * col_cidr = arguments[1].column.get();

            if (const auto * col_addr_const = checkAndGetAnyColumnConst(col_addr))
            {
                if (const auto * col_cidr_const = checkAndGetAnyColumnConst(col_cidr))
                    return executeImpl(*col_addr_const, *col_cidr_const, input_rows_count);
                return executeImpl(*col_addr_const, *col_cidr, input_rows_count);
            }
            if (const auto * col_cidr_const = checkAndGetAnyColumnConst(col_cidr))
                return executeImpl(*col_addr, *col_cidr_const, input_rows_count);
            return executeImpl(*col_addr, *col_cidr, input_rows_count);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 2",
                    getName(), arguments.size());

            const DataTypePtr & addr_type = arguments[0];
            const DataTypePtr & prefix_type = arguments[1];

            WhichDataType type = WhichDataType(addr_type);
            if (const auto * nullable_type = dynamic_cast<const DataTypeNullable *>(&*addr_type))
                type = WhichDataType(nullable_type->getNestedType());

            if (!(type.isString() || type.isIPv4() || type.isIPv6()) || !isString(prefix_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument of function {} must be one of: String, IPv4, IPv6, Nullable(String), Nullable(IPv4), or "
                    "Nullable(IPv6) and the second argument must be String. Type of the first argument: {}, type of the second argument: {}",
                    getName(),
                    addr_type->getName(),
                    prefix_type->getName());

            return std::make_shared<DataTypeUInt8>();
        }

        size_t getNumberOfArguments() const override { return 2; }
        bool useDefaultImplementationForNulls() const override { return false; }

    private:
        /// Like checkAndGetColumnConst() but this function doesn't
        /// care about the type of data column.
        static const ColumnConst * checkAndGetAnyColumnConst(const IColumn * column)
        {
            if (!column || !isColumnConst(*column))
                return nullptr;

            return assert_cast<const ColumnConst *>(column);
        }

        /// Both columns are constant.
        static ColumnPtr executeImpl(
            const ColumnConst & col_addr_const,
            const ColumnConst & col_cidr_const,
            size_t input_rows_count)
        {
            const auto & col_cidr = col_cidr_const.getDataColumn();

            const auto addr = parseConstantIP(col_addr_const);
            const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(0).toView());

            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(1);
            ColumnUInt8::Container & vec_res = col_res->getData();

            vec_res[0] = addr.has_value() && isAddressInRange(*addr, cidr) ? 1 : 0;

            return ColumnConst::create(std::move(col_res), input_rows_count);
        }

        /// Address is constant.
        static ColumnPtr executeImpl(const ColumnConst & col_addr_const, const IColumn & col_cidr, size_t input_rows_count)
        {
            const auto addr = parseConstantIP(col_addr_const);
            if (!addr.has_value())
                return ColumnUInt8::create(input_rows_count, 0);

            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
            ColumnUInt8::Container & vec_res = col_res->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i).toView());
                vec_res[i] = isAddressInRange(*addr, cidr) ? 1 : 0;
            }
            return col_res;
        }

        template <IPKind kind>
        static ColumnPtr executeImpl(const IPTrait<kind>::ColumnType * col_addr, const IPAddressCIDR & cidr, size_t input_rows_count)
        {
            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
            ColumnUInt8::Container & vec_res = col_res->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto addr = parseIP<kind>(col_addr, i);
                vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
            }
            return col_res;
        }

        template <IPKind kind>
        static ColumnPtr executeImpl(
            const IPTrait<kind>::ColumnType * col_addr,
            const ColumnNullable * nullable_column,
            const IPAddressCIDR & cidr,
            size_t input_rows_count)
        {
            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
            ColumnUInt8::Container & vec_res = col_res->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (nullable_column->isNullAt(i))
                    vec_res[i] = 0;
                else
                {
                    const auto addr = parseIP<kind>(col_addr, i);
                    vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                }
            }
            return col_res;
        }

        template <typename T>
        static ColumnPtr executeImpl(const IColumn & col_addr, const T & cidr, size_t input_rows_count)
        {
            EXEC_IMPL(col_addr, executeImpl, cidr, input_rows_count)
            else if (const auto * nullable_column = dynamic_cast<const ColumnNullable *>(&col_addr))
            {
                EXEC_IMPL(nullable_column->getNestedColumn(), executeImpl, nullable_column, cidr, input_rows_count)
            }
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The IP column type must be one of: String, IPv4, IPv6, Nullable(IPv4), Nullable(IPv6), or Nullable(String).");
        }

        /// CIDR is constant.
        static ColumnPtr executeImpl(const IColumn & col_addr, const ColumnConst & col_cidr_const, size_t input_rows_count)
        {
            const auto cidr = parseIPWithCIDR(col_cidr_const.getDataAt(0).toView());
            return executeImpl<IPAddressCIDR>(col_addr, cidr, input_rows_count);
        }

        template <IPKind kind>
        static ColumnPtr executeImpl(const IPTrait<kind>::ColumnType * col_addr, const IColumn & col_cidr, size_t input_rows_count)
        {
            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
            ColumnUInt8::Container & vec_res = col_res->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto addr = parseIP<kind>(col_addr, i);
                const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i).toView());
                vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
            }
            return col_res;
        }

        template <IPKind kind>
        static ColumnPtr executeImpl(
            const IPTrait<kind>::ColumnType * col_addr,
            const ColumnNullable * nullable_column,
            const IColumn & col_cidr,
            size_t input_rows_count)
        {
            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
            ColumnUInt8::Container & vec_res = col_res->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i).toView());
                if (nullable_column->isNullAt(i))
                    vec_res[i] = 0;
                else
                {
                    const auto addr = parseIP<kind>(col_addr, i);
                    vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                }
            }
            return col_res;
        }
    };

    REGISTER_FUNCTION(IsIPAddressContainedIn)
    {
        FunctionDocumentation::Description description = R"(
Determines if an IP address is contained in a network represented in the [Classless Inter-Domain Routing (CIDR)](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) notation.

This function accepts both IPv4 and IPv6 addresses (and networks) represented as strings. It returns `0` if the IP version of the address and the CIDR don't match.
        )";
        FunctionDocumentation::Syntax syntax = "isIPAddressInRange(address, prefix)";
        FunctionDocumentation::Arguments arguments = {
            {"address", "An IPv4 or IPv6 address.", {"String"}},
            {"prefix", "An IPv4 or IPv6 network prefix in CIDR.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the IP version of the address and the CIDR match, otherwise `0`.", {"UInt8"}};
        FunctionDocumentation::Examples examples = {
            {"IPv4 address in range", "SELECT isIPAddressInRange('127.0.0.1', '127.0.0.0/8')", "1"},
            {"IPv4 address not in range", "SELECT isIPAddressInRange('127.0.0.1', 'ffff::/16')", "0"},
            {"IPv6 address not in range", "SELECT isIPAddressInRange('::ffff:192.168.0.1', '::ffff:192.168.0.4/128')", "0"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::IPAddress;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionIsIPAddressContainedIn>(documentation);
    }
}
