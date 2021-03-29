#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/IPv6ToBinary.h>
#include <Common/formatIPv6.h>
#include <Common/IPv6ToBinary.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <variant>
#include <charconv>


#include <common/logger_useful.h>
namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_PARSE_NUMBER;
}

namespace
{

class IPAddressVariant
{
public:

    explicit IPAddressVariant(const StringRef & address_str)
    {
        // IP address parser functions require that the input is
        // NULL-terminated so we need to copy it.
        const auto address_str_copy = std::string(address_str);

        UInt32 v4;
        if (DB::parseIPv4(address_str_copy.c_str(), reinterpret_cast<unsigned char *>(&v4)))
        {
            addr = v4;
        }
        else
        {
            addr = IPv6AddrType();
            bool success = DB::parseIPv6(address_str_copy.c_str(), std::get<IPv6AddrType>(addr).data());
            if (!success)
                throw DB::Exception("Neither IPv4 nor IPv6 address: '" + address_str_copy + "'",
                                    DB::ErrorCodes::CANNOT_PARSE_TEXT);
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

IPAddressCIDR parseIPWithCIDR(const StringRef cidr_str)
{
    std::string_view cidr_str_view(cidr_str);
    size_t pos_slash = cidr_str_view.find('/');

    if (pos_slash == 0)
        throw DB::Exception("Error parsing IP address with prefix: " + std::string(cidr_str), DB::ErrorCodes::CANNOT_PARSE_TEXT);
    if (pos_slash == std::string_view::npos)
        throw DB::Exception("The text does not contain '/': " + std::string(cidr_str), DB::ErrorCodes::CANNOT_PARSE_TEXT);

    std::string_view addr_str = cidr_str_view.substr(0, pos_slash);
    IPAddressVariant addr(StringRef{addr_str.data(), addr_str.size()});

    UInt8 prefix = 0;
    auto prefix_str = cidr_str_view.substr(pos_slash+1);

    const auto * prefix_str_end = prefix_str.data() + prefix_str.size();
    auto [parse_end, parse_error] = std::from_chars(prefix_str.data(), prefix_str_end, prefix);
    UInt8 max_prefix = (addr.asV6() ? IPV6_BINARY_LENGTH : IPV4_BINARY_LENGTH) * 8;
    bool has_error = parse_error != std::errc() || parse_end != prefix_str_end || prefix > max_prefix;
    if (has_error)
        throw DB::Exception("The CIDR has a malformed prefix bits: " + std::string(cidr_str), DB::ErrorCodes::CANNOT_PARSE_TEXT);

    return {addr, prefix};
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
    template <typename Name>
    class ExecutableFunctionIsIPAddressContainedIn : public IExecutableFunctionImpl
    {
    public:
        String getName() const override
        {
            return Name::name;
        }

        ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
        {
            const IColumn * col_addr = arguments[0].column.get();
            const IColumn * col_cidr = arguments[1].column.get();

            if (const auto * col_addr_const = checkAndGetAnyColumnConst(col_addr))
            {
                // col_addr_const is constant and is either String or Nullable(String).
                // We don't care which one it exactly is.
                if (const auto * col_cidr_const = checkAndGetAnyColumnConst(col_cidr))
                    return executeImpl(*col_addr_const, *col_cidr_const, return_type, input_rows_count);
                else
                    return executeImpl(*col_addr_const, *col_cidr, return_type, input_rows_count);
            }
            else
            {
                if (const auto * col_cidr_const = checkAndGetAnyColumnConst(col_cidr))
                    return executeImpl(*col_addr, *col_cidr_const, return_type, input_rows_count);
                else
                    return executeImpl(*col_addr, *col_cidr, input_rows_count);
            }
        }

        bool useDefaultImplementationForNulls() const override
        {
            // We can't use the default implementation because that would end up
            // parsing invalid addresses or prefixes at NULL fields, which would
            // throw exceptions instead of returning NULL.
            return false;
        }

    private:
        // Like checkAndGetColumnConst() but this function doesn't
        // care about the type of data column.
        static const ColumnConst * checkAndGetAnyColumnConst(const IColumn * column)
        {
            if (!column || !isColumnConst(*column))
                return nullptr;

            return assert_cast<const ColumnConst *>(column);
        }

        // Both columns are constant.
        ColumnPtr executeImpl(const ColumnConst & col_addr_const, const ColumnConst & col_cidr_const, const DataTypePtr & return_type, size_t input_rows_count) const
        {
            const auto & col_addr = col_addr_const.getDataColumn();
            const auto & col_cidr = col_cidr_const.getDataColumn();

            if (col_addr.isNullAt(0) || col_cidr.isNullAt(0))
            {
                // If either of the arguments are NULL, the result is also NULL.
                assert(return_type->isNullable());
                return return_type->createColumnConstWithDefaultValue(input_rows_count);
            }
            else
            {
                const auto addr = IPAddressVariant(col_addr.getDataAt(0));
                const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(0));

                ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(1);
                ColumnUInt8::Container & vec_res = col_res->getData();

                vec_res[0] = isAddressInRange(addr, cidr) ? 1 : 0;

                if (return_type->isNullable())
                {
                    ColumnUInt8::MutablePtr col_null_map_res = ColumnUInt8::create(1);
                    ColumnUInt8::Container & vec_null_map_res = col_null_map_res->getData();

                    vec_null_map_res[0] = false;

                    return ColumnConst::create(ColumnNullable::create(std::move(col_res), std::move(col_null_map_res)), input_rows_count);
                }
                else
                {
                    return ColumnConst::create(std::move(col_res), input_rows_count);
                }
            }
        }

        // Address is constant.
        ColumnPtr executeImpl(const ColumnConst & col_addr_const, const IColumn & col_cidr, const DataTypePtr & return_type, size_t input_rows_count) const
        {
            const auto & col_addr = col_addr_const.getDataColumn();

            if (col_addr.isNullAt(0))
            {
                // It's constant NULL so the result is also constant NULL.
                assert(return_type->isNullable());
                return return_type->createColumnConstWithDefaultValue(input_rows_count);
            }
            else
            {
                const auto addr = IPAddressVariant(col_addr.getDataAt   (0));

                ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
                ColumnUInt8::Container & vec_res = col_res->getData();

                if (col_addr.isNullable() || col_cidr.isNullable())
                {
                    ColumnUInt8::MutablePtr col_null_map_res = ColumnUInt8::create(input_rows_count);
                    ColumnUInt8::Container & vec_null_map_res = col_null_map_res->getData();

                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        if (col_cidr.isNullAt(i))
                        {
                            vec_null_map_res[i] = true;
                        }
                        else
                        {
                            const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i));
                            vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                            vec_null_map_res[i] = false;
                        }
                    }

                    return ColumnNullable::create(std::move(col_res), std::move(col_null_map_res));
                }
                else
                {
                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i));
                        vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                    }

                    return col_res;
                }
            }
        }

        // CIDR is constant.
        ColumnPtr executeImpl(const IColumn & col_addr, const ColumnConst & col_cidr_const, const DataTypePtr & return_type, size_t input_rows_count) const
        {
            const auto & col_cidr = col_cidr_const.getDataColumn();

            if (col_cidr.isNullAt(0))
            {
                // It's constant NULL so the result is also constant NULL.
                assert(return_type->isNullable());
                return return_type->createColumnConstWithDefaultValue(input_rows_count);
            }
            else
            {
                const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(0));

                ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
                ColumnUInt8::Container & vec_res = col_res->getData();

                if (col_addr.isNullable() || col_cidr.isNullable())
                {
                    ColumnUInt8::MutablePtr col_null_map_res = ColumnUInt8::create(input_rows_count);
                    ColumnUInt8::Container & vec_null_map_res = col_null_map_res->getData();

                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        if (col_addr.isNullAt(i))
                        {
                            vec_null_map_res[i] = true;
                        }
                        else
                        {
                            const auto addr = IPAddressVariant(col_addr.getDataAt(i));
                            vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                            vec_null_map_res[i] = false;
                        }
                    }

                    return ColumnNullable::create(std::move(col_res), std::move(col_null_map_res));
                }
                else
                {
                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        const auto addr = IPAddressVariant(col_addr.getDataAt(i));
                        vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                    }

                    return col_res;
                }
            }
        }

        // Neither are constant.
        ColumnPtr executeImpl(const IColumn & col_addr, const IColumn & col_cidr, size_t input_rows_count) const
        {
            ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(input_rows_count);
            ColumnUInt8::Container & vec_res = col_res->getData();

            if (col_addr.isNullable() || col_cidr.isNullable())
            {
                ColumnUInt8::MutablePtr col_null_map_res = ColumnUInt8::create(input_rows_count);
                ColumnUInt8::Container & vec_null_map_res = col_null_map_res->getData();

                for (size_t i = 0; i < input_rows_count; i++)
                {
                    if (col_addr.isNullAt(i) || col_cidr.isNullAt(i))
                    {
                        vec_null_map_res[i] = true;
                    }
                    else
                    {
                        const auto addr = IPAddressVariant(col_addr.getDataAt(i));
                        const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i));

                        vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                        vec_null_map_res[i] = false;
                    }
                }

                return ColumnNullable::create(std::move(col_res), std::move(col_null_map_res));
            }
            else
            {
                for (size_t i = 0; i < input_rows_count; i++)
                {
                    const auto addr = IPAddressVariant(col_addr.getDataAt(i));
                    const auto cidr = parseIPWithCIDR(col_cidr.getDataAt(i));

                    vec_res[i] = isAddressInRange(addr, cidr) ? 1 : 0;
                }

                return col_res;
            }
        }
    };

    template <typename Name>
    class FunctionBaseIsIPAddressContainedIn : public IFunctionBaseImpl
    {
    public:
        explicit FunctionBaseIsIPAddressContainedIn(DataTypes argument_types_, DataTypePtr return_type_)
            : argument_types(std::move(argument_types_))
            , return_type(std::move(return_type_)) {}

        String getName() const override
        {
            return Name::name;
        }

        const DataTypes & getArgumentTypes() const override
        {
            return argument_types;
        }

        const DataTypePtr & getResultType() const override
        {
            return return_type;
        }

        ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
        {
            return std::make_unique<ExecutableFunctionIsIPAddressContainedIn<Name>>();
        }

    private:
        DataTypes argument_types;
        DataTypePtr return_type;
    };

    template <typename Name>
    class IsIPAddressContainedInOverloadResolver : public IFunctionOverloadResolverImpl
    {
    public:
        static constexpr auto name = Name::name;

        static FunctionOverloadResolverImplPtr create(const Context &)
        {
            return std::make_unique<IsIPAddressContainedInOverloadResolver<Name>>();
        }

        String getName() const override
        {
            return Name::name;
        }

        FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            const DataTypePtr & addr_type   = removeNullable(arguments[0].type);
            const DataTypePtr & prefix_type = removeNullable(arguments[1].type);
            DataTypes argument_types = { addr_type, prefix_type };

            /* The arguments can be any of Nullable(NULL), Nullable(String), and
             * String. We can't do this check in getReturnType() because it
             * won't be called when there are any constant NULLs in the
             * arguments. */
            if (!(WhichDataType(addr_type).isNothing() || isString(addr_type)) ||
                !(WhichDataType(prefix_type).isNothing() || isString(prefix_type)))
                throw Exception("The arguments of function " + getName() + " must be String",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return std::make_unique<FunctionBaseIsIPAddressContainedIn<Name>>(argument_types, return_type);
        }

        DataTypePtr getReturnType(const DataTypes &) const override
        {
            return std::make_shared<DataTypeUInt8>();
        }

        size_t getNumberOfArguments() const override
        {
            return 2;
        }
    };

    struct NameIsIPAddressContainedIn
    {
        static constexpr auto name = "isIPAddressContainedIn";
    };

    void registerFunctionIsIPAddressContainedIn(FunctionFactory & factory)
    {
        factory.registerFunction<IsIPAddressContainedInOverloadResolver<NameIsIPAddressContainedIn>>();
    }
}
