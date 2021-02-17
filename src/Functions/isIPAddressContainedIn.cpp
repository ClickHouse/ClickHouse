#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/IPv6ToBinary.h>
#include <Common/formatIPv6.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <variant>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PARSE_TEXT;
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }
}

namespace ipaddr
{
    class Address
    {
    public:
        Address() = delete;

        explicit Address(const StringRef & in)
        {
            // IP address parser functions require that the input is
            // NULL-terminated so we need to copy it.
            const auto in_copy = std::string(in);

            UInt32 v4;
            if (DB::parseIPv4(in_copy.c_str(), reinterpret_cast<unsigned char *>(&v4)))
            {
                addr = V4(v4);
            }
            else
            {
                V6 v6;
                if (DB::parseIPv6(in_copy.c_str(), v6.addr.data()))
                    addr = std::move(v6);
                else
                    throw DB::Exception("Neither IPv4 nor IPv6 address: " + in_copy,
                                        DB::ErrorCodes::CANNOT_PARSE_TEXT);
            }
        }

        template <typename ConcreteType, size_t numOctets>
        struct IPVersionBase
        {
            IPVersionBase()
                : addr {} {}

            explicit IPVersionBase(const std::array<uint8_t, numOctets> & octets)
                : addr(octets) {}

            constexpr size_t numBits() const
            {
                return numOctets * 8;
            }

            uint8_t operator[] (size_t i) const
            {
                assert(i >= 0 && i < numOctets);
                return addr[i];
            }

            uint8_t & operator[] (size_t i)
            {
                assert(i >= 0 && i < numOctets);
                return addr[i];
            }

            bool operator<= (const ConcreteType & rhs) const
            {
                for (size_t i = 0; i < numOctets; i++)
                {
                    if ((*this)[i] < rhs[i]) return true;
                    if ((*this)[i] > rhs[i]) return false;
                }
                return true;
            }

            bool operator>= (const ConcreteType & rhs) const
            {
                for (size_t i = 0; i < numOctets; i++)
                {
                    if ((*this)[i] > rhs[i]) return true;
                    if ((*this)[i] < rhs[i]) return false;
                }
                return true;
            }

            ConcreteType operator& (const ConcreteType & rhs) const
            {
                ConcreteType lhs(addr);

                for (size_t i = 0; i < numOctets; i++)
                    lhs[i] &= rhs[i];

                return lhs;
            }

            ConcreteType operator| (const ConcreteType & rhs) const
            {
                ConcreteType lhs(addr);

                for (size_t i = 0; i < numOctets; i++)
                    lhs[i] |= rhs[i];

                return lhs;
            }

            ConcreteType operator~ () const
            {
                ConcreteType tmp(addr);

                for (size_t i = 0; i < numOctets; i++)
                    tmp[i] = ~tmp[i];

                return tmp;
            }

        private:
            // Big-endian
            std::array<uint8_t, numOctets> addr;
            friend class Address;
        };

        struct V4 : public IPVersionBase<V4, 4>
        {
            V4() = default;

            explicit V4(UInt32 addr_)
            {
                addr[0] = (addr_ >> 24) & 0xFF;
                addr[1] = (addr_ >> 16) & 0xFF;
                addr[2] = (addr_ >>  8) & 0xFF;
                addr[3] =  addr_        & 0xFF;
            }

            explicit V4(const std::array<uint8_t, 4> & components)
                : IPVersionBase(components) {}
        };

        struct V6 : public IPVersionBase<V6, 16>
        {
            V6() = default;

            explicit V6(const std::array<uint8_t, 16> & components)
                : IPVersionBase(components) {}
        };

        constexpr const std::variant<V4, V6> & variant() const
        {
            return addr;
        }

    private:
        std::variant<V4, V6> addr;
    };

    class CIDR
    {
    public:
        CIDR() = delete;

        explicit CIDR(const StringRef & in)
        {
            const auto in_view = std::string_view(in);
            const auto pos_slash = in_view.find('/');

            if (pos_slash == std::string_view::npos)
                throw DB::Exception("The text does not contain '/': " + std::string(in_view),
                                    DB::ErrorCodes::CANNOT_PARSE_TEXT);

            prefix = Address(StringRef(in_view.substr(0, pos_slash)));

            // DB::parse<Uint8>() in <IO/ReadHelpers.h> ignores
            // non-digit characters. std::stoi() skips whitespaces. We
            // need to parse the prefix bits in a strict way.

            if (pos_slash + 1 == in_view.size())
                throw DB::Exception("The CIDR has no prefix bits: " + std::string(in_view),
                                    DB::ErrorCodes::CANNOT_PARSE_TEXT);

            bits = 0;
            for (size_t i = pos_slash + 1; i < in_view.size(); i++)
            {
                const auto c = in_view[i];
                if (c >= '0' && c <= '9')
                {
                    bits *= 10;
                    bits += c - '0';
                }
                else
                {
                    throw DB::Exception("The CIDR has a malformed prefix bits: " + std::string(in_view),
                                        DB::ErrorCodes::CANNOT_PARSE_TEXT);
                }
            }

            const size_t max_bits
                = std::visit([&](const auto & addr_v) -> size_t
                             {
                                 return addr_v.numBits();
                             }, prefix->variant());
            if (bits > max_bits)
                throw DB::Exception("The CIDR has an invalid prefix bits: " + std::string(in_view),
                                    DB::ErrorCodes::CANNOT_PARSE_TEXT);
        }

    private:
        template <typename PrefixT>
        static PrefixT toMask(uint8_t bits)
        {
            if constexpr (std::is_same_v<PrefixT, Address::V4>)
            {
                return PrefixT(DB::getCIDRMaskIPv4(bits));
            }
            else
            {
                return PrefixT(DB::getCIDRMaskIPv6(bits));
            }
        }

        template <typename PrefixT>
        static PrefixT startOf(const PrefixT & prefix, uint8_t bits)
        {
            return prefix & toMask<PrefixT>(bits);
        }

        template <typename PrefixT>
        static PrefixT endOf(const PrefixT & prefix, uint8_t bits)
        {
            return prefix | ~toMask<PrefixT>(bits);
        }

        /* Convert a CIDR notation into an IP address range [start, end]. */
        template <typename PrefixT>
        static std::pair<PrefixT, PrefixT> toRange(const PrefixT & prefix, uint8_t bits)
        {
            return std::make_pair(startOf(prefix, bits), endOf(prefix, bits));
        }

    public:
        bool contains(const Address & addr) const
        {
            return std::visit([&](const auto & addr_v) -> bool
            {
                return std::visit([&](const auto & prefix_v) -> bool
                {
                    using AddrT = std::decay_t<decltype(addr_v)>;
                    using PrefixT = std::decay_t<decltype(prefix_v)>;

                    if constexpr (std::is_same_v<AddrT, Address::V4>)
                    {
                        if constexpr (std::is_same_v<PrefixT, Address::V4>)
                        {
                            const auto range = toRange(prefix_v, bits);
                            return addr_v >= range.first && addr_v <= range.second;
                        }
                        else
                        {
                            return false; // IP version mismatch is not an error.
                        }
                    }
                    else
                    {
                        if constexpr (std::is_same_v<PrefixT, Address::V6>)
                        {
                            const auto range = toRange(prefix_v, bits);
                            return addr_v >= range.first && addr_v <= range.second;
                        }
                        else
                        {
                            return false; // IP version mismatch is not an error.
                        }
                    }
                }, prefix->variant());
            }, addr.variant());
        }

    private:
        std::optional<Address> prefix; // Guaranteed to have a value after construction.
        uint8_t bits;
    };
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
                // col_addr_const is constant and is either String or
                // Nullable(String). We don't care which one it exactly is.

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
                const auto addr = ipaddr::Address(col_addr.getDataAt(0));
                const auto cidr = ipaddr::CIDR(col_cidr.getDataAt(0));

                ColumnUInt8::MutablePtr col_res = ColumnUInt8::create(1);
                ColumnUInt8::Container & vec_res = col_res->getData();

                vec_res[0] = cidr.contains(addr) ? 1 : 0;

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
                const auto addr = ipaddr::Address(col_addr.getDataAt(0));

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
                            const auto cidr = ipaddr::CIDR(col_cidr.getDataAt(i));
                            vec_res[i] = cidr.contains(addr) ? 1 : 0;
                            vec_null_map_res[i] = false;
                        }
                    }

                    return ColumnNullable::create(std::move(col_res), std::move(col_null_map_res));
                }
                else
                {
                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        const auto cidr = ipaddr::CIDR(col_cidr.getDataAt(i));
                        vec_res[i] = cidr.contains(addr) ? 1 : 0;
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
                const auto cidr = ipaddr::CIDR(col_cidr.getDataAt(0));

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
                            const auto addr = ipaddr::Address(col_addr.getDataAt(i));
                            vec_res[i] = cidr.contains(addr) ? 1 : 0;
                            vec_null_map_res[i] = false;
                        }
                    }

                    return ColumnNullable::create(std::move(col_res), std::move(col_null_map_res));
                }
                else
                {
                    for (size_t i = 0; i < input_rows_count; i++)
                    {
                        const auto addr = ipaddr::Address(col_addr.getDataAt(i));
                        vec_res[i] = cidr.contains(addr) ? 1 : 0;
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
                        const auto addr = ipaddr::Address(col_addr.getDataAt(i));
                        const auto cidr = ipaddr::CIDR(col_cidr.getDataAt(i));

                        vec_res[i] = cidr.contains(addr) ? 1 : 0;
                        vec_null_map_res[i] = false;
                    }
                }

                return ColumnNullable::create(std::move(col_res), std::move(col_null_map_res));
            }
            else
            {
                for (size_t i = 0; i < input_rows_count; i++)
                {
                    const auto addr = ipaddr::Address(col_addr.getDataAt(i));
                    const auto cidr = ipaddr::CIDR(col_cidr.getDataAt(i));

                    vec_res[i] = cidr.contains(addr) ? 1 : 0;
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
