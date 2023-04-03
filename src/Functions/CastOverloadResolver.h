#pragma once
#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/*
 * CastInternal does not preserve nullability of the data type,
 * i.e. CastInternal(toNullable(toInt8(1)) as Int32) will be Int32(1).
 *
 * Cast preserves nullability according to setting `cast_keep_nullable`,
 * i.e. Cast(toNullable(toInt8(1)) as Int32) will be Nullable(Int32(1)) if `cast_keep_nullable` == 1.
**/
template<CastType cast_type, bool internal, typename CastName, typename FunctionName>
class CastOverloadResolverImpl : public IFunctionOverloadResolver
{
public:
    using MonotonicityForRange = FunctionCastBase::MonotonicityForRange;
    using Diagnostic = FunctionCastBase::Diagnostic;

    static constexpr auto name = cast_type == CastType::accurate
        ? CastName::accurate_cast_name
        : (cast_type == CastType::accurateOrNull ? CastName::accurate_cast_or_null_name : CastName::cast_name);

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    explicit CastOverloadResolverImpl(std::optional<Diagnostic> diagnostic_, bool keep_nullable_, bool cast_ipv4_ipv6_default_on_conversion_error_)
        : diagnostic(std::move(diagnostic_))
        , keep_nullable(keep_nullable_)
        , cast_ipv4_ipv6_default_on_conversion_error(cast_ipv4_ipv6_default_on_conversion_error_)
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        const auto & settings_ref = context->getSettingsRef();

        if constexpr (internal)
            return createImpl({}, false /*keep_nullable*/, settings_ref.cast_ipv4_ipv6_default_on_conversion_error);

        return createImpl({}, settings_ref.cast_keep_nullable, settings_ref.cast_ipv4_ipv6_default_on_conversion_error);
    }

    static FunctionOverloadResolverPtr createImpl(std::optional<Diagnostic> diagnostic = {}, bool keep_nullable = false, bool cast_ipv4_ipv6_default_on_conversion_error = false)
    {
        assert(!internal || !keep_nullable);
        return std::make_unique<CastOverloadResolverImpl>(std::move(diagnostic), keep_nullable, cast_ipv4_ipv6_default_on_conversion_error);
    }

protected:

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        auto monotonicity = MonotonicityHelper::getMonotonicityInformation(arguments.front().type, return_type.get());
        return std::make_unique<FunctionCast<FunctionName>>(name, std::move(monotonicity), data_types, return_type, diagnostic, cast_type, cast_ipv4_ipv6_default_on_conversion_error);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & column = arguments.back().column;
        if (!column)
            throw Exception("Second argument to " + getName() + " must be a constant string describing type."
                " Instead there is non-constant column of type " + arguments.back().type->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * type_col = checkAndGetColumnConst<ColumnString>(column.get());
        if (!type_col)
            throw Exception("Second argument to " + getName() + " must be a constant string describing type."
                " Instead there is a column with the following structure: " + column->dumpStructure(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr type = DataTypeFactory::instance().get(type_col->getValue<String>());

        if constexpr (cast_type == CastType::accurateOrNull)
            return makeNullable(type);

        if constexpr (internal)
            return type;

        if (keep_nullable && arguments.front().type->isNullable() && type->canBeInsideNullable())
            return makeNullable(type);

        return type;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    std::optional<Diagnostic> diagnostic;
    bool keep_nullable;
    bool cast_ipv4_ipv6_default_on_conversion_error;
};


struct CastOverloadName
{
    static constexpr auto cast_name = "CAST";
    static constexpr auto accurate_cast_name = "accurateCast";
    static constexpr auto accurate_cast_or_null_name = "accurateCastOrNull";
};

struct CastInternalOverloadName
{
    static constexpr auto cast_name = "_CAST";
    static constexpr auto accurate_cast_name = "accurate_Cast";
    static constexpr auto accurate_cast_or_null_name = "accurate_CastOrNull";
};

template <CastType cast_type>
using CastOverloadResolver = CastOverloadResolverImpl<cast_type, false, CastOverloadName, CastName>;

template <CastType cast_type>
using CastInternalOverloadResolver = CastOverloadResolverImpl<cast_type, true, CastInternalOverloadName, CastInternalName>;

}
