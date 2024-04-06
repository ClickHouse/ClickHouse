#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

FunctionBasePtr createFunctionBaseCast(
    ContextPtr context,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type);


/** CastInternal does not preserve nullability of the data type,
  * i.e. CastInternal(toNullable(toInt8(1)) as Int32) will be Int32(1).
  *
  * Cast preserves nullability according to setting `cast_keep_nullable`,
  * i.e. Cast(toNullable(toInt8(1)) as Int32) will be Nullable(Int32(1)) if `cast_keep_nullable` == 1.
  */
class CastOverloadResolverImpl : public IFunctionOverloadResolver
{
public:
    const char * getNameImpl() const
    {
        if (cast_type == CastType::accurate)
            return "accurateCast";
        if (cast_type == CastType::accurateOrNull)
            return "accurateCastOrNull";
        if (internal)
            return "_CAST";
        else
            return "CAST";
    }

    String getName() const override
    {
        return getNameImpl();
    }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    explicit CastOverloadResolverImpl(ContextPtr context_, CastType cast_type_, bool internal_, std::optional<CastDiagnostic> diagnostic_, bool keep_nullable_, const DataTypeValidationSettings & data_type_validation_settings_)
        : context(context_)
        , cast_type(cast_type_)
        , internal(internal_)
        , diagnostic(std::move(diagnostic_))
        , keep_nullable(keep_nullable_)
        , data_type_validation_settings(data_type_validation_settings_)
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context, CastType cast_type, bool internal, std::optional<CastDiagnostic> diagnostic)
    {
        if (internal)
        {
            return std::make_unique<CastOverloadResolverImpl>(context, cast_type, internal, diagnostic, false /*keep_nullable*/, DataTypeValidationSettings{});
        }
        else
        {
            const auto & settings_ref = context->getSettingsRef();
            return std::make_unique<CastOverloadResolverImpl>(context, cast_type, internal, diagnostic, settings_ref.cast_keep_nullable, DataTypeValidationSettings(settings_ref));
        }
    }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        return createFunctionBaseCast(context, getNameImpl(), arguments, return_type, diagnostic, cast_type);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & column = arguments.back().column;
        if (!column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant string describing type. "
                "Instead there is non-constant column of type {}", getName(), arguments.back().type->getName());

        const auto * type_col = checkAndGetColumnConst<ColumnString>(column.get());
        if (!type_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant string describing type. "
                "Instead there is a column with the following structure: {}", getName(), column->dumpStructure());

        DataTypePtr type = DataTypeFactory::instance().get(type_col->getValue<String>());
        validateDataType(type, data_type_validation_settings);

        if (cast_type == CastType::accurateOrNull)
        {
            /// Variant handles NULLs by itself during conversions.
            if (!isVariant(type))
                return makeNullable(type);
        }

        if (internal)
            return type;

        if (keep_nullable && arguments.front().type->isNullable() && type->canBeInsideNullable())
            return makeNullable(type);

        return type;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    ContextPtr context;
    CastType cast_type;
    bool internal;
    std::optional<CastDiagnostic> diagnostic;
    bool keep_nullable;
    DataTypeValidationSettings data_type_validation_settings;
};


FunctionOverloadResolverPtr createInternalCastOverloadResolver(CastType type, std::optional<CastDiagnostic> diagnostic)
{
    return CastOverloadResolverImpl::create(ContextPtr{}, type, true, diagnostic);
}

REGISTER_FUNCTION(CastOverloadResolvers)
{
    factory.registerFunction("_CAST", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::nonAccurate, true, {}); }, {}, FunctionFactory::CaseInsensitive);
    /// Note: "internal" (not affected by null preserving setting) versions of accurate cast functions are unneeded.

    factory.registerFunction("CAST", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::nonAccurate, false, {}); }, {}, FunctionFactory::CaseInsensitive);
    factory.registerFunction("accurateCast", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::accurate, false, {}); }, {});
    factory.registerFunction("accurateCastOrNull", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::accurateOrNull, false, {}); }, {});
}

}
