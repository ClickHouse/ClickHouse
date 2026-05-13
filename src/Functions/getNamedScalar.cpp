#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Access/ContextAccess.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/NamedScalars/NamedScalar.h>
#include <Interpreters/NamedScalars/NamedScalarsManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NAMED_SCALAR_HAS_NO_VALUE;
    extern const int NAMED_SCALAR_NOT_FOUND;
}

namespace
{

enum class ErrorHandlingMode : uint8_t
{
    Exception,
    Default,
};

template <ErrorHandlingMode Mode>
constexpr const char * functionNameFor()
{
    return Mode == ErrorHandlingMode::Exception ? "getNamedScalar" : "getNamedScalarOrDefault";
}

String extractNamedScalarName(const ColumnsWithTypeAndName & arguments, const String & function_name)
{
    const auto * column = arguments[0].column.get();
    const auto * const_col = column ? checkAndGetColumnConstStringOrFixedString(column) : nullptr;
    if (!isString(arguments[0].type) || !const_col)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The argument of function {} should be a constant string with the name of a scalar",
            function_name);

    String name(column->getDataAt(0));
    if (name.empty())
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The argument of function {} must be a non-empty string",
            function_name);
    return name;
}

Field extractDefaultValue(const ColumnsWithTypeAndName & arguments, const String & function_name)
{
    if (!arguments[1].column || !isColumnConst(*arguments[1].column))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The second argument of function {} should be a constant default value",
            function_name);
    return (*arguments[1].column)[0];
}

class ExecutableFunctionGetNamedScalar final : public IExecutableFunction
{
public:
    ExecutableFunctionGetNamedScalar(String name_, Field value_)
        : name(std::move(name_))
        , value(std::move(value_))
    {
    }

    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, value);
    }

private:
    String name;
    Field value;
};

class FunctionBaseGetNamedScalar final : public IFunctionBase
{
public:
    FunctionBaseGetNamedScalar(String name_, DataTypes argument_types_, DataTypePtr result_type_, Field value_)
        : name(std::move(name_))
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
        , value(std::move(value_))
    {
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionGetNamedScalar>(name, value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

private:
    String name;
    DataTypes argument_types;
    DataTypePtr result_type;
    Field value;
};

template <ErrorHandlingMode Mode>
class GetNamedScalarOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = functionNameFor<Mode>();

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<GetNamedScalarOverloadResolver>(context_);
    }

    explicit GetNamedScalarOverloadResolver(ContextPtr context_)
        : context(std::move(context_))
    {
    }

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return (Mode == ErrorHandlingMode::Default) ? 2 : 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (Mode == ErrorHandlingMode::Default)
            return {0, 1};
        else
            return {0};
    }

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const override
    {
        checkNumberOfArguments(arguments.size());
        context->checkAccess(AccessType::getNamedScalar);

        const String bare_name = extractNamedScalarName(arguments, getName());
        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        /// Single lookup: tryGetScopedScalar returns the scalar plus its
        /// cache_kind so the error path's `kind_str` can't race a concurrent
        /// drop and report the wrong kind.
        auto scoped = context->getNamedScalarsManager().tryGetScopedScalar(bare_name);
        const auto & scalar = scoped ? scoped->scalar : nullptr;
        if (scalar)
        {
            if (auto snapshot = scalar->tryGetValue())
            {
                return std::make_unique<FunctionBaseGetNamedScalar>(
                    getName(),
                    std::move(argument_types),
                    snapshot->type,
                    std::move(snapshot->value));
            }
        }

        if constexpr (Mode == ErrorHandlingMode::Default)
        {
            return std::make_unique<FunctionBaseGetNamedScalar>(
                getName(),
                std::move(argument_types),
                arguments[1].type,
                extractDefaultValue(arguments, getName()));
        }
        else
        {
            if (!scalar)
                throw Exception(
                    ErrorCodes::NAMED_SCALAR_NOT_FOUND,
                    "No named scalar '{}'",
                    bare_name);

            const std::string_view kind_str = toString(scoped->cache_kind);

            /// last_error is the definer's exception text; it can disclose
            /// schema / permission detail under the definer's privileges.
            /// Only surface it to readers who already have operator-tier
            /// visibility (SHOW_NAMED_SCALARS); other callers get the short
            /// form and are pointed at system.named_scalars.
            const auto status = scalar->getInfo();
            const bool show_full = context->getAccess()->isGranted(AccessType::SHOW_NAMED_SCALARS);
            if (show_full && !status.last_error.empty())
                throw Exception(
                    ErrorCodes::NAMED_SCALAR_HAS_NO_VALUE,
                    "{} scalar '{}' has no value (last error [{}]: {})",
                    kind_str,
                    bare_name,
                    status.last_error_type,
                    status.last_error);

            throw Exception(
                ErrorCodes::NAMED_SCALAR_HAS_NO_VALUE,
                "{} scalar '{}' has no value; query system.named_scalars "
                "with SHOW_NAMED_SCALARS for the last error",
                kind_str,
                bare_name);
        }
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(GetNamedScalar)
{
    using M = ErrorHandlingMode;
    using FunctionFactory_Case = FunctionFactory::Case;

    factory.registerFunction<GetNamedScalarOverloadResolver<M::Exception>>(
        FunctionDocumentation{
            .description = "Returns the current value of a named scalar. Throws if the named scalar does not exist or has no value.",
            .syntax = "getNamedScalar(named_scalar_name)",
            .arguments = {{"named_scalar_name", "The bare scalar name.", {"const String"}}},
            .returned_value = {"Current value of the named scalar.", {"Any"}},
            .introduced_in = {26, 3},
            .category = FunctionDocumentation::Category::Other,
        },
        FunctionFactory_Case::Sensitive);

    factory.registerFunction<GetNamedScalarOverloadResolver<M::Default>>(
        FunctionDocumentation{
            .description = "Returns the current value of a named scalar, or the provided default if it is not defined or has no value.",
            .syntax = "getNamedScalarOrDefault(named_scalar_name, default_value)",
            .arguments = {
                {"named_scalar_name", "The bare scalar name.", {"const String"}},
                {"default_value", "Value to return if the named scalar is not defined.", {"Any"}},
            },
            .returned_value = {"Current value, or default_value.", {"Any"}},
            .introduced_in = {26, 3},
            .category = FunctionDocumentation::Category::Other,
        },
        FunctionFactory_Case::Sensitive);
}

}
