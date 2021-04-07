#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeSecret.h>
#include <Secrets/SecretsManager.h>
#include <Common/typeid_cast.h>
#include <common/StringRef.h>

#include <Functions/FunctionHelpers.h>

#include <Interpreters/Context.h>

namespace DB
{

class ISecretsProvider;

class FunctionSecret : public IFunction
{
    const ISecretsProvider & secrets_provider;

public:
    explicit FunctionSecret(const ISecretsProvider & secrets_provider_)
        : secrets_provider(secrets_provider_)
    {}

    static FunctionPtr create(const ContextPtr & context)
    {
        return std::make_shared<FunctionSecret>(context->getSecretsProvider());
    }

    static constexpr auto name = "secret";

    String getName() const override
    {
        return "secret";
    }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & secret_names_col = typeid_cast<const ColumnString &>(*arguments[0].column);
        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto secret_value = secrets_provider.getSecretValue(static_cast<std::string_view>(secret_names_col.getDataAt(i)));
            result->insertData(secret_value.str().data(), secret_value.str().length());
        }

        result->validate();
        return result;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateFunctionArgumentTypes(*this, arguments,
                FunctionArgumentDescriptors{{"secret_name", isString, isColumnConst, "String with name of the secret."}});

        return std::make_shared<DataTypeSecret>();
    }
};

void registerFunctionSecret(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSecret>(FunctionFactory::CaseSensitive);
}

}
