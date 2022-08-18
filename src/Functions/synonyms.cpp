#include "config_core.h"

#if USE_NLP

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/SynonymsExtensions.h>

#include <string_view>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
}

class FunctionSynonyms : public IFunction
{
public:
    static constexpr auto name = "synonyms";
    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef().allow_experimental_nlp_functions)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Natural language processing function '{}' is experimental. Set `allow_experimental_nlp_functions` setting to enable it", name);

        return std::make_shared<FunctionSynonyms>(context->getSynonymsExtensions());
    }

private:
    SynonymsExtensions & extensions;

public:
    explicit FunctionSynonyms(SynonymsExtensions & extensions_)
        : extensions(extensions_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & extcolumn = arguments[0].column;
        const auto & strcolumn = arguments[1].column;

        const ColumnConst * ext_col = checkAndGetColumn<ColumnConst>(extcolumn.get());
        const ColumnString * word_col = checkAndGetColumn<ColumnString>(strcolumn.get());

        if (!ext_col)
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        if (!word_col)
            throw Exception(
                "Illegal column " + arguments[1].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        String ext_name = ext_col->getValue<String>();
        auto extension = extensions.getExtension(ext_name);

        /// Create and fill the result array.
        const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();

        auto out = ColumnArray::create(elem_type->createColumn());
        IColumn & out_data = out->getData();
        IColumn::Offsets & out_offsets = out->getOffsets();

        const ColumnString::Chars & data = word_col->getChars();
        const ColumnString::Offsets & offsets = word_col->getOffsets();
        out_data.reserve(input_rows_count);
        out_offsets.resize(input_rows_count);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            std::string_view word(reinterpret_cast<const char *>(data.data() + offsets[i - 1]), offsets[i] - offsets[i - 1] - 1);

            const auto * synset = extension->getSynonyms(word);

            if (synset)
            {
                for (const auto & token : *synset)
                    out_data.insert(Field(token.data(), token.size()));

                current_offset += synset->size();
            }
            out_offsets[i] = current_offset;
        }

        return out;
    }
};

void registerFunctionSynonyms(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSynonyms>(FunctionFactory::CaseInsensitive);
}

}

#endif
