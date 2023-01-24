#include <memory>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <base/map.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FORMAT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** formatRow(<format>, x, y, ...) is a function that allows you to use RowOutputFormat over
  * several columns to generate a string per row, such as CSV, TSV, JSONEachRow, etc.
  * formatRowNoNewline(...) trims the newline character of each row.
  */
template <bool no_newline>
class FunctionFormatRow : public IFunction
{
public:
    static constexpr auto name = no_newline ? "formatRowNoNewline" : "formatRow";

    FunctionFormatRow(const String & format_name_, ContextPtr context_) : format_name(format_name_), context(context_)
    {
        if (!FormatFactory::instance().getAllFormats().contains(format_name))
            throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown format {}", format_name);
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_str = ColumnString::create();
        ColumnString::Chars & vec = col_str->getChars();
        WriteBufferFromVector buffer(vec);
        ColumnString::Offsets & offsets = col_str->getOffsets();
        offsets.resize(input_rows_count);
        Block arg_columns;
        for (auto i = 1u; i < arguments.size(); ++i)
            arg_columns.insert(arguments[i]);
        materializeBlockInplace(arg_columns);
        auto format_settings = getFormatSettings(context);
        auto out = FormatFactory::instance().getOutputFormat(format_name, buffer, arg_columns, context, format_settings);

        /// This function make sense only for row output formats.
        auto * row_output_format = dynamic_cast<IRowOutputFormat *>(out.get());
        if (!row_output_format)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot turn rows into a {} format strings. {} function supports only row output formats",
                            format_name, getName());

        auto columns = arg_columns.getColumns();
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            row_output_format->writePrefixIfNeeded();
            row_output_format->writeRow(columns, i);
            row_output_format->finalize();
            if constexpr (no_newline)
            {
                // replace '\n' with '\0'
                if (buffer.position() != buffer.buffer().begin() && buffer.position()[-1] == '\n')
                    buffer.position()[-1] = '\0';
            }
            else
                writeChar('\0', buffer);

            offsets[i] = buffer.count();
            row_output_format->resetFormatter();
        }

        return col_str;
    }

private:
    String format_name;
    ContextPtr context;
};

template <bool no_newline>
class FormatRowOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = no_newline ? "formatRowNoNewline" : "formatRow";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<FormatRowOverloadResolver>(context); }
    explicit FormatRowOverloadResolver(ContextPtr context_) : context(context_) { }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least two arguments: the format name and its output expression(s)", getName());

        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments.at(0).column.get()))
            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionFormatRow<no_newline>>(name_col->getValue<String>(), context),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument to {} must be a format name", getName());
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(FormatRow)
{
    factory.registerFunction<FormatRowOverloadResolver<true>>();
    factory.registerFunction<FormatRowOverloadResolver<false>>();
}

}
