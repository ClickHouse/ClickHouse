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
  */

class FunctionFormatRow : public IFunction
{
public:
    static constexpr auto name = "formatRow";

    FunctionFormatRow(const String & format_name_, ContextPtr context_) : format_name(format_name_), context(context_)
    {
        if (!FormatFactory::instance().getAllFormats().contains(format_name))
            throw Exception("Unknown format " + format_name, ErrorCodes::UNKNOWN_FORMAT);
        /// It's impossible to output separate rows in Avro format, because of specific
        /// implementation (we cannot separate table schema and rows, rows are written
        /// in our buffer in batches)
        if (format_name == "Avro")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format Avro is not supported in function {}");

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
        /// For SQLInsert output format we should set max_batch_size settings to 1 so
        /// each line will contain prefix INSERT INTO ... (otherwise only subset of columns
        /// will contain it according to max_batch_size setting)
        format_settings.sql_insert.max_batch_size = 1;
        auto out = FormatFactory::instance().getOutputFormat(format_name, buffer, arg_columns, context, {}, format_settings);

        /// This function make sense only for row output formats.
        auto * row_output_format = dynamic_cast<IRowOutputFormat *>(out.get());
        if (!row_output_format)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot turn rows into a {} format strings. {} function supports only row output formats", format_name, getName());

        auto & working_buf = row_output_format->getWriteBuffer();
        auto columns = arg_columns.getColumns();
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            row_output_format->write(columns, i);
            writeChar('\0', working_buf);
            offsets[i] = working_buf.count();
        }

        return col_str;
    }

private:
    String format_name;
    ContextPtr context;
};

class FormatRowOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "formatRow";
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
            throw Exception(
                "Function " + getName() + " requires at least two arguments: the format name and its output expression(s)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments.at(0).column.get()))
            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionFormatRow>(name_col->getValue<String>(), context),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        else
            throw Exception("First argument to " + getName() + " must be a format name", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(FormatRow)
{
    factory.registerFunction<FormatRowOverloadResolver>();
}

}
