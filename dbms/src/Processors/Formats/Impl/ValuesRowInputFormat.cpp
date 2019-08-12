#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Formats/Impl/ValuesRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Common/FieldVisitors.h>
#include <Core/Block.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_DATE;
    extern const int SYNTAX_ERROR;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}


ValuesRowInputFormat::ValuesRowInputFormat(
    ReadBuffer & in_, Block header_, Params params_, const Context & context_, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header_), in_, params_)
    , context(std::make_unique<Context>(context_)), format_settings(format_settings_)
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(in);
}


bool ValuesRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    size_t num_columns = columns.size();
    auto & header = getPort().getHeader();

    skipWhitespaceIfAny(in);

    if (in.eof() || *in.position() == ';')
        return false;

    /** Typically, this is the usual format for streaming parsing.
      * But as an exception, it also supports processing arbitrary expressions instead of values.
      * This is very inefficient. But if there are no expressions, then there is no overhead.
      */
    ParserExpression parser;

    assertChar('(', in);

    for (size_t i = 0; i < num_columns; ++i)
    {
        skipWhitespaceIfAny(in);

        char * prev_in_position = in.position();
        size_t prev_in_bytes = in.count() - in.offset();

        bool rollback_on_exception = false;
        try
        {
            header.getByPosition(i).type->deserializeAsTextQuoted(*columns[i], in, format_settings);
            rollback_on_exception = true;
            skipWhitespaceIfAny(in);

            if (i != num_columns - 1)
                assertChar(',', in);
            else
                assertChar(')', in);
        }
        catch (const Exception & e)
        {
            if (!format_settings.values.interpret_expressions)
                throw;

            /** The normal streaming parser could not parse the value.
              * Let's try to parse it with a SQL parser as a constant expression.
              * This is an exceptional case.
              */
            if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
                || e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
                || e.code() == ErrorCodes::CANNOT_PARSE_NUMBER
                || e.code() == ErrorCodes::CANNOT_PARSE_DATE
                || e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
                || e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT)
            {
                /// TODO Case when the expression does not fit entirely in the buffer.

                /// If the beginning of the value is no longer in the buffer.
                if (in.count() - in.offset() != prev_in_bytes)
                    throw;

                if (rollback_on_exception)
                    columns[i]->popBack(1);

                const IDataType & type = *header.getByPosition(i).type;

                Expected expected;

                Tokens tokens(prev_in_position, in.buffer().end());
                IParser::Pos token_iterator(tokens);

                ASTPtr ast;
                if (!parser.parse(token_iterator, ast, expected))
                    throw Exception("Cannot parse expression of type " + type.getName() + " here: "
                        + String(prev_in_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, in.buffer().end() - prev_in_position)),
                        ErrorCodes::SYNTAX_ERROR);

                in.position() = const_cast<char *>(token_iterator->begin);

                std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(ast, *context);
                Field value = convertFieldToType(value_raw.first, type, value_raw.second.get());

                /// Check that we are indeed allowed to insert a NULL.
                if (value.isNull())
                {
                    if (!type.isNullable())
                        throw Exception{"Expression returns value " + applyVisitor(FieldVisitorToString(), value)
                            + ", that is out of range of type " + type.getName()
                            + ", at: " + String(prev_in_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, in.buffer().end() - prev_in_position)),
                            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE};
                }

                columns[i]->insert(value);

                skipWhitespaceIfAny(in);

                if (i != num_columns - 1)
                    assertChar(',', in);
                else
                    assertChar(')', in);
            }
            else
                throw;
        }
    }

    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == ',')
        ++in.position();

    return true;
}


void registerInputFormatProcessorValues(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("Values", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<ValuesRowInputFormat>(buf, sample, std::move(params), context, settings);
    });
}

}
