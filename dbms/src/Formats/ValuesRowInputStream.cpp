#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/ExpressionListParsers.h>
#include <Formats/ValuesRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
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


ValuesRowInputStream::ValuesRowInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_, const FormatSettings & format_settings)
    : istr(istr_), header(header_), context(context_), format_settings(format_settings)
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(istr);
}


bool ValuesRowInputStream::read(MutableColumns & columns)
{
    size_t num_columns = columns.size();

    skipWhitespaceIfAny(istr);

    if (istr.eof() || *istr.position() == ';')
        return false;

    /** Typically, this is the usual format for streaming parsing.
      * But as an exception, it also supports processing arbitrary expressions instead of values.
      * This is very inefficient. But if there are no expressions, then there is no overhead.
      */
    ParserExpression parser;

    assertChar('(', istr);

    for (size_t i = 0; i < num_columns; ++i)
    {
        skipWhitespaceIfAny(istr);

        char * prev_istr_position = istr.position();
        size_t prev_istr_bytes = istr.count() - istr.offset();

        bool rollback_on_exception = false;
        try
        {
            header.getByPosition(i).type->deserializeTextQuoted(*columns[i], istr, format_settings);
            rollback_on_exception = true;
            skipWhitespaceIfAny(istr);

            if (i != num_columns - 1)
                assertChar(',', istr);
            else
                assertChar(')', istr);
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
                if (istr.count() - istr.offset() != prev_istr_bytes)
                    throw;

                if (rollback_on_exception)
                    columns[i]->popBack(1);

                const IDataType & type = *header.getByPosition(i).type;

                Expected expected;

                Tokens tokens(prev_istr_position, istr.buffer().end());
                TokenIterator token_iterator(tokens);

                ASTPtr ast;
                if (!parser.parse(token_iterator, ast, expected))
                    throw Exception("Cannot parse expression of type " + type.getName() + " here: "
                        + String(prev_istr_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - prev_istr_position)),
                        ErrorCodes::SYNTAX_ERROR);

                istr.position() = const_cast<char *>(token_iterator->begin);

                std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(ast, context);
                Field value = convertFieldToType(value_raw.first, type, value_raw.second.get());

                if (value.isNull())
                {
                    /// Check that we are indeed allowed to insert a NULL.
                    if (!type.isNullable())
                        throw Exception{"Expression returns value " + applyVisitor(FieldVisitorToString(), value)
                            + ", that is out of range of type " + type.getName()
                            + ", at: " + String(prev_istr_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - prev_istr_position)),
                            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE};
                }

                columns[i]->insert(value);

                skipWhitespaceIfAny(istr);

                if (i != num_columns - 1)
                    assertChar(',', istr);
                else
                    assertChar(')', istr);
            }
            else
                throw;
        }
    }

    skipWhitespaceIfAny(istr);
    if (!istr.eof() && *istr.position() == ',')
        ++istr.position();

    return true;
}


void registerInputFormatValues(FormatFactory & factory)
{
    factory.registerInputFormat("Values", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        size_t max_block_size,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
            std::make_shared<ValuesRowInputStream>(buf, sample, context, settings),
            sample, max_block_size, settings);
    });
}

}
