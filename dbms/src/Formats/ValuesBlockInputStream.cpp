#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/ExpressionListParsers.h>
#include <Formats/ValuesBlockInputStream.h>
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
    extern const int CANNOT_CREATE_EXPRESSION_TEMPLATE;
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
    extern const int CANNOT_EVALUATE_EXPRESSION_TEMPLATE;
}


ValuesBlockInputStream::ValuesBlockInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_,
                                               const FormatSettings & format_settings, UInt64 max_block_size_)
        : istr(istr_), header(header_), context(std::make_unique<Context>(context_)),
          format_settings(format_settings), max_block_size(max_block_size_), num_columns(header.columns())
{
    templates.resize(header.columns());
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(istr);
}

Block ValuesBlockInputStream::readImpl()
{
    MutableColumns columns = header.cloneEmptyColumns();

    for (size_t rows_in_block = 0; rows_in_block < max_block_size; ++rows_in_block)
    {
        try
        {
            skipWhitespaceIfAny(istr);
            if (istr.eof() || *istr.position() == ';')
                break;
            assertChar('(', istr);

            for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
            {
                skipWhitespaceIfAny(istr);
                istr.setCheckpoint();

                bool parse_separate_value = true;
                if (templates[column_idx])
                {
                    /// Try to parse expression using template if one was successfully generated while parsing the first row
                    try
                    {
                        templates[column_idx].value().parseExpression(istr, format_settings);
                        assertDelimAfterValue(column_idx);
                        parse_separate_value = false;
                    }
                    catch (DB::Exception & e)
                    {
                        if (e.code() != ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE)
                            throw;
                        /// Expression in the current row is not match generated on the first row template.
                        /// Evaluate expressions, which were parsed using this template.
                        columns[column_idx] = std::move(*templates[column_idx].value().evaluateAll()).mutate();
                        /// Do not use the template anymore and fallback to slow SQL parser
                        templates[column_idx].reset();
                        istr.rollbackToCheckpoint();
                    }
                }

                /// Parse value using fast streaming parser for literals and slow SQL parser for expressions.
                /// If there is SQL expression in the first row, template of this expression will be generated,
                /// so it makes possible to parse next rows much faster if expressions in next rows have the same structure
                if (parse_separate_value)
                    readValue(*columns[column_idx], column_idx, rows_in_block == 0);

                istr.dropCheckpoint();
            }

            skipWhitespaceIfAny(istr);
            if (!istr.eof() && *istr.position() == ',')
                ++istr.position();

            ++total_rows;
        }
        catch (Exception & e)
        {
            if (isParseError(e.code()))
                e.addMessage(" at row " + std::to_string(total_rows));
            throw;
        }
    }

    /// Evaluate expressions, which were parsed using templates, if any
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (templates[i])
        {
            columns[i] = std::move(*templates[i].value().evaluateAll()).mutate();
            templates[i].reset();
        }
    }

    if (columns.empty() || columns[0]->empty())
        return {};

    return header.cloneWithColumns(std::move(columns));
}

void ValuesBlockInputStream::readValue(IColumn & column, size_t column_idx, bool generate_template)
{
    bool rollback_on_exception = false;
    try
    {
        header.getByPosition(column_idx).type->deserializeAsTextQuoted(column, istr, format_settings);
        rollback_on_exception = true;

        skipWhitespaceIfAny(istr);

        assertDelimAfterValue(column_idx);
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
            if (rollback_on_exception)
                column.popBack(1);

            istr.rollbackToCheckpoint();
            parseExpression(column, column_idx, generate_template);
        }
        else
            throw;
    }
}

void
ValuesBlockInputStream::parseExpression(IColumn & column, size_t column_idx, bool generate_template)
{
    const IDataType & type = *header.getByPosition(column_idx).type;

    Expected expected;

    // TODO make tokenizer to work with buffers, not only with continuous memory
    Tokens tokens(istr.position(), istr.buffer().end());
    TokenIterator token_iterator(tokens);

    ASTPtr ast;
    if (!parser.parse(token_iterator, ast, expected))
    {
        istr.rollbackToCheckpoint();
        throw Exception("Cannot parse expression of type " + type.getName() + " here: "
                        + String(istr.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - istr.position())),
                        ErrorCodes::SYNTAX_ERROR);
    }

    std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(ast, *context);
    Field value = convertFieldToType(value_raw.first, type, value_raw.second.get());

    /// Check that we are indeed allowed to insert a NULL.
    if (value.isNull() && !type.isNullable())
    {
        istr.rollbackToCheckpoint();
        throw Exception{"Expression returns value " + applyVisitor(FieldVisitorToString(), value)
                        + ", that is out of range of type " + type.getName()
                        + ", at: " +
                        String(istr.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - istr.position())),
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE};
    }

    istr.position() = const_cast<char *>(token_iterator->begin);

    if (generate_template)
    {
        if (templates[column_idx])
            throw DB::Exception("Template for column " + std::to_string(column_idx) + " already exists and it was not evaluated yet",
                                ErrorCodes::LOGICAL_ERROR);
        try
        {
            templates[column_idx] = ConstantExpressionTemplate(type, TokenIterator(tokens), token_iterator, *context);
            istr.rollbackToCheckpoint();
            templates[column_idx].value().parseExpression(istr, format_settings);
            assertDelimAfterValue(column_idx);
            return;
        }
        catch (...)
        {
            /// Continue parsing without template
            templates[column_idx].reset();
            istr.position() = const_cast<char *>(token_iterator->begin);
        }
    }

    assertDelimAfterValue(column_idx);
    column.insert(value);
}

void ValuesBlockInputStream::assertDelimAfterValue(size_t column_idx)
{
    skipWhitespaceIfAny(istr);

    if (column_idx + 1 != num_columns)
        assertChar(',', istr);
    else
        assertChar(')', istr);
}


void registerInputFormatValues(FormatFactory & factory)
{
    factory.registerInputFormat("Values", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        const FormatSettings & settings)
    {
        return std::make_shared<ValuesBlockInputStream>(buf, sample, context, settings, max_block_size);
    });
}

}
