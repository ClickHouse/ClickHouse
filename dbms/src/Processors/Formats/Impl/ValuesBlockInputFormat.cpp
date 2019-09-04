#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
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
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
}


ValuesBlockInputFormat::ValuesBlockInputFormat(ReadBuffer & in_, const Block & header_, const RowInputFormatParams & params_,
                                               const Context & context_, const FormatSettings & format_settings_)
        : IInputFormat(header_, buf), buf(in_), params(params_), context(std::make_unique<Context>(context_)),
          format_settings(format_settings_), num_columns(header_.columns()),
          attempts_to_deduce_template(num_columns), rows_parsed_using_template(num_columns), templates(num_columns)
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(buf);
    /// TODO remove before merge
    const_cast<FormatSettings&>(this->format_settings).values.interpret_expressions = false;
}

Chunk ValuesBlockInputFormat::generate()
{
    const Block & header = getPort().getHeader();
    MutableColumns columns = header.cloneEmptyColumns();

    for (size_t rows_in_block = 0, batch = 0; rows_in_block < params.max_block_size; ++rows_in_block, ++batch)
    {
        if (params.rows_portion_size && batch == params.rows_portion_size)
        {
            batch = 0;
            if (!checkTimeLimit(params, total_stopwatch) || isCancelled())
                break;
        }
        try
        {
            skipWhitespaceIfAny(buf);
            if (buf.eof() || *buf.position() == ';')
                break;
            assertChar('(', buf);

            for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
            {
                skipWhitespaceIfAny(buf);
                PeekableReadBufferCheckpoint checkpoint{buf};

                /// Parse value using fast streaming parser for literals and slow SQL parser for expressions.
                /// If there is SQL expression in the first row, template of this expression will be deduced,
                /// so it makes possible to parse next rows much faster if expressions in next rows have the same structure
                if (!parseExpressionUsingTemplate(columns[column_idx], column_idx))
                    readValueOrParseSeparateExpression(*columns[column_idx], column_idx);
            }

            skipWhitespaceIfAny(buf);
            if (!buf.eof() && *buf.position() == ',')
                ++buf.position();

            ++total_rows;
        }
        catch (Exception & e)
        {
            if (isParseError(e.code()) || e.code() == ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE)
                e.addMessage(" at row " + std::to_string(total_rows));
            throw;
        }
    }
    if (!buf.eof())
    {
        assertChar(';', buf);
        skipWhitespaceIfAny(buf);
    }

    /// Evaluate expressions, which were parsed using templates, if any
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!templates[i] || !templates[i].value().rowsCount())
            continue;
        if (columns[i]->empty())
            columns[i] = std::move(*templates[i].value().evaluateAll()).mutate();
        else
        {
            ColumnPtr evaluated = templates[i].value().evaluateAll();
            columns[i]->insertRangeFrom(*evaluated, 0, evaluated->size());
        }
    }

    if (columns.empty() || columns[0]->empty())
    {
        readSuffix();
        return {};
    }

    size_t rows_in_block = columns[0]->size();
    return Chunk{std::move(columns), rows_in_block};
}

bool ValuesBlockInputFormat::parseExpressionUsingTemplate(MutableColumnPtr & column, size_t column_idx)
{
    if (templates[column_idx])
    {
        /// Try to parse expression using template if one was successfully deduced while parsing the first row
        try
        {
            templates[column_idx].value().parseExpression(buf, format_settings);
            assertDelimiterAfterValue(column_idx);
            ++rows_parsed_using_template[column_idx];
            return true;
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE &&
                e.code() != ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
                throw;
            /// Expression in the current row is not match template deduced on the first row.
            /// Evaluate expressions, which were parsed using this template.
            if (column->empty())
                column = std::move(*templates[column_idx].value().evaluateAll()).mutate();
            else
            {
                ColumnPtr evaluated = templates[column_idx].value().evaluateAll();
                column->insertRangeFrom(*evaluated, 0, evaluated->size());
            }
            /// Do not use this template anymore (will try to deduce the new one or fallback to slow SQL parser)
            templates[column_idx].reset();
            ++attempts_to_deduce_template[column_idx];
            buf.rollbackToCheckpoint();
        }
    }
    return false;
}

void ValuesBlockInputFormat::readValueOrParseSeparateExpression(IColumn & column, size_t column_idx)
{
    //bool rollback_on_exception = false;
    try
    {
        /// TODO remove before merge
        throw Exception("always use templates", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
        //const Block & header = getPort().getHeader();
        //header.getByPosition(column_idx).type->deserializeAsTextQuoted(column, buf, format_settings);
        //rollback_on_exception = true;

        //skipWhitespaceIfAny(buf);

        //assertDelimiterAfterValue(column_idx);
    }
    catch (const Exception & e)
    {
        //bool deduce_template = shouldDeduceNewTemplate(column_idx);
        //if (!format_settings.values.interpret_expressions && !(format_settings.values.deduce_templates_of_expressions && deduce_template))
        //    throw;

        /** The normal streaming parser could not parse the value.
          * Let's try to parse it with a SQL parser as a constant expression.
          * This is an exceptional case.
          */
        if (isParseError(e.code()))
        {
            //if (rollback_on_exception)
            //    column.popBack(1);

            buf.rollbackToCheckpoint();
            parseExpression(column, column_idx, true);// deduce_template);
        }
        else
            throw;
    }
}

void
ValuesBlockInputFormat::parseExpression(IColumn & column, size_t column_idx, bool deduce_template)
{
    const Block & header = getPort().getHeader();
    const IDataType & type = *header.getByPosition(column_idx).type;

    Expected expected;
    auto tokens = std::make_unique<Tokens>(buf.position(), buf.buffer().end());
    IParser::Pos token_iterator(*tokens);
    ASTPtr ast;

    bool near_the_end_of_buffer = true;
    bool can_peek = true;
    while (near_the_end_of_buffer)
    {
        bool parsed = parser.parse(token_iterator, ast, expected);
        buf.position() = const_cast<char *>(token_iterator->begin);
        parsed &= checkDelimiterAfterValue(column_idx);

        near_the_end_of_buffer = buf.available() < 256 && can_peek;
        if (near_the_end_of_buffer)
        {
            /// Rare case: possibly only some prefix of expression was parsed. Peek more data and try again
            can_peek = buf.peekNext();
            buf.rollbackToCheckpoint();
            tokens = std::make_unique<Tokens>(buf.position(), buf.buffer().end());
            token_iterator = IParser::Pos{*tokens};
        }
        else if (!parsed)
        {
            buf.rollbackToCheckpoint();
            throw Exception("Cannot parse expression of type " + type.getName() + " here: "
                            + String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position())),
                            ErrorCodes::SYNTAX_ERROR);
        }
    }

    if (format_settings.values.deduce_templates_of_expressions && deduce_template)
    {
        if (templates[column_idx])
            throw DB::Exception("Template for column " + std::to_string(column_idx) + " already exists and it was not evaluated yet",
                                ErrorCodes::LOGICAL_ERROR);
        try
        {
            templates[column_idx] = ConstantExpressionTemplate(header.getByPosition(column_idx).type, TokenIterator(*tokens), token_iterator, ast, *context);
            buf.rollbackToCheckpoint();
            templates[column_idx].value().parseExpression(buf, format_settings);
            assertDelimiterAfterValue(column_idx);
            return;
        }
        catch (...)
        {
            if (!format_settings.values.interpret_expressions)
                throw;
            /// Continue parsing without template
            templates[column_idx].reset();
        }
    }

    std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(ast, *context);
    Field value = convertFieldToType(value_raw.first, type, value_raw.second.get());

    /// Check that we are indeed allowed to insert a NULL.
    if (value.isNull() && !type.isNullable())
    {
        buf.rollbackToCheckpoint();
        throw Exception{"Expression returns value " + applyVisitor(FieldVisitorToString(), value)
                        + ", that is out of range of type " + type.getName()
                        + ", at: " +
                        String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position())),
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE};
    }

    assertDelimiterAfterValue(column_idx);
    column.insert(value);
}

void ValuesBlockInputFormat::assertDelimiterAfterValue(size_t column_idx)
{
    skipWhitespaceIfAny(buf);

    if (column_idx + 1 != num_columns)
        assertChar(',', buf);
    else
        assertChar(')', buf);
}

bool ValuesBlockInputFormat::checkDelimiterAfterValue(size_t column_idx)
{
    skipWhitespaceIfAny(buf);

    if (column_idx + 1 != num_columns)
        return checkChar(',', buf);
    else
        return checkChar(')', buf);
}

bool ValuesBlockInputFormat::shouldDeduceNewTemplate(size_t column_idx)
{
    /// TODO better heuristic
    constexpr size_t max_attempts = 3;
    constexpr size_t rows_per_attempt = 10;
    if (attempts_to_deduce_template[column_idx] < max_attempts)
        return true;
    if (rows_parsed_using_template[column_idx] / attempts_to_deduce_template[column_idx] > rows_per_attempt)
    {
        /// Try again
        attempts_to_deduce_template[column_idx] = 0;
        rows_parsed_using_template[column_idx] = 0;
        return true;
    }
    return false;
}


void registerInputFormatProcessorValues(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("Values", [](
        ReadBuffer & buf,
        const Block & header,
        const Context & context,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<ValuesBlockInputFormat>(buf, header, params, context, settings);
    });
}

}
