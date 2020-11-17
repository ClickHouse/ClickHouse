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
#include <common/find_symbols.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


ValuesBlockInputFormat::ValuesBlockInputFormat(ReadBuffer & in_, const Block & header_, const RowInputFormatParams & params_,
                                               const FormatSettings & format_settings_)
        : IInputFormat(header_, buf), buf(in_), params(params_),
          format_settings(format_settings_), num_columns(header_.columns()),
          parser_type_for_column(num_columns, ParserType::Streaming),
          attempts_to_deduce_template(num_columns), attempts_to_deduce_template_cached(num_columns),
          rows_parsed_using_template(num_columns), templates(num_columns), types(header_.getDataTypes())
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(buf);
}

Chunk ValuesBlockInputFormat::generate()
{
    const Block & header = getPort().getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    block_missing_values.clear();

    for (size_t rows_in_block = 0; rows_in_block < params.max_block_size; ++rows_in_block)
    {
        try
        {
            skipWhitespaceIfAny(buf);
            if (buf.eof() || *buf.position() == ';')
                break;
            readRow(columns, rows_in_block);
            if (params.callback)
                params.callback();
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
        if (!templates[i] || !templates[i]->rowsCount())
            continue;
        if (columns[i]->empty())
            columns[i] = IColumn::mutate(templates[i]->evaluateAll(block_missing_values, i));
        else
        {
            ColumnPtr evaluated = templates[i]->evaluateAll(block_missing_values, i, columns[i]->size());
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

void ValuesBlockInputFormat::readRow(MutableColumns & columns, size_t row_num)
{
    assertChar('(', buf);

    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        skipWhitespaceIfAny(buf);
        PeekableReadBufferCheckpoint checkpoint{buf};
        bool read;

        /// Parse value using fast streaming parser for literals and slow SQL parser for expressions.
        /// If there is SQL expression in some row, template of this expression will be deduced,
        /// so it makes possible to parse the following rows much faster
        /// if expressions in the following rows have the same structure
        if (parser_type_for_column[column_idx] == ParserType::Streaming)
            read = tryReadValue(*columns[column_idx], column_idx);
        else if (parser_type_for_column[column_idx] == ParserType::BatchTemplate)
            read = tryParseExpressionUsingTemplate(columns[column_idx], column_idx);
        else /// if (parser_type_for_column[column_idx] == ParserType::SingleExpressionEvaluation)
            read = parseExpression(*columns[column_idx], column_idx);

        if (!read)
            block_missing_values.setBit(column_idx, row_num);
        /// If read is true, value still may be missing. Bit mask for these values will be copied from ConstantExpressionTemplate later.
    }

    skipWhitespaceIfAny(buf);
    if (!buf.eof() && *buf.position() == ',')
        ++buf.position();

    ++total_rows;
}

bool ValuesBlockInputFormat::tryParseExpressionUsingTemplate(MutableColumnPtr & column, size_t column_idx)
{
    /// Try to parse expression using template if one was successfully deduced while parsing the first row
    auto settings = context->getSettingsRef();
    if (templates[column_idx]->parseExpression(buf, format_settings, settings))
    {
        ++rows_parsed_using_template[column_idx];
        return true;
    }

    /// Expression in the current row is not match template deduced on the first row.
    /// Evaluate expressions, which were parsed using this template.
    if (column->empty())
        column = IColumn::mutate(templates[column_idx]->evaluateAll(block_missing_values, column_idx));
    else
    {
        ColumnPtr evaluated = templates[column_idx]->evaluateAll(block_missing_values, column_idx, column->size());
        column->insertRangeFrom(*evaluated, 0, evaluated->size());
    }
    /// Do not use this template anymore
    templates[column_idx].reset();
    buf.rollbackToCheckpoint();

    /// It will deduce new template or fallback to slow SQL parser
    return parseExpression(*column, column_idx);
}

bool ValuesBlockInputFormat::tryReadValue(IColumn & column, size_t column_idx)
{
    bool rollback_on_exception = false;
    try
    {
        bool read = true;
        const auto & type = types[column_idx];
        if (format_settings.null_as_default && !type->isNullable())
            read = DataTypeNullable::deserializeTextQuoted(column, buf, format_settings, type);
        else
            type->deserializeAsTextQuoted(column, buf, format_settings);
        rollback_on_exception = true;

        skipWhitespaceIfAny(buf);
        assertDelimiterAfterValue(column_idx);
        return read;
    }
    catch (const Exception & e)
    {
        /// Do not consider decimal overflow as parse error to avoid attempts to parse it as expression with float literal
        bool decimal_overflow = e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND;
        if (!isParseError(e.code()) || decimal_overflow)
            throw;
        if (rollback_on_exception)
            column.popBack(1);

        /// Switch to SQL parser and don't try to use streaming parser for complex expressions
        /// Note: Throwing exceptions for each expression may be very slow because of stacktraces
        buf.rollbackToCheckpoint();
        return parseExpression(column, column_idx);
    }
}

bool ValuesBlockInputFormat::parseExpression(IColumn & column, size_t column_idx)
{
    const Block & header = getPort().getHeader();
    const IDataType & type = *header.getByPosition(column_idx).type;
    auto settings = context->getSettingsRef();

    /// We need continuous memory containing the expression to use Lexer
    skipToNextRow(0, 1);
    buf.makeContinuousMemoryFromCheckpointToPos();
    buf.rollbackToCheckpoint();

    Expected expected;
    Tokens tokens(buf.position(), buf.buffer().end());
    IParser::Pos token_iterator(tokens, settings.max_parser_depth);
    ASTPtr ast;

    bool parsed = parser.parse(token_iterator, ast, expected);

    /// Consider delimiter after value (',' or ')') as part of expression
    if (column_idx + 1 != num_columns)
        parsed &= token_iterator->type == TokenType::Comma;
    else
        parsed &= token_iterator->type == TokenType::ClosingRoundBracket;

    if (!parsed)
        throw Exception("Cannot parse expression of type " + type.getName() + " here: "
                        + String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position())),
                        ErrorCodes::SYNTAX_ERROR);
    ++token_iterator;

    if (parser_type_for_column[column_idx] != ParserType::Streaming && dynamic_cast<const ASTLiteral *>(ast.get()))
    {
        /// It's possible that streaming parsing has failed on some row (e.g. because of '+' sign before integer),
        /// but it still can parse the following rows
        /// Check if we can use fast streaming parser instead if using templates
        bool rollback_on_exception = false;
        bool ok = false;
        try
        {
            header.getByPosition(column_idx).type->deserializeAsTextQuoted(column, buf, format_settings);
            rollback_on_exception = true;
            skipWhitespaceIfAny(buf);
            if (checkDelimiterAfterValue(column_idx))
                ok = true;
        }
        catch (const Exception & e)
        {
            bool decimal_overflow = e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND;
            if (!isParseError(e.code()) || decimal_overflow)
                throw;
        }
        if (ok)
        {
            parser_type_for_column[column_idx] = ParserType::Streaming;
            return true;
        }
        else if (rollback_on_exception)
            column.popBack(1);
    }

    parser_type_for_column[column_idx] = ParserType::SingleExpressionEvaluation;

    /// Try to deduce template of expression and use it to parse the following rows
    if (shouldDeduceNewTemplate(column_idx))
    {
        if (templates[column_idx])
            throw DB::Exception("Template for column " + std::to_string(column_idx) + " already exists and it was not evaluated yet",
                                ErrorCodes::LOGICAL_ERROR);
        std::exception_ptr exception;
        try
        {
            bool found_in_cache = false;
            const auto & result_type = header.getByPosition(column_idx).type;
            const char * delimiter = (column_idx + 1 == num_columns) ? ")" : ",";
            auto structure = templates_cache.getFromCacheOrConstruct(result_type, format_settings.null_as_default,
                                                                     TokenIterator(tokens), token_iterator,
                                                                     ast, *context, &found_in_cache, delimiter);
            templates[column_idx].emplace(structure);
            if (found_in_cache)
                ++attempts_to_deduce_template_cached[column_idx];
            else
                ++attempts_to_deduce_template[column_idx];

            buf.rollbackToCheckpoint();
            if (templates[column_idx]->parseExpression(buf, format_settings, settings))
            {
                ++rows_parsed_using_template[column_idx];
                parser_type_for_column[column_idx] = ParserType::BatchTemplate;
                return true;
            }
        }
        catch (...)
        {
            exception = std::current_exception();
        }
        if (!format_settings.values.interpret_expressions)
        {
            if (exception)
                std::rethrow_exception(exception);
            else
            {
                buf.rollbackToCheckpoint();
                size_t len = const_cast<char *>(token_iterator->begin) - buf.position();
                throw Exception("Cannot deduce template of expression: " + std::string(buf.position(), len), ErrorCodes::SYNTAX_ERROR);
            }
        }
        /// Continue parsing without template
        templates[column_idx].reset();
    }

    if (!format_settings.values.interpret_expressions)
        throw Exception("Interpreting expressions is disabled", ErrorCodes::SUPPORT_IS_DISABLED);

    /// Try to evaluate single expression if other parsers don't work
    buf.position() = const_cast<char *>(token_iterator->begin);

    std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(ast, *context);
    Field value = convertFieldToType(value_raw.first, type, value_raw.second.get());

    /// Check that we are indeed allowed to insert a NULL.
    if (value.isNull() && !type.isNullable())
    {
        if (format_settings.null_as_default)
        {
            type.insertDefaultInto(column);
            return false;
        }
        buf.rollbackToCheckpoint();
        throw Exception{"Cannot insert NULL value into a column of type '" + type.getName() + "'"
                        + " at: " +
                        String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position())),
                        ErrorCodes::TYPE_MISMATCH};
    }

    column.insert(value);
    return true;
}

/// Can be used in fileSegmentationEngine for parallel parsing of Values
bool ValuesBlockInputFormat::skipToNextRow(size_t min_chunk_bytes, int balance)
{
    skipWhitespaceIfAny(buf);
    if (buf.eof() || *buf.position() == ';')
        return false;
    bool quoted = false;

    size_t chunk_begin_buf_count = buf.count();
    while (!buf.eof() && (balance || buf.count() - chunk_begin_buf_count < min_chunk_bytes))
    {
        buf.position() = find_first_symbols<'\\', '\'', ')', '('>(buf.position(), buf.buffer().end());
        if (buf.position() == buf.buffer().end())
            continue;
        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (!buf.eof())
                ++buf.position();
        }
        else if (*buf.position() == '\'')
        {
            quoted ^= true;
            ++buf.position();
        }
        else if (*buf.position() == ')')
        {
            ++buf.position();
            if (!quoted)
                --balance;
        }
        else if (*buf.position() == '(')
        {
            ++buf.position();
            if (!quoted)
                ++balance;
        }
    }

    if (!buf.eof() && *buf.position() == ',')
        ++buf.position();
    return true;
}

void ValuesBlockInputFormat::assertDelimiterAfterValue(size_t column_idx)
{
    if (unlikely(!checkDelimiterAfterValue(column_idx)))
        throwAtAssertionFailed((column_idx + 1 == num_columns) ? ")" : ",", buf);
}

bool ValuesBlockInputFormat::checkDelimiterAfterValue(size_t column_idx)
{
    skipWhitespaceIfAny(buf);

    if (likely(column_idx + 1 != num_columns))
        return checkChar(',', buf);
    else
        return checkChar(')', buf);
}

bool ValuesBlockInputFormat::shouldDeduceNewTemplate(size_t column_idx)
{
    if (!format_settings.values.deduce_templates_of_expressions)
        return false;

    /// TODO better heuristic

    /// Using template from cache is approx 2x faster, than evaluating single expression
    /// Construction of new template is approx 1.5x slower, than evaluating single expression
    float attempts_weighted = 1.5 * attempts_to_deduce_template[column_idx] +  0.5 * attempts_to_deduce_template_cached[column_idx];

    constexpr size_t max_attempts = 100;
    if (attempts_weighted < max_attempts)
        return true;

    if (rows_parsed_using_template[column_idx] / attempts_weighted > 1)
    {
        /// Try again
        attempts_to_deduce_template[column_idx] = 0;
        attempts_to_deduce_template_cached[column_idx] = 0;
        rows_parsed_using_template[column_idx] = 0;
        return true;
    }
    return false;
}

void ValuesBlockInputFormat::readSuffix()
{
    if (buf.hasUnreadData())
        throw Exception("Unread data in PeekableReadBuffer will be lost. Most likely it's a bug.", ErrorCodes::LOGICAL_ERROR);
}

void ValuesBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    // I'm not resetting parser modes here.
    // There is a good chance that all messages have the same format.
    buf.reset();
    total_rows = 0;
}

void registerInputFormatProcessorValues(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("Values", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<ValuesBlockInputFormat>(buf, header, params, settings);
    });
}

}
