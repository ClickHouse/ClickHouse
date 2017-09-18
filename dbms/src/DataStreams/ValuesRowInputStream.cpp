#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/ExpressionListParsers.h>
#include <DataStreams/ValuesRowInputStream.h>
#include <DataTypes/DataTypeArray.h>
#include <Core/FieldVisitors.h>
#include <Core/Block.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_DATE;
    extern const int SYNTAX_ERROR;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}


ValuesRowInputStream::ValuesRowInputStream(ReadBuffer & istr_, const Context & context_, bool interpret_expressions_)
    : istr(istr_), context(context_), interpret_expressions(interpret_expressions_)
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(istr);
}


bool ValuesRowInputStream::read(Block & block)
{
    size_t size = block.columns();

    skipWhitespaceIfAny(istr);

    if (istr.eof() || *istr.position() == ';')
        return false;

    /** Typically, this is the usual format for streaming parsing.
      * But as an exception, it also supports processing arbitrary expressions instead of values.
      * This is very inefficient. But if there are no expressions, then there is no overhead.
      */
    ParserExpression parser;

    assertChar('(', istr);

    for (size_t i = 0; i < size; ++i)
    {
        skipWhitespaceIfAny(istr);

        char * prev_istr_position = istr.position();
        size_t prev_istr_bytes = istr.count() - istr.offset();

        auto & col = block.getByPosition(i);

        bool rollback_on_exception = false;
        try
        {
            col.type->deserializeTextQuoted(*col.column, istr);
            rollback_on_exception = true;
            skipWhitespaceIfAny(istr);

            if (i != size - 1)
                assertChar(',', istr);
            else
                assertChar(')', istr);
        }
        catch (const Exception & e)
        {
            if (!interpret_expressions)
                throw;

            /** The normal streaming parser could not parse the value.
              * Let's try to parse it with a SQL parser as a constant expression.
              * This is an exceptional case.
              */
            if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
                || e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
                || e.code() == ErrorCodes::CANNOT_PARSE_DATE
                || e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
                || e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT)
            {
                /// TODO Case when the expression does not fit entirely in the buffer.

                /// If the beginning of the value is no longer in the buffer.
                if (istr.count() - istr.offset() != prev_istr_bytes)
                    throw;

                if (rollback_on_exception)
                    col.column->popBack(1);

                IDataType & type = *block.safeGetByPosition(i).type;

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

                col.column->insert(value);

                skipWhitespaceIfAny(istr);

                if (i != size - 1)
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

}
