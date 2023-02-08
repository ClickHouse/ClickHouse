#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/TokenIterator.h>
#include <Processors/Formats/Impl/SQLiteInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Core/Block.h>
#include <base/find_symbols.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/ObjectUtils.h>


namespace DB
{


SQLiteInputFormat::SQLiteInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    const RowInputFormatParams & params_,
    const FormatSettings & format_settings_)
    : IInputFormat(header_, in_), params(params_), format_settings(format_settings_)
{
}

void SQLiteInputFormat::resetParser()
{
    IInputFormat::resetParser();
}

void SQLiteInputFormat::setReadBuffer(ReadBuffer & in_)
{
    IInputFormat::setReadBuffer(in_);
}

Chunk SQLiteInputFormat::generate()
{
    Chunk chunk;
    return chunk;
}


void registerInputFormatSQLite(FormatFactory & factory)
{
    factory.registerInputFormat("SQLiteInputFormat", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params_,
        const FormatSettings & settings)
    {
        return std::make_shared<SQLiteInputFormat>(buf, header, params_,settings);
    });
}


}
