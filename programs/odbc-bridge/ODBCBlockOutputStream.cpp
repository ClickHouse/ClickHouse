#include "ODBCBlockOutputStream.h"

#include <common/logger_useful.h>
#include <Core/Field.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include "getIdentifierQuote.h"


namespace DB
{

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    std::string getInsertQuery(const std::string & db_name, const std::string & table_name, const ColumnsWithTypeAndName & columns, IdentifierQuotingStyle quoting)
    {
        ASTInsertQuery query;
        query.table_id.database_name = db_name;
        query.table_id.table_name = table_name;
        query.columns = std::make_shared<ASTExpressionList>(',');
        query.children.push_back(query.columns);
        for (const auto & column : columns)
            query.columns->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

        std::stringstream ss;
        IAST::FormatSettings settings(ss, true);
        settings.always_quote_identifiers = true;
        settings.identifier_quoting_style = quoting;
        query.IAST::format(settings);
        return ss.str();
    }

    std::string getQuestionMarks(size_t n)
    {
        std::string result = "(";
        for (size_t i = 0; i < n; ++i)
        {
            if (i > 0)
                result += ",";
            result += "?";
        }
        return result + ")";
    }

    Poco::Dynamic::Var getVarFromField(const Field & field, const ValueType type)
    {
        switch (type)
        {
            case ValueType::vtUInt8:
                return Poco::Dynamic::Var(static_cast<UInt64>(field.get<UInt64>())).convert<UInt64>();
            case ValueType::vtUInt16:
                return Poco::Dynamic::Var(static_cast<UInt64>(field.get<UInt64>())).convert<UInt64>();
            case ValueType::vtUInt32:
                return Poco::Dynamic::Var(static_cast<UInt64>(field.get<UInt64>())).convert<UInt64>();
            case ValueType::vtUInt64:
                return Poco::Dynamic::Var(field.get<UInt64>()).convert<UInt64>();
            case ValueType::vtInt8:
                return Poco::Dynamic::Var(static_cast<Int64>(field.get<Int64>())).convert<Int64>();
            case ValueType::vtInt16:
                return Poco::Dynamic::Var(static_cast<Int64>(field.get<Int64>())).convert<Int64>();
            case ValueType::vtInt32:
                return Poco::Dynamic::Var(static_cast<Int64>(field.get<Int64>())).convert<Int64>();
            case ValueType::vtInt64:
                return Poco::Dynamic::Var(field.get<Int64>()).convert<Int64>();
            case ValueType::vtFloat32:
                return Poco::Dynamic::Var(field.get<Float64>()).convert<Float64>();
            case ValueType::vtFloat64:
                return Poco::Dynamic::Var(field.get<Float64>()).convert<Float64>();
            case ValueType::vtString:
                return Poco::Dynamic::Var(field.get<String>()).convert<String>();
            case ValueType::vtDate:
                return Poco::Dynamic::Var(LocalDate(DayNum(field.get<UInt64>())).toString()).convert<String>();
            case ValueType::vtDateTime:
                return Poco::Dynamic::Var(std::to_string(LocalDateTime(time_t(field.get<UInt64>())))).convert<String>();
            case ValueType::vtUUID:
                return Poco::Dynamic::Var(UUID(field.get<UInt128>()).toUnderType().toHexString()).convert<std::string>();
        }
        __builtin_unreachable();
    }
}

ODBCBlockOutputStream::ODBCBlockOutputStream(Poco::Data::Session && session_,
                                             const std::string & remote_database_name_,
                                             const std::string & remote_table_name_,
                                             const Block & sample_block_,
                                             IdentifierQuotingStyle quoting_)
    : session(session_)
    , db_name(remote_database_name_)
    , table_name(remote_table_name_)
    , sample_block(sample_block_)
    , quoting(quoting_)
    , log(&Poco::Logger::get("ODBCBlockOutputStream"))
{
    description.init(sample_block);
}

Block ODBCBlockOutputStream::getHeader() const
{
    return sample_block;
}

void ODBCBlockOutputStream::write(const Block & block)
{
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < block.columns(); ++i)
        columns.push_back({block.getColumns()[i], sample_block.getDataTypes()[i], sample_block.getNames()[i]});

    std::vector<Poco::Dynamic::Var> row_to_insert(block.columns());
    Poco::Data::Statement statement(session << getInsertQuery(db_name, table_name, columns, quoting) + getQuestionMarks(block.columns()));
    for (size_t i = 0; i < block.columns(); ++i)
        statement.addBind(Poco::Data::Keywords::use(row_to_insert[i]));

    for (size_t i = 0; i < block.rows(); ++i)
    {
        for (size_t col_idx = 0; col_idx < block.columns(); ++col_idx)
        {
            Field val;
            columns[col_idx].column->get(i, val);
            if (val.isNull())
                row_to_insert[col_idx] = Poco::Dynamic::Var();
            else
                row_to_insert[col_idx] = getVarFromField(val, description.types[col_idx].first);
        }
        statement.execute();
    }
}

}
