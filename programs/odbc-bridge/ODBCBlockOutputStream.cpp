#include "ODBCBlockOutputStream.h"

#include <common/logger_useful.h>
#include <Core/Field.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>

namespace DB
{

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    std::string commaSeparateColumnNames(const ColumnsWithTypeAndName & columns)
    {
        std::string result = "(";
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i > 0)
                result += ",";
            result += columns[i].name;
        }
        return result + ")";
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
        return Poco::Dynamic::Var(); // Throw smth here?
    }
}

ODBCBlockOutputStream::ODBCBlockOutputStream(Poco::Data::Session && session_,
                                             const std::string & remote_database_name_,
                                             const std::string & remote_table_name_,
                                             const Block & sample_block_)
    : session(session_)
    , db_name(remote_database_name_)
    , table_name(remote_table_name_)
    , sample_block(sample_block_)
    , log(&Logger::get("ODBCBlockOutputStream"))
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
    Poco::Data::Statement statement(session << "INSERT INTO " + db_name + "." + table_name + " " +
                                               commaSeparateColumnNames(columns) +
                                               " VALUES " + getQuestionMarks(block.columns()));
    for (size_t i = 0; i < block.columns(); ++i)
        statement, Poco::Data::Keywords::use(row_to_insert[i]);

    for (size_t i = 0; i < block.rows(); ++i)
    {
        for (size_t col_idx = 0; col_idx < block.columns(); ++col_idx)
        {
            Field val;
            columns[col_idx].column->get(i, val);
            row_to_insert[col_idx] = getVarFromField(val, description.types[col_idx].first);
        }
        statement.execute();
    }
}

}