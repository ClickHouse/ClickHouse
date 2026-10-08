#pragma once

#include <memory>
#include <utility>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/types.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace Paimon
{
using namespace DB;
enum class RootDataType
{
    CHAR,

    VARCHAR,

    BOOLEAN,

    BINARY,

    VARBINARY,

    DECIMAL,

    TINYINT,

    SMALLINT,

    INTEGER,

    BIGINT,

    FLOAT,

    DOUBLE,

    DATE,

    TIME_WITHOUT_TIME_ZONE,

    TIMESTAMP_WITHOUT_TIME_ZONE,

    TIMESTAMP_WITH_LOCAL_TIME_ZONE,

    VARIANT,

    ARRAY,

    MULTISET,

    MAP,

    ROW,
};

struct DataType
{
    RootDataType root_type;
    String raw_type;
    DataTypePtr clickhouse_data_type;
    static DataType parse(const Poco::JSON::Object::Ptr & json_object, const String & key)
    {
        DataType type;
        type.raw_type = json_object->getValue<String>(key);
        LOG_TEST(&Poco::Logger::get("DataType::parse"), "raw_type: {}", type.raw_type);
        auto check_and_remove_nullable = [](const String & type_)
        {
            String result(type_);
            bool nullable{true};
            String suffix(" NOT NULL");
            auto start_index = type_.find(suffix);
            if (start_index != String::npos)
            {
                nullable = false;
                result.replace(start_index, suffix.length(), "");
            }
            return std::make_pair(nullable, result);
        };
        if (!json_object->isObject(key))
        {
            auto has_precision = [](const String & type_)
            {
                size_t start = type_.find('(');
                size_t end = type_.find(')');
                return !(start == std::string::npos || end == std::string::npos || start >= end);
            };

            auto parse_precision = [has_precision](const String & type_)
            {
                size_t start = type_.find('(');
                size_t end = type_.find(')');
                if (!has_precision(type_))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "not found precision: {}", type_);
                }
                std::vector<size_t> precision_infos;
                String precision_str = type_.substr(start + 1, end - start - 1);
                const char * token_start = nullptr;
                const char * p = precision_str.c_str();
                const char * end_pos = precision_str.c_str() + precision_str.size();
                for (; p != end_pos; ++p)
                {
                    LOG_TEST(
                        &Poco::Logger::get("parse_precision"),
                        "size: {} isdigit {} is, {} is blank {}",
                        precision_str.size(),
                        std::isdigit(*p),
                        (*p != ','),
                        (*p != ' '));
                    if (!std::isdigit(*p) && *p != ',' && *p != ' ')
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "parse precision meet invalid char: {}, precision_str: {}", *p, precision_str);
                    }
                    if (!token_start && std::isdigit(*p))
                    {
                        token_start = p;
                    }
                    if (*p == ' ' || *p == ',')
                    {
                        if (token_start && p > token_start)
                        {
                            precision_infos.emplace_back(std::stoul(String(token_start, p - token_start)));
                            LOG_TEST(&Poco::Logger::get("parse_precision"), "precision_infos back {}", precision_infos.back());
                            token_start = nullptr;
                        }
                    }
                }
                if (token_start && p > token_start)
                {
                    precision_infos.emplace_back(std::stoul(String(token_start, p - token_start)));
                }
                return precision_infos;
            };

            // check nullable
            auto result = check_and_remove_nullable(type.raw_type);
            bool nullable = result.first;
            const String & real_type = result.second;

            if (real_type == "BOOLEAN")
            {
                type.root_type = RootDataType::BOOLEAN;
                type.clickhouse_data_type = std::make_shared<DataTypeInt8>();
            }
            else if (real_type == "STRING" || real_type.starts_with("VARCHAR"))
            {
                type.root_type = RootDataType::VARCHAR;
                type.clickhouse_data_type = std::make_shared<DataTypeString>();
            }
            else if (real_type == "BYTES" || real_type.starts_with("VARBINARY"))
            {
                type.root_type = RootDataType::VARBINARY;
                type.clickhouse_data_type = std::make_shared<DataTypeString>();
            }
            else if (real_type == "TINYINT")
            {
                type.root_type = RootDataType::TINYINT;
                type.clickhouse_data_type = std::make_shared<DataTypeInt8>();
            }
            else if (real_type == "SMALLINT")
            {
                type.root_type = RootDataType::SMALLINT;
                type.clickhouse_data_type = std::make_shared<DataTypeInt16>();
            }
            else if (real_type == "INT")
            {
                type.root_type = RootDataType::INTEGER;
                type.clickhouse_data_type = std::make_shared<DataTypeInt32>();
            }
            else if (real_type == "BIGINT")
            {
                type.root_type = RootDataType::BIGINT;
                type.clickhouse_data_type = std::make_shared<DataTypeInt64>();
            }
            else if (real_type == "FLOAT")
            {
                type.root_type = RootDataType::FLOAT;
                type.clickhouse_data_type = std::make_shared<DataTypeFloat32>();
            }
            else if (real_type == "DOUBLE")
            {
                type.root_type = RootDataType::DOUBLE;
                type.clickhouse_data_type = std::make_shared<DataTypeFloat64>();
            }
            else if (real_type == "DATE")
            {
                type.root_type = RootDataType::DATE;
                type.clickhouse_data_type = std::make_shared<DataTypeDate>();
            }
            else if (real_type.starts_with("TIME") && !real_type.starts_with("TIMESTAMP"))
            {
                type.root_type = RootDataType::TIME_WITHOUT_TIME_ZONE;
                type.clickhouse_data_type = std::make_shared<DataTypeInt64>();
            }
            else if (real_type.starts_with("TIMESTAMP"))
            {
                type.root_type = real_type.contains("LOCAL TIME ZONE") ? RootDataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE
                                                                       : RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE;
                size_t p = has_precision(type.raw_type) ? parse_precision(type.raw_type)[0] : 6;
                String time_zone_string = type.root_type == RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE ? "UTC" : "";
                type.clickhouse_data_type = std::make_shared<DataTypeDateTime64>(p, time_zone_string);
            }
            else if (real_type.starts_with("CHAR"))
            {
                size_t n = has_precision(type.raw_type) ? parse_precision(type.raw_type)[0] : 1;
                type.clickhouse_data_type = std::make_shared<DataTypeFixedString>(n);
                type.root_type = RootDataType::CHAR;
            }
            else if (real_type.starts_with("BINARY"))
            {
                type.root_type = RootDataType::BINARY;
                size_t n = has_precision(real_type) ? parse_precision(real_type)[0] : 1;
                type.clickhouse_data_type = std::make_shared<DataTypeFixedString>(n);
            }
            else if (real_type.starts_with("DECIMAL"))
            {
                std::vector<size_t> n = has_precision(real_type) ? parse_precision(real_type) : std::vector<size_t>{10, 0};
                if (n.size() != 2)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "DECIMAL precision info is invaliad, precision info size: {}", n.size());
                }
                type.root_type = RootDataType::DECIMAL;
                auto precision = static_cast<Int32>(n[0]);
                auto scale = static_cast<Int32>(n[1]);
                type.clickhouse_data_type = createDecimal<DataTypeDecimal>(precision, scale);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type: {}", real_type);
            }
            if (nullable)
            {
                type.clickhouse_data_type = std::make_shared<DataTypeNullable>(type.clickhouse_data_type);
            }
            return type;
        }
        else
        {
            auto inner_json_object = json_object->getObject(key);
            String inner_type = inner_json_object->getValue<String>("type");
            // check nullable
            auto result = check_and_remove_nullable(inner_type);
            bool nullable = result.first;
            const String & real_type = result.second;
            if (real_type == "ARRAY")
            {
                type.root_type = RootDataType::ARRAY;
                auto nested_type = parse(inner_json_object, "element");
                type.clickhouse_data_type = std::make_shared<DataTypeArray>(nested_type.clickhouse_data_type);
                if (nullable)
                {
                    type.clickhouse_data_type = std::make_shared<DataTypeNullable>(type.clickhouse_data_type);
                }
            }
            else if (real_type == "MAP")
            {
                type.root_type = RootDataType::MAP;
                auto key_type = parse(inner_json_object, "key");
                auto value_type = parse(inner_json_object, "value");
                type.clickhouse_data_type = std::make_shared<DataTypeMap>(key_type.clickhouse_data_type, value_type.clickhouse_data_type);
                if (nullable)
                {
                    type.clickhouse_data_type = std::make_shared<DataTypeNullable>(type.clickhouse_data_type);
                }
            }
            else
            {
                throw Exception();
            }
            return type;
        }
    }
};
}
