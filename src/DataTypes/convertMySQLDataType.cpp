#include "config.h"
#if USE_MYSQL

#if __has_include(<mysql.h>)
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif

// NB: swapping mysql.h include with unit's header breaks the build because of enum forward declaration in mysqlxx/Types.h
// See another example of such issue in mysqlxx/Row.cpp
#include "convertMySQLDataType.h"

#include <Core/Field.h>
#include <base/types.h>
#include <Core/MultiEnum.h>
#include <Core/SettingsEnums.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include "DataTypeDate.h"
#include "DataTypeDate32.h"
#include "DataTypeDateTime.h"
#include "DataTypeDateTime64.h"
#include "DataTypesDecimal.h"
#include "DataTypeFixedString.h"
#include "DataTypeNullable.h"
#include "DataTypeString.h"
#include "DataTypeNothing.h"
#include "DataTypesNumber.h"
#include "DataTypeCustomGeo.h"
#include "DataTypeFactory.h"
#include "IDataType.h"
#include <Common/logger_useful.h>

namespace DB
{

DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support,
        const std::string & mysql_data_type,
        bool is_nullable,
        bool is_unsigned,
        size_t length,
        size_t precision,
        size_t scale)
{
    // Mysql returns mysql_data_type as below:
    // 1. basic_type
    // 2. basic_type options
    // 3. type_with_params(param1, param2, ...)
    // 4. type_with_params(param1, param2, ...) options
    // The options can be unsigned, zerofill, or some other strings.
    auto data_type = std::string_view(mysql_data_type);
    const auto type_end_pos = data_type.find_first_of(R"(( )"); // FIXME: fix style-check script instead
    const auto type_name = data_type.substr(0, type_end_pos);

    DataTypePtr res;

    if (type_name == "tinyint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt8>();
        else
            res = std::make_shared<DataTypeInt8>();
    }
    else if (type_name == "smallint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt16>();
        else
            res = std::make_shared<DataTypeInt16>();
    }
    else if (type_name == "int" || type_name == "mediumint" || type_name == "integer")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt32>();
        else
            res = std::make_shared<DataTypeInt32>();
    }
    else if (type_name == "bigint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt64>();
        else
            res = std::make_shared<DataTypeInt64>();
    }
    else if (type_name == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (type_name == "double")
        res = std::make_shared<DataTypeFloat64>();
    else if (type_name == "date")
     {
        if (type_support.isSet(MySQLDataTypesSupport::DATE2DATE32))
            res = std::make_shared<DataTypeDate32>();
        else if (type_support.isSet(MySQLDataTypesSupport::DATE2STRING))
            res = std::make_shared<DataTypeString>();
        else
            res = std::make_shared<DataTypeDate>();
    }
    else if (type_name == "binary")
    {
        //compatible with binary(0) DataType
        if (length == 0) length = 1;
        res = std::make_shared<DataTypeFixedString>(length);
    }
    else if (type_name == "datetime" || type_name == "timestamp")
    {
        if (!type_support.isSet(MySQLDataTypesSupport::DATETIME64))
        {
            res = std::make_shared<DataTypeDateTime>();
        }
        else if (type_name == "timestamp" && scale == 0)
        {
            res = std::make_shared<DataTypeDateTime>();
        }
        else if (type_name == "datetime" || type_name == "timestamp")
        {
            res = std::make_shared<DataTypeDateTime64>(scale);
        }
    }
    else if (type_name == "bit")
    {
        res = std::make_shared<DataTypeUInt64>();
    }
    else if (type_support.isSet(MySQLDataTypesSupport::DECIMAL) && (type_name == "numeric" || type_name == "decimal"))
    {
        if (precision <=  DataTypeDecimalBase<Decimal32>::maxPrecision())
            res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        else if (precision <= DataTypeDecimalBase<Decimal64>::maxPrecision())
            res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        else if (precision <= DataTypeDecimalBase<Decimal128>::maxPrecision())
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        else if (precision <= DataTypeDecimalBase<Decimal256>::maxPrecision())
            res = std::make_shared<DataTypeDecimal<Decimal256>>(precision, scale);
    }
    else if (type_name == "point")
    {
        res = DataTypeFactory::instance().get("Point");
    }

    /// Also String is fallback for all unknown types.
    if (!res)
        res = std::make_shared<DataTypeString>();

    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);

    return res;
}

DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support, MYSQL_FIELD & field)
{
    bool is_nullable = !(field.flags & NOT_NULL_FLAG);
    bool is_unsigned = (field.flags & UNSIGNED_FLAG);

    DataTypePtr data_type;

    switch (field.type)
    {
        case enum_field_types::MYSQL_TYPE_TINY:
            if (is_unsigned)
                data_type = std::make_shared<DataTypeUInt8>();
            else
                data_type = std::make_shared<DataTypeInt8>();
            break;
        case enum_field_types::MYSQL_TYPE_SHORT:
            if (is_unsigned)
                data_type = std::make_shared<DataTypeUInt16>();
            else
                data_type = std::make_shared<DataTypeInt16>();
            break;
        case enum_field_types::MYSQL_TYPE_INT24: // Treat int24 as int32
        case enum_field_types::MYSQL_TYPE_LONG:
            if (is_unsigned)
                data_type = std::make_shared<DataTypeUInt32>();
            else
                data_type = std::make_shared<DataTypeInt32>();
            break;
        case enum_field_types::MYSQL_TYPE_LONGLONG:
            if (is_unsigned)
                data_type = std::make_shared<DataTypeUInt64>();
            else
                data_type = std::make_shared<DataTypeInt64>();
            break;

        case enum_field_types::MYSQL_TYPE_DECIMAL:
        case enum_field_types::MYSQL_TYPE_NEWDECIMAL:
            if (type_support.isSet(MySQLDataTypesSupport::DECIMAL))
            {
                // MySQL DECIMAL includes space for sign and decimal point in length
                auto precision = field.length;
                if (field.decimals > 0)
                    precision -= 1; // Decimal point
                if (!(field.flags & UNSIGNED_FLAG))
                    precision -= 1; // Sign

                data_type = createDecimal<DataTypeDecimal>(precision, field.decimals);
            }
            break;

        case enum_field_types::MYSQL_TYPE_FLOAT:
            data_type = std::make_shared<DataTypeFloat32>();
            break;
        case enum_field_types::MYSQL_TYPE_DOUBLE:
            data_type = std::make_shared<DataTypeFloat64>();
            break;

        case enum_field_types::MYSQL_TYPE_BIT:
            data_type = std::make_shared<DataTypeUInt64>();
            break;

        case enum_field_types::MYSQL_TYPE_DATETIME: // Process DATETIME and TIMESTAMP as in string->DataType cast
        case enum_field_types::MYSQL_TYPE_TIMESTAMP:
            if (!type_support.isSet(MySQLDataTypesSupport::DATETIME64))
            {
                data_type = std::make_shared<DataTypeDateTime>();
            }
            else if (field.type == MYSQL_TYPE_TIMESTAMP && field.decimals == 0) // scale == field.decimals
            {
                data_type = std::make_shared<DataTypeDateTime>();
            }
            data_type = std::make_shared<DataTypeDateTime64>(field.decimals);
            break;

        case enum_field_types::MYSQL_TYPE_DATE:
        case enum_field_types::MYSQL_TYPE_NEWDATE:
            if (type_support.isSet(MySQLDataTypesSupport::DATE2DATE32))
                data_type = std::make_shared<DataTypeDate32>();
            else if (type_support.isSet(MySQLDataTypesSupport::DATE2STRING))
                data_type = std::make_shared<DataTypeString>();
            else
                data_type = std::make_shared<DataTypeDate>();
            break;

        case enum_field_types::MYSQL_TYPE_TIME:
            data_type = std::make_shared<DataTypeString>(); // ClickHouse doesn't have a TIME type
            break;
        case enum_field_types::MYSQL_TYPE_YEAR:
            data_type = std::make_shared<DataTypeString>(); // ClickHouse doesn't have a YEAR type
            break;

        case enum_field_types::MYSQL_TYPE_GEOMETRY:
            data_type = DataTypeFactory::instance().get("Point");
            break;

        case enum_field_types::MYSQL_TYPE_NULL:
            data_type = std::make_shared<DataTypeNothing>();
            break;

        case enum_field_types::MYSQL_TYPE_STRING:
        case enum_field_types::MYSQL_TYPE_VAR_STRING:
        case enum_field_types::MYSQL_TYPE_BLOB:
        case enum_field_types::MYSQL_TYPE_VARCHAR:
            data_type = std::make_shared<DataTypeString>();
            break;
        default:
            // For unknown types, fallback to String
            data_type = std::make_shared<DataTypeString>();
    }

    // If type conversion conditions weren't met (e.g. DECIMAL), fallback to String
    if (!data_type)
    {
        data_type = std::make_shared<DataTypeString>();
    }

    if (is_nullable && !data_type->isNullable())
        data_type = makeNullable(data_type);
    return data_type;
}

}

#endif
