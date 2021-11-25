#include "convertMySQLDataType.h"

#include <Core/Field.h>
#include <base/types.h>
#include <Core/MultiEnum.h>
#include <Core/SettingsEnums.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include "DataTypeDate.h"
#include "DataTypeDateTime.h"
#include "DataTypeDateTime64.h"
#include "DataTypeEnum.h"
#include "DataTypesDecimal.h"
#include "DataTypeFixedString.h"
#include "DataTypeNullable.h"
#include "DataTypeString.h"
#include "DataTypesNumber.h"
#include "IDataType.h"

namespace DB
{
ASTPtr dataTypeConvertToQuery(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);

    if (!which.isNullable())
        return std::make_shared<ASTIdentifier>(data_type->getName());

    return makeASTFunction("Nullable", dataTypeConvertToQuery(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));
}

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
    else if (type_name == "int" || type_name == "mediumint")
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
        res = std::make_shared<DataTypeDate>();
    else if (type_name == "binary")
        res = std::make_shared<DataTypeFixedString>(length);
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
    else if (type_support.isSet(MySQLDataTypesSupport::DECIMAL) && (type_name == "numeric" || type_name == "decimal"))
    {
        if (precision <= DecimalUtils::max_precision<Decimal32>)
            res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        else if (precision <= DecimalUtils::max_precision<Decimal64>) //-V547
            res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        else if (precision <= DecimalUtils::max_precision<Decimal128>) //-V547
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
    }

    /// Also String is fallback for all unknown types.
    if (!res)
        res = std::make_shared<DataTypeString>();

    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);

    return res;
}

}
