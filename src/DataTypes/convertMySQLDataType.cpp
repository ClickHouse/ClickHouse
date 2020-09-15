#include "convertMySQLDataType.h"

#include <Core/Field.h>
#include <common/types.h>
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
    // we expect mysql_data_type to be either "basic_type" or "type_with_params(param1, param2, ...)"
    auto data_type = std::string_view(mysql_data_type);
    const auto param_start_pos = data_type.find("(");
    const auto type_name = data_type.substr(0, param_start_pos);

    DataTypePtr res = [&]() -> DataTypePtr {
        if (type_name == "tinyint")
        {
            if (is_unsigned)
                return std::make_shared<DataTypeUInt8>();
            else
                return std::make_shared<DataTypeInt8>();
        }
        if (type_name == "smallint")
        {
            if (is_unsigned)
                return std::make_shared<DataTypeUInt16>();
            else
                return std::make_shared<DataTypeInt16>();
        }
        if (type_name == "int" || type_name == "mediumint")
        {
            if (is_unsigned)
                return std::make_shared<DataTypeUInt32>();
            else
                return std::make_shared<DataTypeInt32>();
        }
        if (type_name == "bigint")
        {
            if (is_unsigned)
                return std::make_shared<DataTypeUInt64>();
            else
                return std::make_shared<DataTypeInt64>();
        }
        if (type_name == "float")
            return std::make_shared<DataTypeFloat32>();
        if (type_name == "double")
            return std::make_shared<DataTypeFloat64>();
        if (type_name == "date")
            return std::make_shared<DataTypeDate>();
        if (type_name == "binary")
            return std::make_shared<DataTypeFixedString>(length);
        if (type_name == "datetime" || type_name == "timestamp")
        {
            if (!type_support.isSet(MySQLDataTypesSupport::DATETIME64))
                return std::make_shared<DataTypeDateTime>();

            if (type_name == "timestamp" && scale == 0)
            {
                return std::make_shared<DataTypeDateTime>();
            }
            else if (type_name == "datetime" || type_name == "timestamp")
            {
                return std::make_shared<DataTypeDateTime64>(scale);
            }
        }

        if (type_support.isSet(MySQLDataTypesSupport::DECIMAL) && (type_name == "numeric" || type_name == "decimal"))
        {
            if (precision <= DecimalUtils::maxPrecision<Decimal32>())
                return std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
            else if (precision <= DecimalUtils::maxPrecision<Decimal64>())
                return std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
            else if (precision <= DecimalUtils::maxPrecision<Decimal128>())
                return std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        }

        /// Also String is fallback for all unknown types.
        return std::make_shared<DataTypeString>();
    }();

    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);

    return res;
}

}
