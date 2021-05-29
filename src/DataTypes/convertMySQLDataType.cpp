#include "convertMySQLDataType.h"

#include <Core/Field.h>
#include <Core/Types.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include "DataTypeDate.h"
#include "DataTypeDateTime.h"
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

DataTypePtr convertMySQLDataType(const std::string & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length)
{
    DataTypePtr res;
    if (mysql_data_type == "tinyint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt8>();
        else
            res = std::make_shared<DataTypeInt8>();
    }
    else if (mysql_data_type == "smallint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt16>();
        else
            res = std::make_shared<DataTypeInt16>();
    }
    else if (mysql_data_type == "int" || mysql_data_type == "mediumint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt32>();
        else
            res = std::make_shared<DataTypeInt32>();
    }
    else if (mysql_data_type == "bigint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt64>();
        else
            res = std::make_shared<DataTypeInt64>();
    }
    else if (mysql_data_type == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (mysql_data_type == "double")
        res = std::make_shared<DataTypeFloat64>();
    else if (mysql_data_type == "date")
        res = std::make_shared<DataTypeDate>();
    else if (mysql_data_type == "datetime" || mysql_data_type == "timestamp")
        res = std::make_shared<DataTypeDateTime>();
    else if (mysql_data_type == "binary")
        res = std::make_shared<DataTypeFixedString>(length);
    else
        /// Also String is fallback for all unknown types.
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);
    return res;
}

}
