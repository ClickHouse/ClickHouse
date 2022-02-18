#include <iostream>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>

using namespace DB;


void deserializeTextHiveText(const String & type_name, const String & str)
{
    auto data_type = DataTypeFactory::instance().get(type_name);
    auto col = data_type->createColumn();
    ReadBuffer buffer(const_cast<char *>(str.data()), str.size(), 0);
    data_type->getDefaultSerialization()->deserializeTextHiveText(*col, buffer, HiveTextRowInputFormat::updateFormatSettings({}));
    std::cout << "type:" << type_name << ", str:" << str << ", value:" << toString((*col)[0]) << std::endl;
}

int main()
{
    deserializeTextHiveText("Map(String, String)", "a\003b\002c\003d");
    deserializeTextHiveText("Array(String)", "a\002b");
    deserializeTextHiveText("Tuple(a String, b Int64)", "a\003aa\002b\0031000");
    deserializeTextHiveText("Tuple(String, Int64)", "a\0021000");
    return 0;
}
