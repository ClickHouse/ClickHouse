#include "parseKeyValue.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Common/assert_cast.h>
#include <Functions/keyvaluepair/KeyValuePairExtractorBuilder.h>

namespace DB
{

ParseKeyValue::ParseKeyValue()
: return_type(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()))
{
}

String ParseKeyValue::getName() const
{
    return name;
}

ColumnPtr ParseKeyValue::executeImpl([[maybe_unused]] const ColumnsWithTypeAndName & arguments, [[maybe_unused]] const DataTypePtr & result_type, [[maybe_unused]] size_t input_rows_count) const
{
    auto column = return_type->createColumn();
    [[maybe_unused]] auto * map_column = assert_cast<ColumnMap *>(column.get());

    auto data_column = arguments[0].column;

    auto item_delimiter = arguments[1].column->getDataAt(0).toView();
    auto key_value_pair_delimiter = arguments[2].column->getDataAt(0).toView();

    for (auto i = 0u; i < data_column->size(); i++)
    {
        auto row = data_column->getDataAt(i);

        auto extractor = KeyValuePairExtractorBuilder()
                             .withKeyValuePairDelimiter(key_value_pair_delimiter.front())
                             .withItemDelimiter(item_delimiter.front())
                             .build();

        auto response = extractor->extract(row.toString());
        for (auto & pair : response) {
            std::cout<<pair.first<<": "<<pair.second<<"\n";
        }
    }



    ColumnUInt64::MutablePtr offsets = ColumnUInt64::create();

    [[maybe_unused]] auto keys = ColumnString::create();
    [[maybe_unused]] auto values = ColumnString::create();

    return nullptr;
}

bool ParseKeyValue::isVariadic() const
{
    return true;
}

bool ParseKeyValue::isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const
{
    return false;
}

size_t ParseKeyValue::getNumberOfArguments() const
{
    return 0u;
}

DataTypePtr ParseKeyValue::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    return return_type;
}


REGISTER_FUNCTION(ParseKeyValue)
{
    factory.registerFunction<ParseKeyValue>();
}

}
