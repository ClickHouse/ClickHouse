#include <DataTypes/DeserializeBinaryBulkStateJSONB.h>
#include <IO/ReadHelpers.h>
#include <ext/scope_guard.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnUnique.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

DeserializeBinaryBulkStateJSONB::DeserializeBinaryBulkStateJSONB(IDataType::DeserializeBinaryBulkSettings & settings)
{
    UInt64 serialize_version;
    SCOPE_EXIT({ settings.path.pop_back(); });
    settings.path.push_back(IDataType::Substream::JSONBinaryRelations);
    readVarUInt(serialize_version, *settings.getter(settings.path));
}

ColumnPtr DeserializeBinaryBulkStateJSONB::deserializeJSONKeysDictionaryColumn(ReadBuffer & istr) const
{
    UInt64 dictionary_size;
    readVarUInt(dictionary_size, istr);

    MutableColumnPtr dictionary_nested_column = ColumnString::create();
    dictionary_nested_column->reserve(dictionary_size);

    DataTypeString().deserializeBinaryBulk(*dictionary_nested_column, istr, dictionary_size, 0);
    return ColumnUnique<ColumnString>::create(std::move(dictionary_nested_column), false);
}

ColumnPtr DeserializeBinaryBulkStateJSONB::deserializeJSONRelationsDictionaryColumn(ReadBuffer & istr) const
{
    UInt64 nested_offset_size, nested_data_size;
    readVarUInt(nested_offset_size, istr);
    readVarUInt(nested_data_size, istr);

    MutableColumnPtr dictionary_nested_column = ColumnArray::create(ColumnUInt64::create());

    if (auto nested_array_column = typeid_cast<ColumnArray *>(dictionary_nested_column.get()))
    {
        DataTypeUInt64().deserializeBinaryBulk(nested_array_column->getData(), istr, nested_data_size, 0);
        DataTypeNumber<ColumnArray::Offset>().deserializeBinaryBulk(nested_array_column->getOffsetsColumn(), istr, nested_offset_size, 0);
    }

    return ColumnUnique<ColumnArray>::create(std::move(dictionary_nested_column), false);
}

DeserializeBinaryBulkStateJSONB::JSONSerializeInfo DeserializeBinaryBulkStateJSONB::deserializeJSONSerializeInfo(ReadBuffer & istr) const
{
    UInt64 type_index, row_size, is_nullable, multiple_columns;

    readVarUInt(row_size, istr);
    readVarUInt(type_index, istr);
    readVarUInt(is_nullable, istr);
    readVarUInt(multiple_columns, istr);
    return std::make_tuple(type_index, row_size, is_nullable, multiple_columns);
}

using OffsetColumn = ColumnVector<ColumnArray::Offset>;
using OffsetDataType = DataTypeNumber<ColumnArray::Offset>;
DeserializeBinaryBulkStateJSONB::JSONRelationsInfo DeserializeBinaryBulkStateJSONB::deserializeRowRelationsColumn(ReadBuffer & istr) const
{
    std::tuple<UInt64, UInt64, bool, bool> serialize_info = deserializeJSONSerializeInfo(istr);

    MutableColumnPtr offset_column = OffsetDataType().createColumn();
    MutableColumnPtr relations_column = DataTypeUInt64().createColumn();
    OffsetDataType().deserializeBinaryBulk(*offset_column, istr, std::get<1>(serialize_info), 0);
    DataTypeUInt64().deserializeBinaryBulk(*relations_column, istr, checkAndGetColumn<OffsetColumn>(*offset_column)->getData().back(), 0);
    return std::make_tuple<UInt64, UInt64, bool, bool, ColumnPtr>(
        std::move(std::get<0>(serialize_info)), std::move(std::get<1>(serialize_info)), std::move(std::get<2>(serialize_info)),
        std::move(std::get<3>(serialize_info)), ColumnArray::create(std::move(relations_column), std::move(offset_column)));
}

std::vector<ColumnPtr> DeserializeBinaryBulkStateJSONB::deserializeJSONBinaryDataColumn(
    const DeserializeBinaryBulkStateJSONB::JSONRelationsInfo & info, IDataType::DeserializeBinaryBulkSettings & settings) const
{
    if (!std::get<3>(info))
    {
        MutableColumnPtr binary_data_column = ColumnString::create();
        binary_data_column->reserve(std::get<1>(info));

        settings.path.back().type = IDataType::Substream::JSONBinaryMultipleData;
        DataTypeString().deserializeBinaryBulk(*binary_data_column, *settings.getter(settings.path), std::get<1>(info), 0);
        return std::vector<ColumnPtr>({std::get<4>(info), std::move(binary_data_column)});
    }

    return {};
}

}

