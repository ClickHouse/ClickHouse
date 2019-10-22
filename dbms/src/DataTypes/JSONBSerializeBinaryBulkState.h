#pragma once

#include <map>
#include <ext/scope_guard.h>

#include <IO/ReadHelpers.h>
#include <Columns/ColumnJSONB.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

struct JSONBSerializeBinaryBulkState : public IDataType::SerializeBinaryBulkState
{
    JSONBSerializeBinaryBulkState(IDataType::SerializeBinaryBulkSettings & settings, const size_t & serialize_version)
    {
        SCOPE_EXIT({settings.path.pop_back();});
        settings.path.push_back(IDataType::Substream::JSONBinaryRelations);
        writeVarUInt(serialize_version, *settings.getter(settings.path));
    }

    void serializeJSONSerializeInfo(const ColumnJSONB & serialize_binary_column, WriteBuffer & ostr, size_t row_size) const
    {
        writeVarUInt(row_size, ostr);
        writeVarUInt(UInt64(TypeIndex::UInt64), ostr);
        writeVarUInt(UInt64(serialize_binary_column.isNullable()), ostr);
        writeVarUInt(UInt64(serialize_binary_column.isMultipleColumn()), ostr);
    }

    void serializeJSONDictionaryColumn(const IColumnUnique & dictionary_column, WriteBuffer & ostr) const
    {
        writeVarUInt(dictionary_column.getNestedColumn()->size(), ostr);

        if (const auto string_nested_column = checkAndGetColumn<ColumnString>(*dictionary_column.getNestedColumn()))
            DataTypeString().serializeBinaryBulk(*string_nested_column, ostr, 0, 0);
        else if (const auto array_nested_column = checkAndGetColumn<ColumnArray>(*dictionary_column.getNestedColumn()))
        {
            writeVarUInt(array_nested_column->getData().size(), ostr);
            DataTypeUInt64().serializeBinaryBulk(array_nested_column->getData(), ostr, 0, 0);
            DataTypeNumber<ColumnArray::Offset>().serializeBinaryBulk(array_nested_column->getOffsetsColumn(), ostr, 0, 0);
        }
    }

    using OffsetDataType = DataTypeNumber<ColumnArray::Offset>;
    void serializeRowRelationsColumn(const ColumnJSONB &serialize_column, WriteBuffer &ostr, size_t offset, size_t row_size) const
    {
        serializeJSONSerializeInfo(serialize_column, ostr, row_size);
        const auto & relations_binary = checkAndGetColumn<ColumnArray>(serialize_column.getRelationsBinary());
        OffsetDataType().serializeBinaryBulk(relations_binary->getOffsetsColumn(), ostr, offset, row_size);
        DataTypeUInt64().serializeBinaryBulk(relations_binary->getData(), ostr, relations_binary->getOffsets()[offset - 1],
            relations_binary->getOffsets()[offset + row_size - 1] - relations_binary->getOffsets()[offset - 1]);
    }

    void serializeJSONBinaryMultipleColumn(
        const ColumnJSONB & serialize_binary_column, IDataType::SerializeBinaryBulkSettings & settings, size_t offset, size_t limit) const
    {
        if (!serialize_binary_column.isMultipleColumn())
            DataTypeString().serializeBinaryBulk(serialize_binary_column.getDataBinary(), *settings.getter(settings.path), offset, limit);
        /// TODO: 否则, 数据已经被分割到多个列中, 需要对应的序列化
    }

    static const JSONBSerializeBinaryBulkState * check(IDataType::SerializeBinaryBulkStatePtr & state)
    {
        if (!state)
            throw Exception("Got empty state for DataTypeJSONB.", ErrorCodes::LOGICAL_ERROR);

        auto * checked_serialize_state = typeid_cast<JSONBSerializeBinaryBulkState *>(state.get());
        if (!checked_serialize_state)
        {
            auto & state_ref = *state;
            throw Exception(
                "Invalid SerializeBinaryBulkState for DataTypeJSONB. Expected: " + demangle(typeid(JSONBSerializeBinaryBulkState).name()) +
                ", got " + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
        }

        return checked_serialize_state;
    }

//    IDataType::SerializeBinaryBulkStatePtr getOrCreateDataState(WriteBuffer * ostr, const String & state_name, const DataTypePtr & type)
//    {
//        auto state_iterator = states.find(state_name);
//
//        if (state_iterator != states.end())
//            return state_iterator->second;
//
//        IDataType::SerializeBinaryBulkSettings settings;
//        settings.getter = [&](const IDataType::SubstreamPath &) {return ostr; };
//
//        IDataType::SerializeBinaryBulkStatePtr state = states[state_name];
//        type->serializeBinaryBulkStatePrefix(settings, state);
//        return state;
//
//    }
//
//    IDataType::SerializeBinaryBulkStatePtr getOrCreateDataState(WriteBuffer * ostr, const StringRefs & /*data_column_name*/, const DataTypePtr & data_column_type)
//    {
//        const String & type_name = data_column_type->getName();
//
//        writeBinary(type_name, *ostr);
//        if (!data_column_type->haveSubtypes())
//            return IDataType::SerializeBinaryBulkStatePtr{};
//
//        return getOrCreateDataState(ostr, type_name, data_column_type);
//    }
};

}
