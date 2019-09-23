#pragma once

#include <IO/ReadBuffer.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class DeserializeBinaryBulkStateJSONB : public IDataType::DeserializeBinaryBulkState
{
public:
    DeserializeBinaryBulkStateJSONB(IDataType::DeserializeBinaryBulkSettings & settings);

    ColumnPtr deserializeJSONKeysDictionaryColumn(ReadBuffer & istr) const;

    ColumnPtr deserializeJSONRelationsDictionaryColumn(ReadBuffer & istr) const;

    using JSONSerializeInfo = std::tuple<UInt64, UInt64, bool, bool>;
    JSONSerializeInfo deserializeJSONSerializeInfo(ReadBuffer & istr) const;

    using JSONRelationsInfo = std::tuple<UInt64, UInt64, bool, bool, ColumnPtr>;
    JSONRelationsInfo deserializeRowRelationsColumn(ReadBuffer & istr) const;

    Columns deserializeJSONBinaryDataColumn(const JSONRelationsInfo & info, IDataType::DeserializeBinaryBulkSettings & settings) const;

    static DeserializeBinaryBulkStateJSONB * check(IDataType::DeserializeBinaryBulkStatePtr & state)
    {
        if (!state)
            throw Exception("Got empty state for DataTypeJSONB.", ErrorCodes::LOGICAL_ERROR);

        auto * checked_deserialize_state = typeid_cast<DeserializeBinaryBulkStateJSONB *>(state.get());
        if (!checked_deserialize_state)
        {
            auto & state_ref = *state;
            throw Exception("Invalid DeserializeBinaryBulkState for DataTypeJSONB. Expected: "
                            + demangle(typeid(DeserializeBinaryBulkStateJSONB).name()) + ", got "
                            + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
        }

        return checked_deserialize_state;
    }
};

}
