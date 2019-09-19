#pragma once

#include <Core/Types.h>
#include <common/StringRef.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnsNumber.h>
#include <IO/WriteHelpers.h>

namespace DB
{

class JSONStructAndDataColumn : public std::enable_shared_from_this<JSONStructAndDataColumn>
{
public:
    using JSONStructAndDataColumnPtr = typename std::shared_ptr<JSONStructAndDataColumn>;

    JSONStructAndDataColumn(IColumn * column_, const String & name_ = {}, const std::vector<StringRef> & access_path_ = {}, JSONStructAndDataColumn * parent_ = nullptr);

    JSONStructAndDataColumnPtr getParent();

    IColumn * getOrCreateMarkColumn();

    IColumn * getDataColumn(const DataTypePtr & type);

    IColumn * getOrCreateDataColumn(const DataTypePtr & type);

    JSONStructAndDataColumnPtr getChildren(const StringRef & name);

    JSONStructAndDataColumnPtr getOrCreateChildren(const StringRef & name);

    using DataCloneFunc = std::function<IColumn *(IColumn *, bool)>;
    JSONStructAndDataColumnPtr clone(IColumn * column, const DataCloneFunc & data_clone_function);

    JSONStructAndDataColumnPtr cloneChildren(JSONStructAndDataColumnPtr & cloning_to, const DataCloneFunc & data_clone_function);

    void serialize(WriteBuffer & ostr, IDataType::SerializeBinaryBulkSettings & settings,
        const std::function<void(const JSONStructAndDataColumn &, IDataType::SerializeBinaryBulkSettings &)> & data_serialize);

    void deserialize(ReadBuffer & istr, IDataType::DeserializeBinaryBulkSettings & settings,
        const std::function<void(JSONStructAndDataColumn &, size_t, IDataType::DeserializeBinaryBulkSettings &)> & data_deserialize);

public:
    using JSONStructWithStatePtrs = std::vector<JSONStructAndDataColumnPtr>;

    IColumn * column;

    String name;
    StringRefs access_path;
    JSONStructWithStatePtrs children;
    JSONStructAndDataColumn * parent;

    IColumn * mark_column = nullptr;
    std::vector<std::pair<DataTypePtr, IColumn *>> data_columns;
};



}
