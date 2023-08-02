#pragma once

#include "config_formats.h"

#if USE_PROTOBUF
#   include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>


namespace google::protobuf { class Descriptor; }

namespace DB
{
class ProtobufReader;
class ProtobufWriter;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
class WriteBuffer;

/// Utility class, does all the work for serialization in the Protobuf format.
class ProtobufSerializer
{
public:
    virtual ~ProtobufSerializer() = default;

    virtual void setColumns(const ColumnPtr * columns, size_t num_columns) = 0;
    virtual void writeRow(size_t row_num) = 0;
    virtual void finalizeWrite() {}

    virtual void setColumns(const MutableColumnPtr * columns, size_t num_columns) = 0;
    virtual void readRow(size_t row_num) = 0;
    virtual void insertDefaults(size_t row_num) = 0;

    virtual void describeTree(WriteBuffer & out, size_t indent) const = 0;

    static std::unique_ptr<ProtobufSerializer> create(
        const Strings & column_names,
        const DataTypes & data_types,
        std::vector<size_t> & missing_column_indices,
        const google::protobuf::Descriptor & message_descriptor,
        bool with_length_delimiter,
        bool with_envelope,
        bool flatten_google_wrappers,
        ProtobufReader & reader);

    static std::unique_ptr<ProtobufSerializer> create(
        const Strings & column_names,
        const DataTypes & data_types,
        const google::protobuf::Descriptor & message_descriptor,
        bool with_length_delimiter,
        bool with_envelope,
        bool defaults_for_nullable_google_wrappers,
        ProtobufWriter & writer);
};

NamesAndTypesList protobufSchemaToCHSchema(const google::protobuf::Descriptor * message_descriptor, bool skip_unsupported_fields);

}
#endif
