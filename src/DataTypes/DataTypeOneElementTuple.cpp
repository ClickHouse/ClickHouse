#include <DataTypes/DataTypeOneElementTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/Serializations/SerializationTupleElement.h>
// #include <IO/WriteBufferFromString.h>
// #include <IO/Operators.h>
// #include <Common/quoteString.h>
// #include <Parsers/ASTNameTypePair.h>
// #include <Columns/IColumn.h>


namespace DB
{

// namespace
// {

// /** Custom substreams representation for single subcolumn.
//   * It serializes/deserializes column as a nested type, but in that way
//   * if it was a named tuple with one element and a given name.
//   */
// class DataTypeOneElementTupleStreams : public IDataTypeCustomStreams
// {
// private:
//     DataTypePtr nested;
//     String name;
//     bool escape_delimiter;

// public:
//     DataTypeOneElementTupleStreams(const DataTypePtr & nested_, const String & name_, bool escape_delimiter_)
//         : nested(nested_), name(name_), escape_delimiter(escape_delimiter_) {}

//     void enumerateStreams(
//         const IDataType::SubstreamCallback & callback,
//         ISerialization::SubstreamPath & path) const override
//     {
//         addToPath(path);
//         nested->enumerateStreams(callback, path);
//         path.pop_back();
//     }

//     void serializeBinaryBulkStatePrefix(
//         IDataType:: SerializeBinaryBulkSettings & settings,
//         ISerialization::SerializeBinaryBulkStatePtr & state) const override
//     {
//         addToPath(settings.path);
//         nested->serializeBinaryBulkStatePrefix(settings, state);
//         settings.path.pop_back();
//     }

//     void serializeBinaryBulkStateSuffix(
//         ISerialization::SerializeBinaryBulkSettings & settings,
//         ISerialization::SerializeBinaryBulkStatePtr & state) const override
//     {
//         addToPath(settings.path);
//         nested->serializeBinaryBulkStateSuffix(settings, state);
//         settings.path.pop_back();
//     }

//     void deserializeBinaryBulkStatePrefix(
//         ISerialization::DeserializeBinaryBulkSettings & settings,
//         ISerialization::DeserializeBinaryBulkStatePtr & state) const override
//     {
//         addToPath(settings.path);
//         nested->deserializeBinaryBulkStatePrefix(settings, state);
//         settings.path.pop_back();
//     }

//     void serializeBinaryBulkWithMultipleStreams(
//         const IColumn & column,
//         size_t offset,
//         size_t limit,
//         ISerialization::SerializeBinaryBulkSettings & settings,
//         ISerialization::SerializeBinaryBulkStatePtr & state) const override
//     {
//         addToPath(settings.path);
//         nested->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
//         settings.path.pop_back();
//     }

//     void deserializeBinaryBulkWithMultipleStreams(
//         ColumnPtr & column,
//         size_t limit,
//         ISerialization::DeserializeBinaryBulkSettings & settings,
//         ISerialization::DeserializeBinaryBulkStatePtr & state,
//         ISerialization::SubstreamsCache * cache) const override
//     {
//         addToPath(settings.path);
//         nested->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
//         settings.path.pop_back();
//     }

// private:
//     void addToPath(ISerialization::SubstreamPath & path) const
//     {
//         path.push_back(ISerialization::Substream::TupleElement);
//         path.back().tuple_element_name = name;
//         path.back().escape_tuple_delimiter = escape_delimiter;
//     }
// };

// }

DataTypePtr createOneElementTuple(const DataTypePtr & type, const String & name, bool escape_delimiter)
{
    auto custom_desc = std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypeCustomFixedName>(type->getName()),
        std::make_shared<SerializationTupleElement>(type->getDefaultSerialization(), name, escape_delimiter));

    return DataTypeFactory::instance().getCustom(std::move(custom_desc));
}

}
