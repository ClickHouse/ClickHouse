#pragma once
#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization wrapper that acts like nested serialization,
/// but adds a passed name to the substream path like the
/// read column was the tuple element with this name.
/// It's used while reading subcolumns of complex types.
/// In particular while reading components of named tuples.
class SerializationNamed final : public SerializationWrapper
{
private:
    String name;
    SubstreamType substream_type;

    SerializationNamed(const SerializationPtr & nested_, const String & name_, SubstreamType substream_type_);

public:
    static SerializationPtr create(const SerializationPtr & nested_, const String & name_, SubstreamType substream_type_)
    {
        auto ptr = std::unique_ptr<ISerialization>(new SerializationNamed(nested_, name_, substream_type_));
        auto hash = ptr->getHash();
        return SerializationObjectPool::getOrCreate(hash, std::move(ptr));
    }

    UInt128 getHash() const override
    {
        SipHash hash;
        hash.update("Named");
        hash.update(nested_serialization->getHash());
        hash.update(name);
        hash.update(static_cast<int>(substream_type));
        return hash.get128();
    }

    const String & getElementName() const { return name; }

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

private:
    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const String name;
        SubstreamType substream_type;

        SubcolumnCreator(const String & name_, SubstreamType substream_type_)
            : name(name_), substream_type(substream_type_)
        {
        }

        DataTypePtr create(const DataTypePtr & prev) const override { return prev; }
        ColumnPtr create(const ColumnPtr & prev) const override { return prev; }
        SerializationPtr create(const SerializationPtr & prev, const DataTypePtr &) const override
        {
            return SerializationNamed::create(prev, name, substream_type);
        }
    };

    void addToPath(SubstreamPath & path) const;
};

}
