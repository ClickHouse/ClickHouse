#pragma once

#include <Core/Field.h>
#include <common/StringRef.h>
#include <Core/Names.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/PODArray.h>
#include <memory>
#include <chrono>

namespace DB
{

class IDictionarySource;

struct IDictionaryBase;
using DictionaryPtr = std::unique_ptr<IDictionaryBase>;

struct DictionaryLifetime;
struct DictionaryStructure;
class ColumnString;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;


struct IDictionaryBase : public std::enable_shared_from_this<IDictionaryBase>
{
    using Key = UInt64;

    virtual std::exception_ptr getCreationException() const = 0;

    virtual std::string getName() const = 0;

    virtual std::string getTypeName() const = 0;

    virtual std::size_t getBytesAllocated() const = 0;

    virtual std::size_t getQueryCount() const = 0;

    virtual double getHitRate() const = 0;

    virtual std::size_t getElementCount() const = 0;

    virtual double getLoadFactor() const = 0;

    virtual bool isCached() const = 0;
    virtual DictionaryPtr clone() const = 0;

    virtual const IDictionarySource * getSource() const = 0;

    virtual const DictionaryLifetime & getLifetime() const = 0;

    virtual const DictionaryStructure & getStructure() const = 0;

    virtual std::chrono::time_point<std::chrono::system_clock> getCreationTime() const = 0;

    virtual bool isInjective(const std::string & attribute_name) const = 0;

    virtual BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const = 0;

    virtual ~IDictionaryBase() = default;
};


struct IDictionary : IDictionaryBase
{
    virtual bool hasHierarchy() const = 0;

    virtual void toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const = 0;

    virtual void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const = 0;

    /// Methods for hierarchy.

    virtual void isInVectorVector(const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
    {
        throw Exception("Hierarchy is not supported for " + getName() + " dictionary.", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const
    {
        throw Exception("Hierarchy is not supported for " + getName() + " dictionary.", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
    {
        throw Exception("Hierarchy is not supported for " + getName() + " dictionary.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void isInConstantConstant(const Key child_id, const Key ancestor_id, UInt8 & out) const
    {
        PaddedPODArray<UInt8> out_arr(1);
        isInVectorConstant(PaddedPODArray<Key>(1, child_id), ancestor_id, out_arr);
        out = out_arr[0];
    }
};

}
