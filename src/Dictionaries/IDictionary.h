#pragma once


#include <Core/Names.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/StorageID.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/PODArray.h>
#include <common/StringRef.h>
#include "IDictionarySource.h"
#include <Dictionaries/DictionaryStructure.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnsNumber.h>

#include <chrono>
#include <memory>
#include <mutex>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct IDictionaryBase;
using DictionaryPtr = std::unique_ptr<IDictionaryBase>;

/** DictionaryKeyType provides IDictionary client information about
  * which key type is supported by dictionary.
  *
  * Simple is for dictionaries that support UInt64 key column.
  *
  * Complex is for dictionaries that support any combination of key columns.
  *
  * Range is for dictionary that support combination of UInt64 key column,
  * and numeric representable range key column.
  */
enum class DictionaryKeyType
{
    simple,
    complex,
    range
};

/**
 * Base class for Dictionaries implementation.
 */
struct IDictionaryBase : public IExternalLoadable
{
    using Key = UInt64;

    IDictionaryBase(const StorageID & dict_id_)
    : dict_id(dict_id_)
    , full_name(dict_id.getInternalDictionaryName())
    {
    }

    const std::string & getFullName() const{ return full_name; }
    StorageID getDictionaryID() const
    {
        std::lock_guard lock{name_mutex};
        return dict_id;
    }

    void updateDictionaryName(const StorageID & new_name) const
    {
        std::lock_guard lock{name_mutex};
        assert(new_name.uuid == dict_id.uuid && dict_id.uuid != UUIDHelpers::Nil);
        dict_id = new_name;
    }

    const std::string & getLoadableName() const override final { return getFullName(); }

    /// Specifies that no database is used.
    /// Sometimes we cannot simply use an empty string for that because an empty string is
    /// usually replaced with the current database.
    static constexpr char NO_DATABASE_TAG[] = "<no_database>";

    std::string getDatabaseOrNoDatabaseTag() const
    {
        if (!dict_id.database_name.empty())
            return dict_id.database_name;
        return NO_DATABASE_TAG;
    }

    virtual std::string getTypeName() const = 0;

    virtual size_t getBytesAllocated() const = 0;

    virtual size_t getQueryCount() const = 0;

    virtual double getHitRate() const = 0;

    virtual size_t getElementCount() const = 0;

    virtual double getLoadFactor() const = 0;

    virtual const IDictionarySource * getSource() const = 0;

    virtual const DictionaryStructure & getStructure() const = 0;

    virtual bool isInjective(const std::string & attribute_name) const = 0;

    /** Subclass must provide key type that is supported by dictionary.
      * Client will use that key type to provide valid key columns for `getColumn` and `has` functions.
      */
    virtual DictionaryKeyType getKeyType() const = 0;

    /** Subclass must validate key columns and keys types
      * and return column representation of dictionary attribute.
      *
      * Parameter default_values_column must be used to provide default values
      * for keys that are not in dictionary. If null pointer is passed,
      * then default attribute value must be used.
      */
    virtual ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr default_values_column) const = 0;

    /** Subclass must validate key columns and key types and return ColumnUInt8 that
      * is bitmask representation of is key in dictionary or not.
      * If key is in dictionary then value of associated row will be 1, otherwise 0.
      */
    virtual ColumnUInt8::Ptr hasKeys(
        const Columns & key_columns,
        const DataTypes & key_types) const = 0;

    virtual BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const = 0;

    bool supportUpdates() const override { return true; }

    bool isModified() const override
    {
        auto source = getSource();
        return source && source->isModified();
    }

    virtual std::exception_ptr getLastException() const { return {}; }

    std::shared_ptr<IDictionaryBase> shared_from_this()
    {
        return std::static_pointer_cast<IDictionaryBase>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const IDictionaryBase> shared_from_this() const
    {
        return std::static_pointer_cast<const IDictionaryBase>(IExternalLoadable::shared_from_this());
    }

private:
    mutable std::mutex name_mutex;
    mutable StorageID dict_id;

protected:
    const String full_name;
};

struct IDictionary : IDictionaryBase
{
    IDictionary(const StorageID & dict_id_) : IDictionaryBase(dict_id_) {}

    virtual bool hasHierarchy() const = 0;

    virtual void toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const = 0;

    /// TODO: Rewrite
    /// Methods for hierarchy.

    virtual void isInVectorVector(
        const PaddedPODArray<Key> & /*child_ids*/, const PaddedPODArray<Key> & /*ancestor_ids*/, PaddedPODArray<UInt8> & /*out*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Hierarchy is not supported for {} dictionary.", getDictionaryID().getNameForLogs());
    }

    virtual void
    isInVectorConstant(const PaddedPODArray<Key> & /*child_ids*/, const Key /*ancestor_id*/, PaddedPODArray<UInt8> & /*out*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Hierarchy is not supported for {} dictionary.", getDictionaryID().getNameForLogs());
    }

    virtual void
    isInConstantVector(const Key /*child_id*/, const PaddedPODArray<Key> & /*ancestor_ids*/, PaddedPODArray<UInt8> & /*out*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Hierarchy is not supported for {} dictionary.", getDictionaryID().getNameForLogs());
    }

    void isInConstantConstant(const Key child_id, const Key ancestor_id, UInt8 & out) const
    {
        PaddedPODArray<UInt8> out_arr(1);
        isInVectorConstant(PaddedPODArray<Key>(1, child_id), ancestor_id, out_arr);
        out = out_arr[0];
    }
};

}
