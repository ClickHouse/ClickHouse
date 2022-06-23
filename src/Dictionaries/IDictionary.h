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

struct IDictionary;
using DictionaryPtr = std::unique_ptr<IDictionary>;

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
struct IDictionary : public IExternalLoadable
{
    explicit IDictionary(const StorageID & dictionary_id_)
    : dictionary_id(dictionary_id_)
    , full_name(dictionary_id.getInternalDictionaryName())
    {
    }

    const std::string & getFullName() const{ return full_name; }
    StorageID getDictionaryID() const
    {
        std::lock_guard lock{name_mutex};
        return dictionary_id;
    }

    void updateDictionaryName(const StorageID & new_name) const
    {
        std::lock_guard lock{name_mutex};
        assert(new_name.uuid == dictionary_id.uuid && dictionary_id.uuid != UUIDHelpers::Nil);
        dictionary_id = new_name;
    }

    const std::string & getLoadableName() const override final { return getFullName(); }

    /// Specifies that no database is used.
    /// Sometimes we cannot simply use an empty string for that because an empty string is
    /// usually replaced with the current database.
    static constexpr char NO_DATABASE_TAG[] = "<no_database>";

    std::string getDatabaseOrNoDatabaseTag() const
    {
        if (!dictionary_id.database_name.empty())
            return dictionary_id.database_name;

        return NO_DATABASE_TAG;
    }

    virtual std::string getTypeName() const = 0;

    virtual size_t getBytesAllocated() const = 0;

    virtual size_t getQueryCount() const = 0;

    virtual double getFoundRate() const = 0;

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
        const ColumnPtr & default_values_column) const = 0;

    /** Get multiple columns from dictionary.
      *
      * Default implementation just calls getColumn multiple times.
      * Subclasses can provide custom more efficient implementation.
      */
    virtual Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        const Columns & default_values_columns) const
    {
        size_t attribute_names_size = attribute_names.size();

        Columns result;
        result.reserve(attribute_names_size);

        for (size_t i = 0; i < attribute_names_size; ++i)
        {
            const auto & attribute_name = attribute_names[i];
            const auto & result_type = result_types[i];
            const auto & default_values_column = default_values_columns[i];

            result.emplace_back(getColumn(attribute_name, result_type, key_columns, key_types, default_values_column));
        }

        return result;
    }

    /** Subclass must validate key columns and key types and return ColumnUInt8 that
      * is bitmask representation of is key in dictionary or not.
      * If key is in dictionary then value of associated row will be 1, otherwise 0.
      */
    virtual ColumnUInt8::Ptr hasKeys(
        const Columns & key_columns,
        const DataTypes & key_types) const = 0;

    virtual bool hasHierarchy() const { return false; }

    virtual ColumnPtr getHierarchy(
        ColumnPtr key_column [[maybe_unused]],
        const DataTypePtr & key_type [[maybe_unused]]) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method getHierarchy is not supported for {} dictionary.",
                        getDictionaryID().getNameForLogs());
    }

    virtual ColumnUInt8::Ptr isInHierarchy(
        ColumnPtr key_column [[maybe_unused]],
        ColumnPtr in_key_column [[maybe_unused]],
        const DataTypePtr & key_type [[maybe_unused]]) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method isInHierarchy is not supported for {} dictionary.",
                        getDictionaryID().getNameForLogs());
    }

    virtual ColumnPtr getDescendants(
        ColumnPtr key_column [[maybe_unused]],
        const DataTypePtr & key_type [[maybe_unused]],
        size_t level [[maybe_unused]]) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method getDescendants is not supported for {} dictionary.",
                        getDictionaryID().getNameForLogs());
    }

    virtual BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const = 0;

    bool supportUpdates() const override { return true; }

    bool isModified() const override
    {
        const auto * source = getSource();
        return source && source->isModified();
    }

    virtual std::exception_ptr getLastException() const { return {}; }

    std::shared_ptr<IDictionary> shared_from_this()
    {
        return std::static_pointer_cast<IDictionary>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const IDictionary> shared_from_this() const
    {
        return std::static_pointer_cast<const IDictionary>(IExternalLoadable::shared_from_this());
    }

private:
    mutable std::mutex name_mutex;
    mutable StorageID dictionary_id;

protected:
    const String full_name;
};

}
