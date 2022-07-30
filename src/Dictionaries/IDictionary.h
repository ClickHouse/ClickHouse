#pragma once

#include <memory>
#include <mutex>

#include <Core/Names.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/castColumn.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <DataTypes/IDataType.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
}

class IDictionary;
using DictionaryPtr = std::unique_ptr<IDictionary>;

class DictionaryHierarchicalParentToChildIndex;
using DictionaryHierarchicalParentToChildIndexPtr = std::shared_ptr<DictionaryHierarchicalParentToChildIndex>;

/** DictionaryKeyType provides IDictionary client information about
  * which key type is supported by dictionary.
  *
  * Simple is for dictionaries that support UInt64 key column.
  *
  * Complex is for dictionaries that support any combination of key columns.
  */
enum class DictionaryKeyType
{
    Simple,
    Complex
};

/** DictionarySpecialKeyType provides IDictionary client information about
  * which special key type is supported by dictionary.
  */
enum class DictionarySpecialKeyType
{
    None,
    Range
};

/**
 * Base class for Dictionaries implementation.
 */
class IDictionary : public IExternalLoadable
{
public:
    explicit IDictionary(const StorageID & dictionary_id_)
    : dictionary_id(dictionary_id_)
    {
    }

    std::string getFullName() const
    {
        std::lock_guard lock{mutex};
        return dictionary_id.getNameForLogs();
    }

    StorageID getDictionaryID() const
    {
        std::lock_guard lock{mutex};
        return dictionary_id;
    }

    void updateDictionaryName(const StorageID & new_name) const
    {
        std::lock_guard lock{mutex};
        assert(new_name.uuid == dictionary_id.uuid && dictionary_id.uuid != UUIDHelpers::Nil);
        dictionary_id = new_name;
    }

    std::string getLoadableName() const final
    {
        std::lock_guard lock{mutex};
        return dictionary_id.getInternalDictionaryName();
    }

    /// Specifies that no database is used.
    /// Sometimes we cannot simply use an empty string for that because an empty string is
    /// usually replaced with the current database.
    static constexpr char NO_DATABASE_TAG[] = "<no_database>";

    std::string getDatabaseOrNoDatabaseTag() const
    {
        std::lock_guard lock{mutex};

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

    virtual DictionarySourcePtr getSource() const = 0;

    virtual const DictionaryStructure & getStructure() const = 0;

    virtual bool isInjective(const std::string & attribute_name) const = 0;

    /** Subclass must provide key type that is supported by dictionary.
      * Client will use that key type to provide valid key columns for `getColumn` and `hasKeys` functions.
      */
    virtual DictionaryKeyType getKeyType() const = 0;

    virtual DictionarySpecialKeyType getSpecialKeyType() const { return DictionarySpecialKeyType::None; }

    /** Convert key columns for getColumn, hasKeys functions to match dictionary key column types.
      * Default implementation convert key columns into DictionaryStructure key types.
      * Subclass can override this method if keys for getColumn, hasKeys functions
      * are different from DictionaryStructure keys. Or to prevent implicit conversion of key columns.
      */
    virtual void convertKeyColumns(Columns & key_columns, DataTypes & key_types) const
    {
        const auto & dictionary_structure = getStructure();
        auto key_attributes_types = dictionary_structure.getKeyTypes();
        size_t key_attributes_types_size = key_attributes_types.size();
        size_t key_types_size = key_types.size();

        if (key_types_size != key_attributes_types_size)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} key lookup structure does not match, expected {}",
                getFullName(),
                dictionary_structure.getKeyDescription());

        for (size_t key_attribute_type_index = 0; key_attribute_type_index < key_attributes_types_size; ++key_attribute_type_index)
        {
            const auto & key_attribute_type = key_attributes_types[key_attribute_type_index];
            auto & key_type = key_types[key_attribute_type_index];

            if (key_attribute_type->equals(*key_type))
                continue;

            auto & key_column_to_cast = key_columns[key_attribute_type_index];
            ColumnWithTypeAndName column_to_cast = {key_column_to_cast, key_type, ""};
            auto casted_column = castColumnAccurate(column_to_cast, key_attribute_type);
            key_column_to_cast = std::move(casted_column);
            key_type = key_attribute_type;
        }
    }

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

    virtual DictionaryHierarchicalParentToChildIndexPtr getHierarchicalIndex() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method getHierarchicalIndex is not supported for {} dictionary.",
                        getDictionaryID().getNameForLogs());
    }

    virtual size_t getHierarchicalIndexBytesAllocated() const
    {
        return 0;
    }

    virtual ColumnPtr getDescendants(
        ColumnPtr key_column [[maybe_unused]],
        const DataTypePtr & key_type [[maybe_unused]],
        size_t level [[maybe_unused]],
        DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index [[maybe_unused]]) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method getDescendants is not supported for {} dictionary.",
                        getDictionaryID().getNameForLogs());
    }

    virtual Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const = 0;

    bool supportUpdates() const override { return true; }

    bool isModified() const override
    {
        const auto source = getSource();
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

    void setDictionaryComment(String new_comment)
    {
        std::lock_guard lock{mutex};
        dictionary_comment = std::move(new_comment);
    }

    String getDictionaryComment() const
    {
        std::lock_guard lock{mutex};
        return dictionary_comment;
    }

private:
    mutable std::mutex mutex;
    mutable StorageID dictionary_id TSA_GUARDED_BY(mutex);
    String dictionary_comment TSA_GUARDED_BY(mutex);
};

}
