#pragma once

#include <memory>
#include <mutex>

#include <Core/Names.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/JoinUtils.h>
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
enum class DictionaryKeyType : uint8_t
{
    Simple,
    Complex
};

/** DictionarySpecialKeyType provides IDictionary client information about
  * which special key type is supported by dictionary.
  */
enum class DictionarySpecialKeyType : uint8_t
{
    None,
    Range
};

/**
 * Base class for Dictionaries implementation.
 */
class IDictionary : public IExternalLoadable, public IKeyValueEntity
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

    /// Returns fully qualified unquoted dictionary name
    std::string getQualifiedName() const
    {
        std::lock_guard lock{mutex};
        if (dictionary_id.database_name.empty())
            return dictionary_id.table_name;
        return dictionary_id.database_name + "." + dictionary_id.table_name;
    }

    StorageID getDictionaryID() const
    {
        std::lock_guard lock{mutex};
        return dictionary_id;
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

    /// The percentage of time a lookup successfully found an entry.
    /// When there were no lookups, it returns zero (instead of NaN).
    /// The value is calculated non atomically and can be slightly off in the presence of concurrent lookups.
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
      * Parameter default_or_filter is std::variant type, so either a column of
      * default values is passed or a filter used in short circuit case, Filter's
      * 1 represents to be filled later on by lazy execution, and 0 means we have
      * the value right now.
      */
    using RefDefault = std::reference_wrapper<const ColumnPtr>;
    using RefFilter = std::reference_wrapper<IColumn::Filter>;
    using DefaultOrFilter = std::variant<RefDefault, RefFilter>;
    virtual ColumnPtr getColumn(
        const std::string & attribute_name [[maybe_unused]],
        const DataTypePtr & attribute_type [[maybe_unused]],
        const Columns & key_columns [[maybe_unused]],
        const DataTypes & key_types [[maybe_unused]],
        DefaultOrFilter default_or_filter [[maybe_unused]]) const = 0;

    /** Get multiple columns from dictionary.
      *
      * Default implementation just calls getColumn multiple times.
      * Subclasses can provide custom more efficient implementation.
      */
    using RefDefaults = std::reference_wrapper<const Columns>;
    using DefaultsOrFilter = std::variant<RefDefaults, RefFilter>;
    virtual Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & attribute_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultsOrFilter defaults_or_filter) const
    {
        bool is_short_circuit = std::holds_alternative<RefFilter>(defaults_or_filter);
        assert(is_short_circuit || std::holds_alternative<RefDefaults>(defaults_or_filter));

        size_t attribute_names_size = attribute_names.size();

        Columns result;
        result.reserve(attribute_names_size);

        for (size_t i = 0; i < attribute_names_size; ++i)
        {
            const auto & attribute_name = attribute_names[i];
            const auto & attribute_type = attribute_types[i];

            DefaultOrFilter var = is_short_circuit ? DefaultOrFilter{std::get<RefFilter>(defaults_or_filter).get()} :
                                                     std::get<RefDefaults>(defaults_or_filter).get()[i];
            result.emplace_back(getColumn(attribute_name, attribute_type, key_columns, key_types, var));
        }

        return result;
    }

    /**
     * Analogous to getColumn, but for dictGetAll
     */
    virtual ColumnPtr getColumnAllValues(
        const std::string & attribute_name [[maybe_unused]],
        const DataTypePtr & result_type [[maybe_unused]],
        const Columns & key_columns [[maybe_unused]],
        const DataTypes & key_types [[maybe_unused]],
        const ColumnPtr & default_values_column [[maybe_unused]],
        size_t limit [[maybe_unused]]) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method getColumnAllValues is not supported for {} dictionary.",
                        getDictionaryID().getNameForLogs());
    }

    /**
     * Analogous to getColumns, but for dictGetAll
     */
    virtual Columns getColumnsAllValues(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        const Columns & default_values_columns,
        size_t limit) const
    {
        size_t attribute_names_size = attribute_names.size();

        Columns result;
        result.reserve(attribute_names_size);

        for (size_t i = 0; i < attribute_names_size; ++i)
        {
            const auto & attribute_name = attribute_names[i];
            const auto & result_type = result_types[i];
            const auto & default_values_column = default_values_columns[i];

            result.emplace_back(getColumnAllValues(
                attribute_name, result_type, key_columns, key_types, default_values_column, limit));
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

    String getDictionaryComment() const
    {
        std::lock_guard lock{mutex};
        return dictionary_comment;
    }

    /// IKeyValueEntity implementation
    Names getPrimaryKey() const  override { return getStructure().getKeysNames(); }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & out_null_map, const Names & result_names) const override
    {
        if (keys.empty())
            return Chunk(getSampleBlock(result_names).cloneEmpty().getColumns(), 0);

        const auto & dictionary_structure = getStructure();

        /// Split column keys and types into separate vectors, to use in `IDictionary::getColumns`
        Columns key_columns;
        DataTypes key_types;
        for (const auto & key : keys)
        {
            key_columns.emplace_back(key.column);
            key_types.emplace_back(key.type);
        }

        /// Fill null map
        {
            out_null_map.clear();

            auto mask = hasKeys(key_columns, key_types);
            const auto & mask_data = mask->getData();

            out_null_map.resize(mask_data.size(), 0);
            std::copy(mask_data.begin(), mask_data.end(), out_null_map.begin());
        }

        Names attribute_names;
        DataTypes result_types;
        if (!result_names.empty())
        {
            for (const auto & attr_name : result_names)
            {
                if (!dictionary_structure.hasAttribute(attr_name))
                    continue; /// skip keys
                const auto & attr = dictionary_structure.getAttribute(attr_name);
                attribute_names.emplace_back(attr.name);
                result_types.emplace_back(attr.type);
            }
        }
        else
        {
            /// If result_names is empty, then use all attributes from dictionary_structure
            for (const auto & attr : dictionary_structure.attributes)
            {
                attribute_names.emplace_back(attr.name);
                result_types.emplace_back(attr.type);
            }
        }

        Columns default_cols(result_types.size());
        for (size_t i = 0; i < result_types.size(); ++i)
            /// Dictinonary may have non-standard default values specified
            default_cols[i] = result_types[i]->createColumnConstWithDefaultValue(out_null_map.size());

        Columns result_columns = getColumns(attribute_names, result_types, key_columns, key_types, default_cols);

        /// Result block should consist of key columns and then attributes
        for (const auto & key_col : key_columns)
        {
            /// Insert default values for keys that were not found
            ColumnPtr filtered_key_col = JoinCommon::filterWithBlanks(key_col, out_null_map);
            result_columns.insert(result_columns.begin(), filtered_key_col);
        }

        size_t num_rows = result_columns[0]->size();
        return Chunk(std::move(result_columns), num_rows);
    }

    Block getSampleBlock(const Names & result_names) const override
    {
        const auto & dictionary_structure = getStructure();
        const auto & key_types = dictionary_structure.getKeyTypes();
        const auto & key_names = dictionary_structure.getKeysNames();

        Block sample_block;

        for (size_t i = 0; i < key_types.size(); ++i)
            sample_block.insert(ColumnWithTypeAndName(nullptr, key_types.at(i), key_names.at(i)));

        if (result_names.empty())
        {
            for (const auto & attr : dictionary_structure.attributes)
                sample_block.insert(ColumnWithTypeAndName(nullptr, attr.type, attr.name));
        }
        else
        {
            for (const auto & attr_name : result_names)
            {
                if (!dictionary_structure.hasAttribute(attr_name))
                    continue; /// skip keys
                const auto & attr = dictionary_structure.getAttribute(attr_name);
                sample_block.insert(ColumnWithTypeAndName(nullptr, attr.type, attr_name));
            }
        }
        return sample_block;
    }

    /// Internally called by ExternalDictionariesLoader.
    /// In order to update the dictionary ID change its configuration first and then call ExternalDictionariesLoader::reloadConfig().
    void updateDictionaryID(const StorageID & new_dictionary_id)
    {
        std::lock_guard lock{mutex};
        assert((new_dictionary_id.uuid == dictionary_id.uuid) && (dictionary_id.uuid != UUIDHelpers::Nil));
        dictionary_id = new_dictionary_id;
    }

    /// Internally called by ExternalDictionariesLoader.
    /// In order to update the dictionary comment change its configuration first and then call ExternalDictionariesLoader::reloadConfig().
    void updateDictionaryComment(const String & new_dictionary_comment)
    {
        std::lock_guard lock{mutex};
        dictionary_comment = new_dictionary_comment;
    }

private:
    mutable std::mutex mutex;
    StorageID dictionary_id TSA_GUARDED_BY(mutex);
    String dictionary_comment TSA_GUARDED_BY(mutex);
};

}
