#pragma once

#include <atomic>
#include <variant>
#include <vector>
#include <optional>

#include <Common/HashTable/HashSet.h>
#include <Common/Arena.h>
#include <DataTypes/IDataType.h>
#include <Core/Block.h>

#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryHelpers.h"


namespace DB
{

namespace 
{
    // Tree structure used in loading
    struct RegexpTreeNode
    {
        String regexp;
        std::vector<String> attributes;
        UInt64 parent;
        std::vector<RegexpTreeNode *> children;
        
        RegexpTreeNode(String regexp_, std::vector<String> attributes_, std::vector<RegexpTreeNode*> children_)
            : regexp(regexp_)
            , attributes(attributes_)
            , parent(0)
            , children(children_)
        {
        }
        
        RegexpTreeNode(String regexp_, std::vector<String> attributes_, UInt64 parent_, std::vector<RegexpTreeNode*> children_)
            : regexp(regexp_)
            , attributes(attributes_)
            , parent(parent_)
            , children(children_)
        {
        }

	RegexpTreeNode (const RegexpTreeNode& other)
	    : regexp(other.regexp)
	    , attributes(other.attributes)
            , parent(other.parent)
            , children(other.children)
	{
	}
    };
}

class RegexpTreeDictionary final : public IDictionary
{
public:
    struct Configuration
    {
	size_t initial_array_size;
	size_t max_array_size;
	bool require_nonempty;    
    };
    
    RegexpTreeDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        Configuration configuration_,
        BlockPtr update_field_loaded_block_ = nullptr);
        
    std::string getTypeName() const override { return "RegexpTree"; }
    
    size_t getBytesAllocated() const override { return bytes_allocated; }
    
    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }
    
    double getFoundRate() const override
    {
        size_t queries = query_count.load(std::memory_order_relaxed);
        if (!queries)
            return 0;
        return static_cast<double>(found_count.load(std::memory_order_relaxed)) / queries;
    }
    
    double getHitRate() const override { return 1.0; }
    
    size_t getElementCount() const override { return element_count; }
    
    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }
    
    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<RegexpTreeDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, configuration, update_field_loaded_block);
    }
    
    const IDictionarySource * getSource() const override { return source_ptr.get(); }
    
    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }
    
    const DictionaryStructure & getStructure() const override { return dict_struct; }
    
    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }
    
    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Complex; }
    
    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_value_extractor) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;
    
    bool hasHierarchy() const override { return dict_struct.hierarchical_attribute_index.has_value(); }
    
    Pipe read(const Names & column_names, size_t max_block_size) const override;
private:
    template <typename Value>
    using ContainerType = std::conditional_t<std::is_same_v<Value, Array>, std::vector<Value>, PaddedPODArray<Value>>;
    using NullableSet = HashSet<UInt64, DefaultHash<UInt64>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
	
	std::variant<
            ContainerType<UInt8>,
            ContainerType<UInt16>,
            ContainerType<UInt32>,
            ContainerType<UInt64>,
            ContainerType<UInt128>,
            ContainerType<UInt256>,
            ContainerType<Int8>,
            ContainerType<Int16>,
            ContainerType<Int32>,
            ContainerType<Int64>,
            ContainerType<Int128>,
            ContainerType<Int256>,
            ContainerType<Decimal32>,
            ContainerType<Decimal64>,
            ContainerType<Decimal128>,
            ContainerType<Decimal256>,
            ContainerType<Float32>,
            ContainerType<Float64>,
            ContainerType<UUID>,
            ContainerType<StringRef>,
            ContainerType<Array>>
            container;
        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();
    void loadData();
    void calculateBytesAllocated();
    
    Attribute createAttribute(const DictionaryAttribute & attribute);    
   
    template <typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
	RegexpTreeNode node, 
	String regexp_,
	bool isLast,
	size_t attribute_index,
	size_t rows,
	std::vector<StringRef> user_agents,
	ValueSetter && set_value,
	size_t kes_found,
	DefaultValueExtractor & default_value_extractor) const;

    void setNodeValue(UInt64 id, UInt64 parent_id, String regexp, std::vector<String> attributes);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const Configuration configuration;
    
    std::vector<Attribute> attributes;
    std::vector<bool> loaded_keys;
    std::map<UInt64, RegexpTreeNode> nodes;
    std::vector<UInt64> roots;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};
    
    BlockPtr update_field_loaded_block;
};

}
