#include "RegexpTreeDictionary.h"
#include <regex>
#include <Core/Defines.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <common/map.h>

#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Processors/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Dictionaries//DictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
    extern const int TYPE_MISMATCH;
}

static void validateKeyType(const DataTypes & key_types)
{
    if(key_types.empty() || key_types.size() > 1)
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a single user agent column as key column");
    }
    if(!isString(key_types[0]))
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a key column of Strings");
    }
}

RegexpTreeDictionary::RegexpTreeDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    Configuration configuration_,
    BlockPtr update_field_loaded_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , configuration(configuration_)
    , loaded_keys(configuration.initial_array_size, false)
    , update_field_loaded_block(std::move(update_field_loaded_block_))
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}

ColumnPtr RegexpTreeDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & ) const
{
    validateKeyType(key_types);
    
    ColumnPtr result;
    
    auto size = key_columns.front()->size();
    
    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    
    size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute = attributes[attribute_index];
    
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;
        
        auto column = ColumnProvider::getColumn(dictionary_attribute, size);
        
        if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();
            
            getItemsImpl<ValueType>(
		attribute_index,
                attribute,
                key_columns,
                [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); });
        }
        else
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected all attributes columns to be Strings");
        }
        
        result = std::move(column);
    };
    
    callOnDictionaryAttributeType(attribute.type, type_call);
    
    return result;
}

ColumnUInt8::Ptr RegexpTreeDictionary::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    dict_struct.validateKeyTypes(key_types);

    PaddedPODArray<UInt64> backup_storage;
    const auto & keys = getColumnVectorData(this, key_columns.front(), backup_storage);
    size_t keys_size = keys.size();
    
    auto result = ColumnUInt8::create(keys_size);
    auto & out = result->getData();
    
    size_t keys_found = 0;
    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        const auto key = keys[key_index];
        out[key_index] = key < loaded_keys.size() && loaded_keys[key];
        keys_found += out[key_index];
    }
    
    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

RegexpTreeDictionary::Attribute RegexpTreeDictionary::createAttribute(const DictionaryAttribute & dictionary_attribute)  
{
    Attribute attribute{dictionary_attribute.underlying_type, {}, {}, {}};
    
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
	using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        
	if constexpr (std::is_same_v<ValueType, StringRef>)
            attribute.string_arena = std::make_unique<Arena>();
        
	attribute.container.emplace<ContainerType<ValueType>>(configuration.initial_array_size, ValueType());
    };
    
    callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    
    return attribute;
}

void RegexpTreeDictionary::createAttributes()  
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
	if (attribute.is_nullable)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Array or nullable attributes not supported for dictionary of type {}",
                    getTypeName());

        if (attribute.hierarchical)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hierarchical attributes not supported by {} dictionary.",
                            getDictionaryID().getNameForLogs());
	
	attributes.push_back(createAttribute(attribute));
    }
}

template <typename ValueSetter>
void func(
	RegexpTreeNode node,
	String regexp_,
	bool isLast,
	size_t attribute_index,
	size_t rows,
	std::vector<StringRef> user_agents,
	ValueSetter && set_value,
	size_t keys_found)
{
    bool isRegexp = false;
    regexp_+=node.regexp;
    String attribute = node.attributes[attribute_index];
    String final_attribute;
    
    if(startsWith(attribute, "(") && endsWith(attribute, ")"))
    {
        isRegexp = true;
    }
    else if(attribute != "")
    {
        final_attribute = attribute;
    }
    
    int n = node.children.size();
    while(n > 0)
    {
        isLast = false;
        RegexpTreeNode child = node.children[n - 1];
        func(child, regexp_, true, attribute_index, rows, user_agents, set_value, keys_found);
        --n;
    }
    
    if(isLast)
    {
        for (const auto i : collections::range(0, rows))
        {
	// Here I want to use match and extractAllGroupsVertical function that can be used by user, but I didn't find where did they come from, so I just left them here for now.
            if(match(user_agents[i], regexp_) == 1)
            {
                if(isRegexp)
                {
                    final_attribute = std::regex_replace(attribute, "\\1", extractAllGroupsVertical(user_agents[i], node.regexp)[0][0]);
                }
                set_value(i, final_attribute);
                ++keys_found;
                break;
            }
        }
    }
    for(size_t i = 0; i < node.regexp.size(); ++i)
    {
	regexp_.pop_back();
    }
}

template <typename AttributeType, typename ValueSetter >
void RegexpTreeDictionary::getItemsImpl(
    const size_t attribute_index,
    const Attribute &,
    const Columns & key_columns,
    ValueSetter && set_value) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    
    //const auto & container = std::get<ContainerType<AttributeType>>(attribute.container);
    
    size_t keys_found = 0;
    std::vector<StringRef> user_agents;
    for(const auto i : collections::range(0, rows))
    {
        user_agents.push_back(first_column->getDataAt(i));
    }

    String s;
    for (RegexpTreeNode root : roots)
    {
        func(root, s, true, attribute_index, rows, user_agents, set_value, keys_found);
        s.clear();
    }
    query_count.fetch_add(rows, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed); 
}

void RegexpTreeDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());
    for (const auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;
            const auto & container = std::get<ContainerType<ValueType>>(attribute.container);
            bytes_allocated += sizeof(ContainerType<ValueType>);
            if constexpr (std::is_same_v<ValueType, Array>)
            {
                /// It is not accurate calculations
                bytes_allocated += sizeof(Array) * container.size();
            }
            else
            {
                bytes_allocated += container.allocated_bytes();
            }
            bucket_count = container.capacity();
            if constexpr (std::is_same_v<ValueType, StringRef>)
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
        };
        callOnDictionaryAttributeType(attribute.type, type_call);
    }
    if (update_field_loaded_block)
        bytes_allocated += update_field_loaded_block->allocatedBytes();
}


void RegexpTreeDictionary::setNodeValue(UInt64 id, UInt64 parent_id, String regexp, std::vector<String> attributes)
{
    std::vector<RegexpTreeNode> children;
    
    if(parent_id == 0)
    {
        RegexpTreeNode node(regexp, attributes, children);
        nodes.emplace(id, node);
	roots.push_back(node);
    }
    else
    {
        RegexpTreeNode parent = nodes.find(parent_id)->second;
        RegexpTreeNode node(regexp, attributes, &parent, children);
        parent.children.push_back(node);
        nodes.emplace(id, node);
    }
}

void RegexpTreeDictionary::loadData()
{
    QueryPipeline pipeline;
    pipeline.init(source_ptr->loadAll());
    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
    {
        const auto rows = block.rows();
	const ColumnPtr id_column_ptr = block.safeGetByPosition(0).column;
        const ColumnPtr parent_id_column_ptr = block.safeGetByPosition(1).column;
        const ColumnPtr regexp_column_ptr = block.safeGetByPosition(2).column;
        const auto attribute_column_ptrs = collections::map<Columns>(
            collections::range(3, dict_struct.attributes.size()),
            [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });        
       
        for (const auto row : collections::range(0, rows))
        {
            const auto & id = (*id_column_ptr)[row].get<UInt64>();
	    const auto & parent_id = (*parent_id_column_ptr)[row].get<UInt64>();
	    const auto & regexp = (*regexp_column_ptr)[row].get<String>();
	    std::vector<String> attrs;
            
            for (const auto attribute_idx : collections::range(0, dict_struct.attributes.size()))
            {
                const auto & attribute_column = *attribute_column_ptrs[attribute_idx];
                const auto & attribute = attribute_column[row].get<String>();
                attrs.push_back(attribute);
            }

            setNodeValue(id, parent_id, regexp, attrs);
        }
    }
}

Pipe RegexpTreeDictionary::read(const Names & /*column_names*/, size_t /*max_block_size*/) const
{
    return Pipe();
}

void registerDictionaryRegexpTree(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
			    const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            DictionarySourcePtr source_ptr,
                            ContextPtr /* global_context */,
                            bool /* created_from_ddl */) -> DictionaryPtr
    {
	//if (dict_struct.key)
            //throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'regexptree'");
        
	if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "{}: elements .structure.range_min and .structure.range_max should be defined only "
                            "for a dictionary of layout 'range_hashed'",
                            full_name);
        
	static constexpr size_t default_initial_array_size = 1024;
        static constexpr size_t default_max_array_size = 500000;
        
	String dictionary_layout_prefix = config_prefix + ".layout" + ".regexptree";
        
	RegexpTreeDictionary::Configuration configuration
        {
            .initial_array_size = config.getUInt64(dictionary_layout_prefix + ".initial_array_size", default_initial_array_size),
            .max_array_size = config.getUInt64(dictionary_layout_prefix + ".max_array_size", default_max_array_size),
            .require_nonempty = config.getBool(config_prefix + ".require_nonempty", false)
        };
        
	const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        
	return std::make_unique<RegexpTreeDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, std::move(configuration));
    };

    factory.registerLayout("regexptree", create_layout, true);
}

}

