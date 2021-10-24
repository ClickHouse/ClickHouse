#include "RegexpTreeDictionary.h"
#include <regex>
#include <Core/Defines.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <common/map.h>

#include <Functions/FunctionsStringSearch.h>
#include <Functions/MatchImpl.h>
#include <Functions/extractAllGroups.h>
#include <Functions/ReplaceRegexpImpl.h>
#include <Functions/FunctionStringReplace.h>


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

struct NameMatch
{
    static constexpr auto name = "match";
};

struct VerticalImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::VERTICAL;
    static constexpr auto Name = "extractAllGroupsVertical";
};

struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};

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
    const ColumnPtr & default_values_column) const
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

        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(dictionary_attribute.null_value, default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, size);
        
        if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();
            const auto first_column = key_columns.front();
            const auto rows = first_column->size();

            size_t keys_found = 0;

            std::vector<StringRef> user_agents;
            for(size_t i = 0; i < rows; i++)
            {
                user_agents.push_back(first_column->getDataAt(i));
            }

            String s;
            for (UInt64 index : roots)
            {
                RegexpTreeNode root = nodes.find(index)->second;
		getItemsImpl(root, s, true, attribute_index, rows, user_agents, [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); }, keys_found, default_value_extractor);
                s.clear();
            }
            query_count.fetch_add(rows, std::memory_order_relaxed);
            found_count.fetch_add(keys_found, std::memory_order_relaxed);
        }
        else
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected all attributes columns to be Strings.");
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
    Attribute attribute{dictionary_attribute.underlying_type, {}, {}};
    
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

template <typename ValueSetter, typename DefaultValueExtractor>
void RegexpTreeDictionary::getItemsImpl(
	RegexpTreeNode node,
	String regexp_,
	bool isLast,
	size_t attribute_index,
	size_t rows,
	std::vector<StringRef> user_agents,
	ValueSetter && set_value,
	size_t keys_found,
	DefaultValueExtractor & default_value_extractor) const
{
    bool isRegexp = false;
    regexp_+=node.regexp;
    String attribute = node.attributes[attribute_index - 2];
    String final_attribute;

    if(startsWith(attribute, "(") && endsWith(attribute, ")"))
    {
        isRegexp = true;
    }
    else if(attribute != "")
    {
        final_attribute = attribute;
    }
    
    size_t n = 0;
    while(n < node.children.size() )
    {
        isLast = false;
        RegexpTreeNode child = nodes.find(node.children[n]->parent)->second;
        getItemsImpl(child, regexp_, true, attribute_index, rows, user_agents, set_value, keys_found, default_value_extractor);
        ++n;
    }
    if(isLast)
    {
	bool isSet = false;
        for (const auto i : collections::range(0, rows))
        {
	    std::vector<ColumnWithTypeAndName> arguments;
	    auto string_type = std::make_shared<DataTypeString>();
	    auto result_type = std::make_shared<DataTypeNumber<UInt8>>();

	    auto match_first_column = string_type->createColumn();
	    match_first_column->insert(user_agents[i].toString());
	    arguments.push_back({std::move(match_first_column), std::move(string_type), "first"});

	    auto match_second_column = string_type->createColumn();
	    match_second_column->insert(regexp_);
	    ColumnPtr match_const_second_column = ColumnConst::create(std::move(match_second_column), 1);
	    arguments.push_back({match_const_second_column, std::move(string_type), "second"});

	    ColumnPtr match = FunctionsStringSearch<MatchImpl<NameMatch, false>>().executeImpl(arguments, std::move(result_type), rows);
	    size_t isMatched = match->get64(0);
            if(isMatched == 1)
            {
                if(isRegexp)
                {
		    arguments.clear();

		    auto ExtractAllGroupsVertical_first_column = string_type->createColumn();
		    ExtractAllGroupsVertical_first_column->insert(user_agents[i].toString());
		    arguments.push_back({std::move(ExtractAllGroupsVertical_first_column), std::move(string_type), "first"});

                    auto ExtractAllGroupsVertical_second_column = string_type->createColumn();
                    ExtractAllGroupsVertical_second_column->insert(node.regexp);
		    ColumnPtr ExtractAllGroupsVertical_const_second_column = ColumnConst::create(std::move(ExtractAllGroupsVertical_second_column), 1);
                    arguments.push_back({ExtractAllGroupsVertical_const_second_column, std::move(string_type), "second"});
		    
		    ContextPtr context;
		    ColumnPtr EAGV_result = FunctionExtractAllGroups<VerticalImpl>(context).executeImpl(arguments, std::move(string_type), 1);

		    arguments.clear();

		    auto replaceRegexpOne_first_column = string_type->createColumn();
		    replaceRegexpOne_first_column->insert(attribute);
		    arguments.push_back({std::move(replaceRegexpOne_first_column), std::move(string_type), "first"});

		    auto replaceRegexpOne_second_column = string_type->createColumn();
		    replaceRegexpOne_second_column->insert("\\\\1");
		    ColumnPtr replaceRegexpOne_const_second_column = ColumnConst::create(std::move(replaceRegexpOne_second_column), 1);
		    arguments.push_back({replaceRegexpOne_const_second_column, std::move(string_type), "second"});

		    auto replaceRegexpOne_third_column = string_type->createColumn();
		    replaceRegexpOne_third_column->insert(EAGV_result->getDataAt(0).toString());
		    ColumnPtr replaceRegexpOne_const_third_column = ColumnConst::create(std::move(replaceRegexpOne_third_column), 1);
		    arguments.push_back({replaceRegexpOne_const_third_column, std::move(string_type), "third"});

		    final_attribute = FunctionStringReplace<ReplaceRegexpImpl<true>, NameReplaceRegexpOne>().executeImpl(arguments, std::move(string_type), 1)->getDataAt(0).toString();
		    final_attribute = final_attribute.substr(1, final_attribute.size() - 2);
                }
                set_value(i, final_attribute);
                ++keys_found;
		isSet = true;
                break;
            }
        }
	if(!isSet)
            set_value(0,default_value_extractor[0]);

    }
    for(size_t i = 0; i < node.regexp.size(); ++i)
    {
	regexp_.pop_back();
    }
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
    std::vector<RegexpTreeNode *> children;
    if(parent_id == 0)
    {
        RegexpTreeNode node = {regexp, attributes, children};
        nodes.emplace(id, node);
        roots.push_back(id);
    }
    else
    {
	RegexpTreeNode *node_ptr = new RegexpTreeNode(regexp, attributes, id, children);
        (nodes.find(parent_id)->second).children.push_back(node_ptr);
	
        nodes.emplace(id, *node_ptr);
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
	const ColumnPtr regexp_column_ptr = block.safeGetByPosition(0).column;
	const ColumnPtr id_column_ptr = block.safeGetByPosition(1).column;
        const ColumnPtr parent_id_column_ptr = block.safeGetByPosition(2).column;
        
	std::vector<const ColumnPtr> attribute_column_ptrs;
	for (size_t i = 3; i <= dict_struct.attributes.size(); i++)
	{
	    attribute_column_ptrs.push_back(block.safeGetByPosition(i).column);
	}         
       
        for (const auto row : collections::range(0, rows))
        {
            const auto & id = (*id_column_ptr).get64(row);
	    const auto & parent_id = (*parent_id_column_ptr).get64(row);
	    const auto & regexp = (*regexp_column_ptr).getDataAt(row).toString();
	    std::vector<String> attrs;
            for (size_t attribute_index  = 0; attribute_index <= dict_struct.attributes.size() - 3; attribute_index++)
            {
                const auto & attribute_column = *(attribute_column_ptrs[attribute_index]);
                const auto & attribute = attribute_column[row].get<String>();
                attrs.push_back(attribute);
            }

            setNodeValue(id, parent_id, regexp, attrs);
	    loaded_keys[row] = true;
        }
    }
}

Pipe RegexpTreeDictionary::read(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<StringRef> keys;
    keys.reserve(loaded_keys.size());
    for (size_t key_index = 0; key_index < loaded_keys.size(); ++key_index)
        if (loaded_keys[key_index])
            keys.push_back(" ");

    return Pipe(std::make_shared<DictionarySource>(
	DictionarySourceData(shared_from_this(), keys, column_names), max_block_size));
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

