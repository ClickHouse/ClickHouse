#pragma once

#include <DB/Core/ErrorCodes.h>
#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/IO/ReadBufferFromString.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <vector>
#include <string>
#include <map>

namespace DB
{

enum class AttributeUnderlyingType
{
	UInt8,
	UInt16,
	UInt32,
	UInt64,
	Int8,
	Int16,
	Int32,
	Int64,
	Float32,
	Float64,
	String
};

inline AttributeUnderlyingType getAttributeUnderlyingType(const std::string & type)
{
	static const std::unordered_map<std::string, AttributeUnderlyingType> dictionary{
		{ "UInt8", AttributeUnderlyingType::UInt8 },
		{ "UInt16", AttributeUnderlyingType::UInt16 },
		{ "UInt32", AttributeUnderlyingType::UInt32 },
		{ "UInt64", AttributeUnderlyingType::UInt64 },
		{ "Int8", AttributeUnderlyingType::Int8 },
		{ "Int16", AttributeUnderlyingType::Int16 },
		{ "Int32", AttributeUnderlyingType::Int32 },
		{ "Int64", AttributeUnderlyingType::Int64 },
		{ "Float32", AttributeUnderlyingType::Float32 },
		{ "Float64", AttributeUnderlyingType::Float64 },
		{ "String", AttributeUnderlyingType::String },
		{ "Date", AttributeUnderlyingType::UInt16 },
		{ "DateTime", AttributeUnderlyingType::UInt32 },
	};

	const auto it = dictionary.find(type);
	if (it != std::end(dictionary))
		return it->second;

	throw Exception{
		"Unknown type " + type,
		ErrorCodes::UNKNOWN_TYPE
	};
}

inline std::string toString(const AttributeUnderlyingType type)
{
	switch (type)
	{
		case AttributeUnderlyingType::UInt8: return "UInt8";
		case AttributeUnderlyingType::UInt16: return "UInt16";
		case AttributeUnderlyingType::UInt32: return "UInt32";
		case AttributeUnderlyingType::UInt64: return "UInt64";
		case AttributeUnderlyingType::Int8: return "Int8";
		case AttributeUnderlyingType::Int16: return "Int16";
		case AttributeUnderlyingType::Int32: return "Int32";
		case AttributeUnderlyingType::Int64: return "Int64";
		case AttributeUnderlyingType::Float32: return "Float32";
		case AttributeUnderlyingType::Float64: return "Float64";
		case AttributeUnderlyingType::String: return "String";
	}

	throw Exception{
		"Unknown attribute_type " + toString(static_cast<int>(type)),
		ErrorCodes::ARGUMENT_OUT_OF_BOUND
	};
}

/// Min and max lifetimes for a dictionary or it's entry
struct DictionaryLifetime final
{
	std::uint64_t min_sec;
	std::uint64_t max_sec;

	DictionaryLifetime(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
	{
		const auto & lifetime_min_key = config_prefix + ".min";
		const auto has_min = config.has(lifetime_min_key);

		this->min_sec = has_min ? config.getInt(lifetime_min_key) : config.getInt(config_prefix);
		this->max_sec = has_min ? config.getInt(config_prefix + ".max") : this->min_sec;
	}
};

/** Holds the description of a single dictionary attribute:
*	- name, used for lookup into dictionary and source;
*	- type, used in conjunction with DataTypeFactory and getAttributeUnderlyingTypeByname;
*	- null_value, used as a default value for non-existent entries in the dictionary,
*		decimal representation for numeric attributes;
*	- hierarchical, whether this attribute defines a hierarchy;
*	- injective, whether the mapping to parent is injective (can be used for optimization of GROUP BY?)
*/
struct DictionaryAttribute final
{
	const std::string name;
	const AttributeUnderlyingType underlying_type;
	const DataTypePtr type;
	const std::string expression;
	const Field null_value;
	const bool hierarchical;
	const bool injective;
};

/// Name of identifier plus list of attributes
struct DictionaryStructure final
{
	std::string id_name;
	std::vector<DictionaryAttribute> attributes;
	std::string range_min;
	std::string range_max;

	DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
		: id_name{config.getString(config_prefix + ".id.name")},
		  range_min{config.getString(config_prefix + ".range_min.name", "")},
		  range_max{config.getString(config_prefix + ".range_max.name", "")}
	{
		if (id_name.empty())
			throw Exception{
				"No 'id' specified for dictionary",
				ErrorCodes::BAD_ARGUMENTS
			};

		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_prefix, keys);
		auto has_hierarchy = false;

		for (const auto & key : keys)
		{
			if (0 != strncmp(key.data(), "attribute", strlen("attribute")))
				continue;

			const auto prefix = config_prefix + '.' + key + '.';

			const auto name = config.getString(prefix + "name");
			const auto type_string = config.getString(prefix + "type");
			const auto type = DataTypeFactory::instance().get(type_string);
			const auto underlying_type = getAttributeUnderlyingType(type_string);

			const auto expression = config.getString(prefix + "expression", "");

			const auto null_value_string = config.getString(prefix + "null_value");
			Field null_value;
			try
			{
				ReadBufferFromString null_value_buffer{null_value_string};
				type->deserializeText(null_value, null_value_buffer);
			}
			catch (const std::exception & e)
			{
				throw Exception{
					std::string{"Error parsing null_value: "} + e.what(),
					ErrorCodes::BAD_ARGUMENTS
				};
			}

			const auto hierarchical = config.getBool(prefix + "hierarchical", false);
			const auto injective = config.getBool(prefix + "injective", false);
			if (name.empty())
				throw Exception{
					"Properties 'name' and 'type' of an attribute cannot be empty",
					ErrorCodes::BAD_ARGUMENTS
				};

			if (has_hierarchy && hierarchical)
				throw Exception{
					"Only one hierarchical attribute supported",
					ErrorCodes::BAD_ARGUMENTS
				};

			has_hierarchy = has_hierarchy || hierarchical;

			attributes.emplace_back(DictionaryAttribute{
				name, underlying_type, type, expression, null_value, hierarchical, injective
			});
		}

		if (attributes.empty())
			throw Exception{
				"Dictionary has no attributes defined",
				ErrorCodes::BAD_ARGUMENTS
			};
	}
};

}
