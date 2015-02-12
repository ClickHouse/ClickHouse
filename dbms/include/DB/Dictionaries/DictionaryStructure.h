#pragma once

#include <DB/Core/ErrorCodes.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <vector>
#include <string>
#include <map>

namespace DB
{

enum class AttributeType
{
	uint8,
	uint16,
	uint32,
	uint64,
	int8,
	int16,
	int32,
	int64,
	float32,
	float64,
	string
};

inline AttributeType getAttributeTypeByName(const std::string & type)
{
	static const std::unordered_map<std::string, AttributeType> dictionary{
		{ "UInt8", AttributeType::uint8 },
		{ "UInt16", AttributeType::uint16 },
		{ "UInt32", AttributeType::uint32 },
		{ "UInt64", AttributeType::uint64 },
		{ "Int8", AttributeType::int8 },
		{ "Int16", AttributeType::int16 },
		{ "Int32", AttributeType::int32 },
		{ "Int64", AttributeType::int64 },
		{ "Float32", AttributeType::float32 },
		{ "Float64", AttributeType::float64 },
		{ "String", AttributeType::string },
	};

	const auto it = dictionary.find(type);
	if (it != std::end(dictionary))
		return it->second;

	throw Exception{
		"Unknown type " + type,
		ErrorCodes::UNKNOWN_TYPE
	};
}

inline std::string toString(const AttributeType type)
{
	switch (type)
	{
		case AttributeType::uint8: return "UInt8";
		case AttributeType::uint16: return "UInt16";
		case AttributeType::uint32: return "UInt32";
		case AttributeType::uint64: return "UInt64";
		case AttributeType::int8: return "Int8";
		case AttributeType::int16: return "Int16";
		case AttributeType::int32: return "Int32";
		case AttributeType::int64: return "Int64";
		case AttributeType::float32: return "Float32";
		case AttributeType::float64: return "Float64";
		case AttributeType::string: return "String";
	}

	throw Exception{
		"Unknown attribute_type " + toString(static_cast<int>(type)),
		ErrorCodes::ARGUMENT_OUT_OF_BOUND
	};
}

/// Min and max lifetimes for a dictionary or it's entry
struct DictionaryLifetime
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
*	- type, used in conjunction with DataTypeFactory and getAttributeTypeByname;
*	- null_value, used as a default value for non-existent entries in the dictionary,
*		decimal representation for numeric attributes;
*	- hierarchical, whether this attribute defines a hierarchy;
*	- injective, whether the mapping to parent is injective (can be used for optimization of GROUP BY?)
*/
struct DictionaryAttribute
{
	std::string name;
	std::string type;
	std::string null_value;
	bool hierarchical;
	bool injective;
};

/// Name of identifier plus list of attributes
struct DictionaryStructure
{
	std::string id_name;
	std::vector<DictionaryAttribute> attributes;

	DictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
		: id_name{config.getString(config_prefix + ".id.name")}
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
			const auto type = config.getString(prefix + "type");
			const auto null_value = config.getString(prefix + "null_value");
			const auto hierarchical = config.getBool(prefix + "hierarchical", false);
			const auto injective = config.getBool(prefix + "injective", false);
			if (name.empty() || type.empty())
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
				name, type, null_value, hierarchical, injective
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
