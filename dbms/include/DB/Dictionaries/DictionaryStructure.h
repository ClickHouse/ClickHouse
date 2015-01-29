#pragma once

#include <DB/Core/ErrorCodes.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <vector>
#include <string>
#include <map>

namespace DB
{

enum class attribute_type
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

inline attribute_type getAttributeTypeByName(const std::string & type)
{
	static const std::unordered_map<std::string, attribute_type> dictionary{
		{ "UInt8", attribute_type::uint8 },
		{ "UInt16", attribute_type::uint16 },
		{ "UInt32", attribute_type::uint32 },
		{ "UInt64", attribute_type::uint64 },
		{ "Int8", attribute_type::int8 },
		{ "Int16", attribute_type::int16 },
		{ "Int32", attribute_type::int32 },
		{ "Int64", attribute_type::int64 },
		{ "Float32", attribute_type::float32 },
		{ "Float64", attribute_type::float64 },
		{ "String", attribute_type::string },
	};

	const auto it = dictionary.find(type);
	if (it != std::end(dictionary))
		return it->second;

	throw Exception{
		"Unknown type " + type,
		ErrorCodes::UNKNOWN_TYPE
	};
}

inline std::string toString(const attribute_type type)
{
	switch (type)
	{
		case attribute_type::uint8: return "UInt8";
		case attribute_type::uint16: return "UInt16";
		case attribute_type::uint32: return "UInt32";
		case attribute_type::uint64: return "UInt64";
		case attribute_type::int8: return "Int8";
		case attribute_type::int16: return "Int16";
		case attribute_type::int32: return "Int32";
		case attribute_type::int64: return "Int64";
		case attribute_type::float32: return "Float32";
		case attribute_type::float64: return "Float64";
		case attribute_type::string: return "String";
	}

	throw Exception{
		"Unknown attribute_type " + toString(type),
		ErrorCodes::ARGUMENT_OUT_OF_BOUND
	};
}

struct DictionaryAttribute
{
	std::string name;
	std::string type;
	std::string null_value;
	bool hierarchical;
	bool injective;
};

struct DictionaryStructure
{
	std::string id_name;
	std::vector<DictionaryAttribute> attributes;

	static DictionaryStructure fromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
	{
		const auto & id_name = config.getString(config_prefix + ".id.name");
		if (id_name.empty())
			throw Exception{
				"No 'id' specified for dictionary",
				ErrorCodes::BAD_ARGUMENTS
			};

		DictionaryStructure result{id_name};

		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_prefix, keys);
		auto has_hierarchy = false;
		for (const auto & key : keys)
		{
			if (0 != strncmp(key.data(), "attribute", strlen("attribute")))
				continue;

			const auto & prefix = config_prefix + '.' + key + '.';
			const auto & name = config.getString(prefix + "name");
			const auto & type = config.getString(prefix + "type");
			const auto & null_value = config.getString(prefix + "null_value");
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

			result.attributes.emplace_back(DictionaryAttribute{name, type, null_value, hierarchical, injective});
		}

		if (result.attributes.empty())
			throw Exception{
				"Dictionary has no attributes defined",
				ErrorCodes::BAD_ARGUMENTS
			};

		return result;
	}
};

}
