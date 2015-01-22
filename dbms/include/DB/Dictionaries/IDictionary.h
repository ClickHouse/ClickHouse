#pragma once

#include <DB/Core/Field.h>
#include <memory>
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{

class IDictionary
{
public:
    using id_t = std::uint64_t;

	virtual UInt64 getUInt64(const id_t id, const std::string & attribute_name) const = 0;
    virtual StringRef getString(const id_t id, const std::string & attribute_name) const = 0;

    virtual ~IDictionary() = default;
};

using DictionaryPtr = std::unique_ptr<IDictionary>;

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

	static DictionaryStructure fromXML(const Poco::Util::XMLConfiguration & config, const std::string & config_prefix)
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
