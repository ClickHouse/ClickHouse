#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <Poco/Util/XMLConfiguration.h>
#include <map>
#include <vector>

namespace DB
{

class FlatDictionary : public IDictionary
{
public:
    FlatDictionary() = default;

    StringRef getString(const id_t id, const std::string & attribute_name) const override {
        return { "", 0 };
    }

	UInt64 getUInt64(const id_t id, const std::string & attribute_name) const override {
		return 0;
	}

private:
    using value_t = std::pair<id_t, Field>;
    using attribute_t = std::vector<value_t>;
    using attributes_t = std::map<std::string, attribute_t>;

    attribute_t attributes;
};

}
