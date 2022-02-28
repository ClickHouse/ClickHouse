#include <memory>
#include <Storages/MeiliSearch/MeiliSearchColumnDescriptionFetcher.h>
#include <base/JSON.h>
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_MEILISEARCH_TYPE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int MEILISEARCH_EXCEPTION;
}

MeiliSearchColumnDescriptionFetcher::MeiliSearchColumnDescriptionFetcher(const MeiliSearchConfiguration & config) : connection(config)
{
}

void MeiliSearchColumnDescriptionFetcher::addParam(const String & key, const String & val)
{
    query_params[key] = val;
}

bool isDouble(const std::string & val)
{
    std::istringstream iss(val);
    double f;
    iss >> std::noskipws >> f;
    return iss.eof() && !iss.fail() && static_cast<Int64>(f) != f;
}

DataTypePtr parseTypeOfField(JSON ptr)
{
    if (ptr.isString())
    {
        return std::make_shared<DataTypeString>();
    }
    if (ptr.isArray())
    {
        auto nested_type = parseTypeOfField(ptr.begin());
        return std::make_shared<DataTypeArray>(nested_type);
    }
    if (ptr.isBool())
    {
        return std::make_shared<DataTypeUInt8>();
    }
    if (ptr.isNull())
    {
        DataTypePtr res = std::make_shared<DataTypeNullable>(res);
        return res;
    }
    if (isDouble(ptr.toString()))
    {
        return std::make_shared<DataTypeFloat64>();
    }
    if (ptr.isNumber())
    {
        return std::make_shared<DataTypeInt64>();
    }
    if (ptr.isObject())
    {
        return std::make_shared<DataTypeString>();
    }
    throw Exception(ErrorCodes::UNSUPPORTED_MEILISEARCH_TYPE, "Can't recognize type of some fields");
}

ColumnsDescription MeiliSearchColumnDescriptionFetcher::fetchColumnsDescription() const
{
    auto response = connection.searchQuery(query_params);
    JSON jres = JSON(response).begin();

    if (jres.getName() == "message")
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());

    NamesAndTypesList list;

    for (const JSON kv_pair : jres.getValue().begin())
    {
        if (!kv_pair.isNameValuePair())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Bad response data");

        list.emplace_back(kv_pair.getName(), parseTypeOfField(kv_pair.getValue()));
    }

    return ColumnsDescription(list);
}

};
