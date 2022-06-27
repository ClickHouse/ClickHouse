#include <memory>
#include <string>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadHelpers.h>
#include <Storages/MeiliSearch/MeiliSearchColumnDescriptionFetcher.h>
#include <base/JSON.h>
#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
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

bool checkIfInteger(const String & s)
{
    return s.find('.') == String::npos;
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
    if (ptr.isNumber())
    {
        if (checkIfInteger(ptr.toString()))
        {
            return std::make_shared<DataTypeInt64>();
        }
        return std::make_shared<DataTypeFloat64>();
    }
    return std::make_shared<DataTypeString>();
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
