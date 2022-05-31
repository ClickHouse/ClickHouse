#pragma once

#include <unordered_map>
#include <Storages/ColumnsDescription.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <base/types.h>

namespace DB
{
class MeiliSearchColumnDescriptionFetcher
{
public:
    explicit MeiliSearchColumnDescriptionFetcher(const MeiliSearchConfiguration & config);

    void addParam(const String & key, const String & val);

    ColumnsDescription fetchColumnsDescription() const;

private:
    std::unordered_map<String, String> query_params;
    MeiliSearchConnection connection;
};

}
