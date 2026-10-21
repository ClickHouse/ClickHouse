#pragma once

#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>
#include <Common/SecretString.h>
#include <IO/HTTPHeaderEntries.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

struct ElasticsearchConfiguration
{
    String url;
    String index;
    String user;
    SecretString password;
    SecretString bearer_token;
    SecretString api_key;
    String query = R"({"match_all":{}})";

    enum class AuthKind
    {
        None,
        Basic,
        Bearer,
        ApiKey,
    };

    AuthKind auth_kind = AuthKind::None;

    String named_collection_name;

    HTTPHeaderEntries authorizationHeaders() const;

    static ElasticsearchConfiguration fromArguments(ASTs & args, ContextPtr context, const StorageID * table_id = nullptr);
};

}
