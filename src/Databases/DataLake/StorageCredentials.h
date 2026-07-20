#pragma once
#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DataLake
{

class IStorageCredentials
{
public:
    virtual ~IStorageCredentials() = default;

    virtual void addCredentialsToEngineArgs(DB::ASTs & engine_args) const = 0;
};

class S3Credentials final : public IStorageCredentials
{
public:
    S3Credentials(
        const std::string & access_key_id_,
        const std::string & secret_access_key_,
        const std::string & session_token_)
        : access_key_id(access_key_id_)
        , secret_access_key(secret_access_key_)
        , session_token(session_token_)
    {}

    void addCredentialsToEngineArgs(DB::ASTs & engine_args) const override
    {
        if (engine_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Storage credentials specified in AST already");

        engine_args.push_back(std::make_shared<DB::ASTLiteral>(access_key_id));
        engine_args.push_back(std::make_shared<DB::ASTLiteral>(secret_access_key));
        if (!session_token.empty())
            engine_args.push_back(std::make_shared<DB::ASTLiteral>(session_token));
    }

private:
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
};

}
