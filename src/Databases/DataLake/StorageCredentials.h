#pragma once
#include <Core/Types.h>
#include <Common/SensitiveString.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <fmt/format.h>

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
        std::string_view secret_access_key_,
        std::string_view session_token_)
        : access_key_id(access_key_id_)
        , secret_access_key(secret_access_key_)
        , session_token(session_token_)
    {}

    bool isEmpty() const { return access_key_id.empty() || secret_access_key.empty(); }

    void addCredentialsToEngineArgs(DB::ASTs & engine_args) const override
    {
        if (engine_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Storage credentials specified in AST already");

        engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>(access_key_id));
        engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>(secret_access_key.view()));
        if (!session_token.empty())
            engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>(session_token.view()));
    }

    const String & getAccessKeyId() const
    {
        return access_key_id;
    }

    std::string_view getSecretAccessKey() const
    {
        return secret_access_key.view();
    }

    std::string_view getSessionToken() const
    {
        return session_token.view();
    }

private:
    std::string access_key_id;
    DB::SensitiveString secret_access_key;
    DB::SensitiveString session_token;
};

class GCSCredentials final : public IStorageCredentials
{
public:
    explicit GCSCredentials(std::string_view oauth_token_)
        : oauth_token(oauth_token_)
    {}

    void addCredentialsToEngineArgs(DB::ASTs & engine_args) const override
    {
        if (engine_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Storage credentials specified in AST already");

        /// Disable AWS HMAC signing; GCS Bearer token auth is used instead.
        engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>("NOSIGN"));

        /// Inject the Authorization header: headers('Authorization'='Bearer <token>')
        engine_args.push_back(
            DB::makeASTFunction("headers",
                DB::makeASTFunction("equals",
                    DB::make_intrusive<DB::ASTLiteral>("Authorization"),
                    DB::make_intrusive<DB::ASTLiteral>(fmt::format("Bearer {}", oauth_token.view())))));
    }

    std::string_view getToken() const { return oauth_token.view(); }

private:
    DB::SensitiveString oauth_token;
};

class AzureCredentials final : public IStorageCredentials
{
public:
    explicit AzureCredentials(
        std::string_view sas_token_)
        : sas_token(sas_token_)
    {}

    void addCredentialsToEngineArgs(DB::ASTs & engine_args) const override
    {
        if (engine_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Storage credentials specified in AST already");

        engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>(sas_token.view()));
    }

private:
    DB::SensitiveString sas_token;
};

}
