#if 0
void UserAttributes::checkPassword(const String & password_)
{
    auto on_wrong_password = [&]()
    {
        if (password_.empty())
            throw Exception("Password required for user " + name, ErrorCodes::REQUIRED_PASSWORD);
        else
            throw Exception("Wrong password for user " + name, ErrorCodes::WRONG_PASSWORD);
    };

    if (!password_sha256.empty())
    {
#if USE_SSL
        unsigned char hash[32];
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(password_.data()), password_.size());
        SHA256_Final(hash, &ctx);
        if (String(reinterpret_cast<char *>(hash), 32) != password_sha256)
            on_wrong_password();
#else
        throw DB::Exception("SHA256 passwords support is disabled, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
    else if (password_ != password)
    {
        on_wrong_password();
    }
}
#endif
