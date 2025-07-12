#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/AIClientFactory.h>
#include <Client/AI/AIConfiguration.h>
#include <Common/Exception.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/AutoPtr.h>
#include <Poco/Environment.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

/// Test AIClientFactory with invalid configurations
TEST(AIClientFactory, InvalidProvider)
{
    AIConfiguration config;
    config.provider = "invalid_provider";
    config.api_key = "test_key";
    
    EXPECT_THROW({
        AIClientFactory::createClient(config);
    }, Exception);
    
    try
    {
        AIClientFactory::createClient(config);
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, e.code());
        EXPECT_NE(e.message().find("Unknown AI provider"), std::string::npos);
    }
}

/// Test AIClientFactory with empty API key
TEST(AIClientFactory, EmptyAPIKey)
{
    AIConfiguration config;
    config.provider = "openai";
    config.api_key = "";

    // When API key is empty, it should check environment variable
    // If env var is not set, it should throw
    if (!Poco::Environment::has("OPENAI_API_KEY"))
    {
        EXPECT_THROW({
            AIClientFactory::createClient(config);
        }, std::runtime_error);
        
        try
        {
            AIClientFactory::createClient(config);
        }
        catch (const std::runtime_error & e)
        {
            EXPECT_NE(std::string(e.what()).find("environment variable not set"), std::string::npos);
        }
    }
    else
    {
        // If env var is set, it should use that and not throw
        EXPECT_NO_THROW({
            auto client = AIClientFactory::createClient(config);
            EXPECT_TRUE(client.provider_name() == "openai");
        });
    }
}

/// Test configuration validation
TEST(AIClientFactory, LoadConfigurationInvalidProvider)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    
    config->setString("ai.provider", "invalid_provider");
    config->setString("ai.api_key", "test_key");
    
    EXPECT_THROW({
        AIClientFactory::loadConfiguration(*config);
    }, Exception);
    
    try
    {
        AIClientFactory::loadConfiguration(*config);
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, e.code());
        EXPECT_NE(e.message().find("Invalid AI provider"), std::string::npos);
    }
}

/// Test configuration with malformed values
TEST(AIClientFactory, MalformedConfigurationValues)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    
    // The factory doesn't validate temperature range, it just passes it through
    config->setString("ai.provider", "openai");
    config->setString("ai.api_key", "test_key");
    config->setDouble("ai.temperature", 2.5); // This is accepted without validation
    
    AIConfiguration ai_config = AIClientFactory::loadConfiguration(*config);
    EXPECT_EQ(2.5, ai_config.temperature); // It's loaded as-is
    
    // Test negative max_tokens - Poco throws exception on negative integer for unsigned type
    config->setDouble("ai.temperature", 0.5);
    
    // Test negative max_tokens - When calling getUInt64() on a negative value,
    // Poco doesn't throw immediately on setInt(), but on getUInt64()
    config->setInt("ai.max_tokens", -100);
    
    EXPECT_THROW({
        ai_config = AIClientFactory::loadConfiguration(*config);
    }, Poco::SyntaxException);
    
    // Test with valid values
    config->setInt("ai.max_tokens", 1000);
    config->setInt("ai.timeout_seconds", 0); // Zero is accepted
    
    EXPECT_NO_THROW({
        ai_config = AIClientFactory::loadConfiguration(*config);
    });
    EXPECT_EQ(0, ai_config.timeout_seconds);
}

/// Test handling of special characters in API keys
TEST(AIClientFactory, SpecialCharactersInAPIKey)
{
    AIConfiguration config;
    config.provider = "openai";
    
    // Test API key with special characters
    config.api_key = "sk-test!@#$%^&*()_+-=[]{}|;:'\",.<>?/~`";
    
    // Should not throw - API keys can contain special characters
    EXPECT_NO_THROW({
        auto client = AIClientFactory::createClient(config);
        EXPECT_TRUE(client.is_valid());
    });
    
    // Test API key with unicode characters
    config.api_key = "sk-测试-κλειδί-ключ";
    
    EXPECT_NO_THROW({
        auto client = AIClientFactory::createClient(config);
        EXPECT_TRUE(client.is_valid());
    });
}

/// Test real client creation with API key from environment
TEST(AIClientFactory, CreateRealClient)
{
    // Skip if no API key in environment
    if (!Poco::Environment::has("OPENAI_API_KEY") && !Poco::Environment::has("ANTHROPIC_API_KEY"))
    {
        GTEST_SKIP() << "No AI API key in environment";
    }
    
    AIConfiguration config;
    
    if (Poco::Environment::has("OPENAI_API_KEY"))
    {
        config.provider = "openai";
        config.api_key = Poco::Environment::get("OPENAI_API_KEY");
    }
    else
    {
        config.provider = "anthropic";
        config.api_key = Poco::Environment::get("ANTHROPIC_API_KEY");
    }
    
    // Should not throw
    EXPECT_NO_THROW({
        auto client = AIClientFactory::createClient(config);
        EXPECT_TRUE(client.is_valid());
        EXPECT_EQ(config.provider, client.provider_name());
    });
}

/// Test unsupported provider when not compiled
TEST(AIClientFactory, UnsupportedProvider)
{
    AIConfiguration config;
    config.api_key = "test_key";
    
    // Test OpenAI when not compiled
#ifndef AI_SDK_HAS_OPENAI
    config.provider = "openai";
    
    EXPECT_THROW({
        AIClientFactory::createClient(config);
    }, Exception);
    
    try
    {
        AIClientFactory::createClient(config);
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, e.code());
        EXPECT_NE(e.message().find("not compiled"), std::string::npos);
    }
#endif
    
    // Test Anthropic when not compiled
#ifndef AI_SDK_HAS_ANTHROPIC
    config.provider = "anthropic";
    
    EXPECT_THROW({
        AIClientFactory::createClient(config);
    }, Exception);
    
    try
    {
        AIClientFactory::createClient(config);
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, e.code());
        EXPECT_NE(e.message().find("not compiled"), std::string::npos);
    }
#endif
}

/// Test provider case sensitivity
TEST(AIClientFactory, ProviderCaseSensitivity)
{
    AIConfiguration config;
    config.api_key = "test_key";
    
    // Test uppercase
    config.provider = "OPENAI";
    EXPECT_THROW({
        AIClientFactory::createClient(config);
    }, Exception);
    
    // Test mixed case
    config.provider = "OpenAI";
    EXPECT_THROW({
        AIClientFactory::createClient(config);
    }, Exception);
    
    // Only lowercase should work
    config.provider = "openai";
    // This will still fail due to compile flags or actual API validation,
    // but it should not fail due to provider name
    try
    {
        AIClientFactory::createClient(config);
    }
    catch (const Exception & e)
    {
        // Should not contain "Unknown AI provider" if lowercase is correct
        if (e.message().find("Unknown AI provider") != std::string::npos)
        {
            FAIL() << "Lowercase provider name should be recognized";
        }
    }
}

/// Test configuration loading with missing required fields
TEST(AIClientFactory, MissingRequiredFields)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    
    // Missing provider - should use default
    config->setString("ai.api_key", "test_key");
    
    AIConfiguration ai_config = AIClientFactory::loadConfiguration(*config);
    EXPECT_EQ("openai", ai_config.provider); // Should use default
    
    // Missing API key - should remain empty
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config2 = new Poco::Util::XMLConfiguration();
    config2->setString("ai.provider", "anthropic");
    
    ai_config = AIClientFactory::loadConfiguration(*config2);
    EXPECT_TRUE(ai_config.api_key.empty());
}

}

#endif // USE_CLIENT_AI
