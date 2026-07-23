#include <Common/ObjectStorageKeyGenerator.h>

#include <Common/getRandomASCIIString.h>
#include <Common/MatchGenerator.h>

#include <fmt/format.h>


class GeneratorWithTemplate : public DB::IObjectStorageKeysGenerator
{
public:
    explicit GeneratorWithTemplate(String key_template_)
    : key_template(std::move(key_template_))
    , re_gen(key_template)
    {
    }
    DB::ObjectStorageKey generate(const String &, bool /* is_directory */, const std::optional<String> & /* key_prefix */) const override
    {
        return DB::ObjectStorageKey::createAsAbsolute(re_gen.generate());
    }

    bool isRandom() const override { return true; }

private:
    String key_template;
    DB::RandomStringGeneratorByRegexp re_gen;
};


class GeneratorWithPrefix : public DB::IObjectStorageKeysGenerator
{
public:
    explicit GeneratorWithPrefix(String key_prefix_)
        : key_prefix(std::move(key_prefix_))
    {}

    DB::ObjectStorageKey generate(const String &, bool /* is_directory */, const std::optional<String> & /* key_prefix */) const override
    {
        /// Path to store the new S3 object.

        /// Total length is 32 a-z characters for enough randomness.
        /// First 3 characters are used as a prefix for
        /// https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/
        constexpr size_t key_name_total_size = 32;
        constexpr size_t key_name_prefix_size = 3;

        /// Path to store new S3 object.
        String key = fmt::format("{}/{}",
                                 DB::getRandomASCIIString(key_name_prefix_size),
                                 DB::getRandomASCIIString(key_name_total_size - key_name_prefix_size));

        /// what ever key_prefix value is, consider that key as relative
        return DB::ObjectStorageKey::createAsRelative(key_prefix, key);
    }

    bool isRandom() const override { return true; }

private:
    String key_prefix;
};


class GeneratorAsIsWithPrefix : public DB::IObjectStorageKeysGenerator
{
public:
    explicit GeneratorAsIsWithPrefix(String key_prefix_)
        : key_prefix(std::move(key_prefix_))
    {}

    DB::ObjectStorageKey
    generate(const String & path, bool /* is_directory */, const std::optional<String> & /* key_prefix */) const override
    {
        return DB::ObjectStorageKey::createAsRelative(key_prefix, path);
    }

    bool isRandom() const override { return false; }

private:
    String key_prefix;
};


namespace DB
{

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorAsIsWithPrefix(String key_prefix)
{
    return std::make_shared<GeneratorAsIsWithPrefix>(std::move(key_prefix));
}

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByPrefix(String key_prefix)
{
    return std::make_shared<GeneratorWithPrefix>(std::move(key_prefix));
}

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByTemplate(String key_template)
{
    return std::make_shared<GeneratorWithTemplate>(std::move(key_template));
}

}
