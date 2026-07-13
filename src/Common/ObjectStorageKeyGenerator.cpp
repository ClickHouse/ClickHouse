#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/MatchGenerator.h>
#include <Common/re2.h>

#include <fmt/format.h>

#include <filesystem>

namespace
{

class GeneratorWithTemplate : public DB::IObjectStorageKeyGenerator
{
public:
    explicit GeneratorWithTemplate(std::string key_template_)
        : key_template(std::move(key_template_))
        , re_gen(key_template)
    {
    }

    DB::ObjectStorageKey generate() const override
    {
        return DB::ObjectStorageKey::createAsAbsolute(re_gen.generate());
    }

private:
    std::string key_template;
    DB::RandomStringGeneratorByRegexp re_gen;
};

}

namespace DB
{

ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorByPrefix(String key_prefix)
{
    return std::make_shared<GeneratorWithTemplate>(std::filesystem::path(RE2::QuoteMeta(key_prefix)) / "[a-z]{3}/[a-z]{29}");
}

ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorByTemplate(String key_template)
{
    return std::make_shared<GeneratorWithTemplate>(std::move(key_template));
}

}
