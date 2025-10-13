#include <Backups/getBackupObjectKeyGenerator.h>

#include <Common/MatchGenerator.h>
#include <Common/SipHash.h>

#include <pcg_random.hpp>


namespace
{

class DefaultKeysGenerator : public DB::IObjectStorageKeysGenerator
{
public:
    DefaultKeysGenerator() = default;
    DB::ObjectStorageKey
    generate(const String & path, bool /* is_directory */, const std::optional<String> & /* key_prefix */) const override
    {
        return DB::ObjectStorageKey::createAsRelative(/*key_prefix*/ "", path);
    }

    bool isRandom() const override { return false; }

};

class HashBasedPrefixTemplateKeysGenerator : public DB::IObjectStorageKeysGenerator
{
public:
    explicit HashBasedPrefixTemplateKeysGenerator(String prefix_template_)
        : prefix_template(std::move(prefix_template_))
        , re_gen(prefix_template)
    {
    }
    DB::ObjectStorageKey
    generate(const String & path, bool /* is_directory */, const std::optional<String> & /* key_prefix */) const override
    {
        auto hash = sipHash64(path);
        pcg64 rng(hash);
        auto key_prefix = re_gen.generate(rng);
        return DB::ObjectStorageKey::createAsRelative(key_prefix, path);
    }

    bool isRandom() const override { return true; }

private:
    String prefix_template;
    DB::RandomStringGeneratorByRegexp re_gen;
};

}

namespace DB
{

ObjectStorageKeysGeneratorPtr
getBackupObjectKeyGenerator(const Poco::Util::AbstractConfiguration & config, const BackupSettings & backup_settings)
{
    String prefix_template;
    if (!backup_settings.key_prefix_template.empty())
        prefix_template = backup_settings.key_prefix_template;

    if (prefix_template.empty())
    {
        String config_prefix = "backups";
        prefix_template = config.getString(config_prefix + ".key_prefix_template", String());
    }
    if (prefix_template.empty())
        return std::make_shared<DefaultKeysGenerator>();
    else
        return std::make_shared<HashBasedPrefixTemplateKeysGenerator>(prefix_template);
}

}
