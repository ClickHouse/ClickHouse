#include <Backups/getBackupObjectKeyGenerator.h>
#include <Common/MatchGenerator.h>
#include <Common/SipHash.h>

#include <pcg_random.hpp>


namespace
{

class HashBasedTemplatePrefixGenerator : public DB::IObjectStorageKeysGenerator
{
public:
    explicit HashBasedTemplatePrefixGenerator(String prefix_template_)
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
getBackupObjectKeyGenerator(const Poco::Util::AbstractConfiguration & /*config*/)
{
    return std::make_shared<HashBasedTemplatePrefixGenerator>("{a-z}[0-3]");
}

}
