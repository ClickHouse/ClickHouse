#include <Backups/getBackupObjectKeyGenerator.h>

#include <Common/ErrorCodes.h>
#include <Common/SipHash.h>

#include <filesystem>
#include <cstddef>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{

extern const int ARGUMENT_OUT_OF_BOUND;

}

}

namespace
{
constexpr size_t DEFAULT_BACKUP_PREFIX_LENGTH = 0;

class BackupKeysGenerator : public DB::IObjectStorageKeysGenerator
{
public:
    explicit BackupKeysGenerator(size_t prefix_length_)
        : prefix_length(prefix_length_)
    {
        if (prefix_length > 13)
            throw DB::Exception(
                DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Prefix length n={} too large: 26^n exceeds uint64 range", prefix_length);
    }
    DB::ObjectStorageKey
    generate(const String & path, bool /* is_directory */, const std::optional<String> & /* key_prefix */) const override
    {
        chassert(path == fs::path(path).lexically_normal());

        if (prefix_length == 0)
            return DB::ObjectStorageKey::createAsRelative(/*key_prefix*/ "", path);

        std::string prefix(prefix_length, 'a');
        auto hash = sipHash64(path);
        for (int i = prefix_length; i > 0; --i)
        {
            prefix[i - 1] = 'a' + hash % 26;
            hash /= 26;
        }
        return DB::ObjectStorageKey::createAsRelative(prefix, path);
    }

    bool isRandom() const override { return false; }

private:
    const size_t prefix_length;
};

}

namespace DB
{

ObjectStorageKeysGeneratorPtr
getBackupObjectKeyGenerator(const Poco::Util::AbstractConfiguration & config, const BackupSettings & backup_settings)
{
    size_t prefix_length = 0;
    if (backup_settings.key_prefix_length != 0)
        prefix_length = backup_settings.key_prefix_length;

    if (prefix_length == 0)
    {
        String config_prefix = "backups";
        prefix_length = config.getUInt64(config_prefix + ".key_prefix_length", DEFAULT_BACKUP_PREFIX_LENGTH);
    }
    return std::make_shared<BackupKeysGenerator>(prefix_length);
}

}
