#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>

#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>

#include <Common/Exception.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <base/hex.h>

#include <filesystem>
#include <sstream>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

namespace
{
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = 4096;
    constexpr std::string_view TMP_SUFFIX = ".tmp";

    String formatHexU64(UInt64 v)
    {
        /// Match the project-wide "0x" + lowercase fixed-width hex convention.
        return "0x" + getHexUIntLowercase(v);
    }

    UInt64 parseHexU64(const String & s, const char * field_name)
    {
        std::string_view sv(s);
        if (sv.starts_with("0x") || sv.starts_with("0X"))
            sv.remove_prefix(2);
        if (sv.empty() || sv.size() > 16)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`meta.json`: field `{}` is not a valid hex uint64: `{}`", field_name, s);

        UInt64 result = 0;
        for (char c : sv)
        {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "`meta.json`: field `{}` contains non-hex character: `{}`", field_name, s);
            result = (result << 4) | unhex(c);
        }
        return result;
    }

    DiskPtr getDisk(VolumePtr volume)
    {
        if (!volume)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexTableMeta: null volume");
        return volume->getDisk(0);
    }
}

ANNIndexTableMeta ANNIndexTableMeta::loadOrEmpty(VolumePtr volume, const std::string & relative_root_path)
{
    auto disk = getDisk(volume);
    const auto full_path = fs::path(relative_root_path) / ANN_TABLE_META_FILE;

    if (!disk->existsFile(full_path))
        return {};

    auto in = disk->readFile(full_path, ReadSettings{}, std::nullopt);
    String contents;
    {
        const size_t file_size = disk->getFileSize(full_path);
        contents.resize(file_size);
        if (file_size > 0)
            in->readStrict(contents.data(), file_size);
    }

    Poco::JSON::Object::Ptr root;
    try
    {
        Poco::JSON::Parser parser;
        auto parsed = parser.parse(contents);
        root = parsed.extract<Poco::JSON::Object::Ptr>();
        if (!root)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`meta.json`: root is not a JSON object");
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`meta.json`: failed to parse JSON: {}", e.displayText());
    }

    ANNIndexTableMeta meta;
    try
    {
        meta.version = root->getValue<UInt32>("version");

        auto shape_ptr = root->getObject("shape");
        if (!shape_ptr)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "`meta.json`: missing `shape`");
        meta.shape.dim = shape_ptr->getValue<UInt32>("dim");
        meta.shape.metric = static_cast<UInt8>(shape_ptr->getValue<UInt32>("metric"));
        meta.shape.algorithm = shape_ptr->getValue<std::string>("algorithm");
        meta.shape.params_hash = parseHexU64(
            shape_ptr->getValue<std::string>("params_hash"), "shape.params_hash");

        meta.hash_algo = root->getValue<std::string>("hash_algo");
        meta.hash_seed = parseHexU64(
            root->getValue<std::string>("hash_seed"), "hash_seed");
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`meta.json`: malformed structure: {}", e.displayText());
    }

    return meta;
}

void ANNIndexTableMeta::writeTo(
    VolumePtr volume,
    const std::string & relative_root_path,
    DiskTransactionPtr txn) const
{
    Poco::JSON::Object root;
    root.set("version", version);

    Poco::JSON::Object shape_obj;
    shape_obj.set("dim", shape.dim);
    shape_obj.set("metric", static_cast<UInt32>(shape.metric));
    shape_obj.set("algorithm", shape.algorithm);
    shape_obj.set("params_hash", formatHexU64(shape.params_hash));
    root.set("shape", shape_obj);

    root.set("hash_algo", hash_algo);
    root.set("hash_seed", formatHexU64(hash_seed));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(root, oss, 2, -1, Poco::JSON_WRAP_STRINGS | Poco::JSON_ESCAPE_UNICODE);
    const String payload = oss.str();

    auto disk = getDisk(volume);
    const auto target_path = fs::path(relative_root_path) / ANN_TABLE_META_FILE;

    if (txn)
    {
        /// Transactional write: commit is the caller's responsibility; they will use the same
        /// transaction for any related filesystem changes so that `meta.json` and the rest of
        /// the operation become visible atomically.
        auto out = txn->writeFile(target_path, DEFAULT_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        out->write(payload.data(), payload.size());
        out->finalize();
        return;
    }

    /// Non-transactional path: write to `meta.json.tmp` then atomically rename over the
    /// existing `meta.json`. This survives a crash between the two operations: the previous
    /// `meta.json` remains intact until the rename completes.
    const auto tmp_path = fs::path(relative_root_path) / (String(ANN_TABLE_META_FILE) + String(TMP_SUFFIX));
    {
        auto out = disk->writeFile(tmp_path, DEFAULT_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        out->write(payload.data(), payload.size());
        out->finalize();
    }
    disk->replaceFile(tmp_path, target_path);
}

}
