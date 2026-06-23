#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>

#include <Storages/MergeTree/IDataPartStorage.h>

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Common/Exception.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <sstream>
#include <string>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::UniqueKeyTxn
{

const char * UniqueKeyManifest::FILE_NAME = "unique_key.txt";

bool UniqueKeyManifest::exists(const IDataPartStorage & storage)
{
    return storage.existsFile(FILE_NAME);
}

namespace
{
    /// On-disk format version. Bump when the schema or field semantics change;
    /// `read()` rejects anything else so an older binary fail-closes on a
    /// newer manifest rather than misreading it.
    constexpr UInt64 FORMAT_VERSION = 1;

    /// Strict unsigned-integer extraction. Rejects a missing key and any
    /// non-integer JSON type so a corrupted manifest fail-closes: `Poco`'s
    /// `getValue<UInt64>` is coercive (`true`→1, `1.5`→1, `"7"`→7), which would
    /// let a wrong-typed field deserialize to garbage.
    UInt64 getStrictUInt(const Poco::JSON::Object::Ptr & obj, const std::string & key)
    {
        const Poco::Dynamic::Var v = obj->get(key);
        if (v.isEmpty() || v.isBoolean() || !v.isInteger())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "field '{}' is missing or not an integer", key);
        return v.convert<UInt64>();
    }

    std::string getStrictString(const Poco::JSON::Object::Ptr & obj, const std::string & key)
    {
        const Poco::Dynamic::Var v = obj->get(key);
        if (v.isEmpty() || !v.isString())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "field '{}' is missing or not a string", key);
        return v.convert<std::string>();
    }
}

UniqueKeyManifest UniqueKeyManifest::read(const IDataPartStorage & storage)
{
    const std::string file_path = storage.getFullPath() + "/" + FILE_NAME;

    std::string content;
    {
        ReadSettings read_settings;
        auto in = storage.readFile(FILE_NAME, read_settings, /*read_hint=*/{});
        readStringUntilEOF(content, *in);
    }

    UniqueKeyManifest manifest;
    try
    {
        /// Strict parse: `Poco::JSON::Parser` validates the whole document
        /// (rejects trailing garbage / truncation) and `getValue<UInt64>`
        /// throws on a missing key or a non-numeric value — so a corrupted
        /// manifest fail-closes here rather than deserializing to garbage.
        /// (`base/JSON.h` was rejected: its `getUInt` stops at the first
        /// non-digit and returns the partial value without throwing.)
        Poco::JSON::Parser parser;
        const auto object = parser.parse(content).extract<Poco::JSON::Object::Ptr>();

        const UInt64 version = getStrictUInt(object, "version");
        if (version != FORMAT_VERSION)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "unsupported unique_key.txt format version {} (this binary writes {})", version, FORMAT_VERSION);

        manifest.creation_csn = getStrictUInt(object, "creation_csn");
        const UInt64 is_marker_raw = getStrictUInt(object, "is_marker");
        if (is_marker_raw > 1)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "field 'is_marker' must be 0 or 1, got {}", is_marker_raw);
        manifest.is_marker = is_marker_raw != 0;

        /// `(target, csn)` lists. `forwarded` is optional — manifests written
        /// before it existed simply omit the key; unknown keys are ignored.
        auto read_pairs = [&](const char * key, std::vector<std::pair<PartName, CSN>> & out)
        {
            if (!object->has(key))
                return;
            const auto arr = object->getArray(key);
            if (!arr)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "'{}' is not an array", key);
            for (unsigned i = 0; i < arr->size(); ++i)
            {
                const auto entry = arr->getObject(i);
                if (!entry)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "'{}' entry {} is not an object", key, i);
                auto target = getStrictString(entry, "target");
                if (target.empty())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "'{}' entry {} has an empty target", key, i);
                out.emplace_back(std::move(target), getStrictUInt(entry, "csn"));
            }
        };
        read_pairs("bitmaps_created", manifest.bitmaps_created);
        read_pairs("forwarded", manifest.forwarded);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "unique_key.txt manifest parse failure at {}: {}", file_path, e.displayText());
    }

    return manifest;
}

void UniqueKeyManifest::write(IDataPartStorage & storage, const UniqueKeyManifest & manifest)
{
    /// Serialize the JSON body in memory first so the write is a single
    /// buffer flush. Shape mirrors `ttl.txt` (a sibling part-dir JSON sidecar).
    /// `JSON_PRESERVE_KEY_ORDER` keeps field order deterministic; `is_marker` is
    /// emitted as an integer 0/1 (not a JSON bool) because `read()`'s strict
    /// integer gate rejects `true`/`false`.
    std::string serialized;
    {
        Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);
        obj.set("version", FORMAT_VERSION);
        obj.set("creation_csn", manifest.creation_csn);
        obj.set("is_marker", static_cast<UInt64>(manifest.is_marker ? 1 : 0));

        auto pairs_array = [](const std::vector<std::pair<PartName, CSN>> & pairs)
        {
            Poco::JSON::Array arr;
            for (const auto & [target, csn] : pairs)
            {
                Poco::JSON::Object::Ptr entry(new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER));
                entry->set("target", target);
                entry->set("csn", csn);
                arr.add(entry);
            }
            return arr;
        };
        obj.set("bitmaps_created", pairs_array(manifest.bitmaps_created));
        obj.set("forwarded", pairs_array(manifest.forwarded));

        WriteBufferFromString body(serialized);
        std::ostringstream oss;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        obj.stringify(oss);
        writeString(oss.str(), body);
        body.finalize();
    }

    /// Durability ordering: file body durable BEFORE the directory entry, and
    /// the whole manifest write completes before any bitmap sidecar write
    /// (recovery's tmp-scan relies on the manifest being present whenever a
    /// claimed bitmap is). Shape mirrors `DeleteBitmapFileOps::writeBitmapToStorage`.
    {
        WriteSettings write_settings;
        auto buf = storage.writeFile(FILE_NAME, /*buf_size=*/4096, WriteMode::Rewrite, write_settings);
        writeString(serialized, *buf);
        /// `sync()` then `finalize()`, same order as the sibling. `sync()` is
        /// fail-closed (throws `CANNOT_FSYNC`), so a file-content durability
        /// failure aborts the commit — the load-bearing guarantee here.
        buf->sync();
        buf->finalize();
    }

    /// Directory-entry durability. Best-effort, like the bitmap sidecars: the
    /// guard's destructor logs (does not throw) a dir-fsync failure. Dir-entry
    /// durability is uniformly best-effort across the manifest, the bitmaps, and
    /// the part-publish rename; the fail-closed file `sync()` above is what the
    /// manifest-before-bitmaps recovery invariant actually relies on.
    auto sync_guard = storage.getDirectorySyncGuard();
}

}
