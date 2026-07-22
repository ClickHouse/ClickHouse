#include <Interpreters/MecabDictionaryManager.h>

#if USE_MECAB

#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/IArchiveReader.h>

#include <Poco/String.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPBasicCredentials.h>

#include <mecab.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CANNOT_LOAD_CONFIG;
}

MecabDictionary::MecabDictionary(std::unique_ptr<MeCab::Model> model_, String dictionary_dir_)
    : model(std::move(model_)), dictionary_dir(std::move(dictionary_dir_))
{
}

MecabDictionary::~MecabDictionary() = default;

namespace
{

constexpr auto CONFIG_PREFIX = "tokenizer.japanese";
constexpr auto SYSTEM_DICTIONARY_FILE = "sys.dic";

String toHexLowercase(std::string_view bytes)
{
    static constexpr char digits[] = "0123456789abcdef";
    String out;
    out.resize(bytes.size() * 2);
    for (size_t i = 0; i < bytes.size(); ++i)
    {
        const auto c = static_cast<unsigned char>(bytes[i]);
        out[2 * i] = digits[c >> 4];
        out[2 * i + 1] = digits[c & 0x0F];
    }
    return out;
}

/// Opens a read buffer for the archive: http(s) URLs (also covers pre-signed/public S3 URLs) or a
/// local filesystem path.
std::unique_ptr<ReadBuffer> openArchiveSource(const String & location, const ContextPtr & context)
{
    if (location.starts_with("http://") || location.starts_with("https://"))
    {
        Poco::URI uri(location);
        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

        return BuilderRWBufferFromHTTP(uri)
            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
            .withSettings(context->getReadSettings())
            .withTimeouts(timeouts)
            .withHostFilter(&context->getRemoteHostFilter())
            .create({});
    }

    return std::make_unique<ReadBufferFromFile>(location);
}

/// Extracts every file from the archive into `destination`, preserving relative paths.
void extractArchive(const fs::path & archive_path, const fs::path & destination)
{
    auto reader = createArchiveReader(archive_path.string());
    auto enumerator = reader->firstFile();
    if (!enumerator)
        return;

    do
    {
        const String & name = enumerator->getFileName();
        if (name.ends_with("/")) /// directory entry
            continue;

        const fs::path out_path = destination / name;
        fs::create_directories(out_path.parent_path());

        auto file_in = reader->readFile(name, /*throw_on_not_found=*/ true);
        WriteBufferFromFile file_out(out_path.string());
        copyData(*file_in, file_out);
        file_out.finalize();
    } while (enumerator->nextFile());
}

/// Finds the directory containing the compiled system dictionary (`sys.dic`).
fs::path findDictionaryDir(const fs::path & root)
{
    for (const auto & entry : fs::recursive_directory_iterator(root))
        if (entry.is_regular_file() && entry.path().filename() == SYSTEM_DICTIONARY_FILE)
            return entry.path().parent_path();

    throw Exception(
        ErrorCodes::CANNOT_LOAD_CONFIG,
        "The Japanese dictionary archive does not contain a compiled MeCab system dictionary ('{}')",
        SYSTEM_DICTIONARY_FILE);
}

MecabDictionaryPtr createModel(const fs::path & dictionary_dir)
{
    LoggerPtr log = getLogger("MecabDictionaryManager");

    /// MeCab requires a resource file (`mecabrc`) to exist (dictionary params come from
    /// `<dictionary_dir>/dicrc` via `-d`). Provide an empty one if the archive lacks it, so loading
    /// does not fall back to a system-wide `/etc/mecabrc`.
    const fs::path rc_file = dictionary_dir / "mecabrc";
    if (!fs::exists(rc_file))
        WriteBufferFromFile(rc_file.string()).finalize();

    const String dictionary_dir_str = dictionary_dir.string();
    const String rc_file_str = rc_file.string();

    std::vector<char *> argv;
    argv.push_back(const_cast<char *>("mecab"));
    argv.push_back(const_cast<char *>("-d"));
    argv.push_back(const_cast<char *>(dictionary_dir_str.c_str()));
    argv.push_back(const_cast<char *>("-r"));
    argv.push_back(const_cast<char *>(rc_file_str.c_str()));

    std::unique_ptr<MeCab::Model> model(MeCab::createModel(static_cast<int>(argv.size()), argv.data()));
    if (!model)
        throw Exception(
            ErrorCodes::CANNOT_LOAD_CONFIG,
            "Failed to load the MeCab model from '{}': {}",
            dictionary_dir_str, MeCab::getLastError());

    LOG_INFO(log, "Loaded MeCab model from {}", dictionary_dir_str);
    return std::make_shared<const MecabDictionary>(std::move(model), dictionary_dir_str);
}

}

MecabDictionaryManager & MecabDictionaryManager::instance()
{
    static MecabDictionaryManager manager;
    return manager;
}

MecabDictionaryPtr MecabDictionaryManager::getJapaneseDictionary()
{
    std::lock_guard lock(mutex);

    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Cannot load MeCab dictionary: global context is not available");

    const auto & config = context->getConfigRef();
    if (!config.has(CONFIG_PREFIX))
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "The Japanese tokenizer requires a dictionary configured under <tokenizer><japanese> in the server configuration");

    const String sha = Poco::toLower(config.getString(std::string(CONFIG_PREFIX) + ".dictionarySha", ""));

    /// Reuse the cached dictionary if the configuration still points at the same one.
    if (cached_dictionary && cached_sha == sha)
        return cached_dictionary;

    cached_dictionary = loadJapaneseDictionary();
    cached_sha = sha;
    return cached_dictionary;
}

MecabDictionaryPtr MecabDictionaryManager::loadJapaneseDictionary()
{
    auto context = Context::getGlobalContextInstance();
    const auto & config = context->getConfigRef();
    LoggerPtr log = getLogger("MecabDictionaryManager");

    const String location = config.getString(std::string(CONFIG_PREFIX) + ".dictionaryLocation", "");
    const String expected_sha = Poco::toLower(config.getString(std::string(CONFIG_PREFIX) + ".dictionarySha", ""));

    if (location.empty())
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Missing <tokenizer><japanese><dictionaryLocation> in the server configuration");
    if (expected_sha.empty())
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Missing <tokenizer><japanese><dictionarySha> in the server configuration. "
            "The SHA-256 of the dictionary archive is required to verify its integrity before loading");

    /// Cache under the server data path, keyed by the verified SHA (download+verify happen once).
    const fs::path cache_dir = fs::path(context->getPath()) / "mecab_dictionaries" / expected_sha;
    const fs::path ready_marker = cache_dir / ".ready";

    /// Keep the archive's original extension: `createArchiveReader` picks the reader by extension.
    String archive_name = location.substr(location.find_last_of('/') + 1);
    if (const auto query_pos = archive_name.find('?'); query_pos != String::npos)
        archive_name = archive_name.substr(0, query_pos);
    if (archive_name.empty())
        archive_name = "dictionary.tar.gz";
    const fs::path archive_path = cache_dir / archive_name;

    if (!fs::exists(ready_marker))
    {
        LOG_INFO(log, "Downloading Japanese dictionary from {}", location);
        fs::create_directories(cache_dir);

        /// 1. Download the archive to a local file.
        {
            auto source = openArchiveSource(location, context);
            WriteBufferFromFile out(archive_path.string());
            copyData(*source, out);
            out.finalize();
        }

        /// 2. Verify the SHA-256 BEFORE using the archive (guards against corruption or tampering).
        std::string contents;
        {
            ReadBufferFromFile in(archive_path.string());
            readStringUntilEOF(contents, in);
        }
        const String actual_sha = toHexLowercase(encodeSHA256(contents));

        if (actual_sha != expected_sha)
        {
            fs::remove_all(cache_dir);
            throw Exception(
                ErrorCodes::CHECKSUM_DOESNT_MATCH,
                "SHA-256 mismatch for the Japanese dictionary downloaded from '{}'. "
                "Expected {} but the downloaded file has {}. The dictionary was NOT loaded. "
                "This indicates the file is corrupted or has been tampered with; verify "
                "<tokenizer><japanese><dictionarySha> and the source at <dictionaryLocation>.",
                location, expected_sha, actual_sha);
        }

        /// 3. Extract, then mark the cache entry complete only after a fully successful verify+extract.
        extractArchive(archive_path, cache_dir);
        fs::remove(archive_path);
        WriteBufferFromFile(ready_marker.string()).finalize();

        LOG_INFO(log, "Japanese dictionary verified (sha256={}) and extracted to {}", expected_sha, cache_dir.string());
    }

    /// 4. Locate the compiled system dictionary and load the model.
    return createModel(findDictionaryDir(cache_dir));
}

}

#endif
